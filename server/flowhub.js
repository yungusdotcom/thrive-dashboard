// server/flowhub.js — Thrive Cannabis Marketplace
// Bulk-fetch: 1 API call per store for full date range, bucket into weeks
const fetch = require('node-fetch');
const fs = require('fs');

const BASE       = 'https://api.flowhub.co';
const CLIENT_ID  = process.env.FLOWHUB_CLIENT_ID;
const CLIENT_KEY = process.env.FLOWHUB_API_KEY;
const CACHE_DIR  = process.env.CACHE_DIR || '/tmp';
const CACHE_FILE = `${CACHE_DIR}/thrive-week-cache.json`;

const STORE_CONFIG = [
  { id: 'cactus',   match: 'cactus',      display: 'Cactus',      color: '#00e5a0' },
  { id: 'cheyenne', match: 'cheyenne',    display: 'Cheyenne',    color: '#4db8ff' },
  { id: 'jackpot',  match: 'jackpot',     display: 'Jackpot',     color: '#c084fc' },
  { id: 'main',     match: 'main street', display: 'Main Street', color: '#ffd166' },
  { id: 'reno',     match: 'reno',        display: 'Reno',        color: '#ff8c42' },
  { id: 'sahara',   match: 'sahara',      display: 'Sahara',      color: '#ff4d6d' },
  { id: 'sammy',    match: 'sammy',       display: 'Sammy',       color: '#a8e6cf' },
];
const EXCLUDED_KEYWORDS = ['smoke', 'mirrors', 'mbnv', 'cultivation'];

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function round2(n) { return Math.round(n * 100) / 100; }

// ── Date helpers (Pacific Time) ──────────────────────────────
const TZ = 'America/Los_Angeles';
function todayPacific() { return new Date().toLocaleDateString('en-CA', { timeZone: TZ }); }
function dowPacific() {
  const dn = new Date().toLocaleDateString('en-US', { timeZone: TZ, weekday: 'short' });
  return { Sun:0, Mon:1, Tue:2, Wed:3, Thu:4, Fri:5, Sat:6 }[dn] ?? 0;
}
function addDays(ds, n) { const d = new Date(ds+'T12:00:00Z'); d.setDate(d.getDate()+n); return d.toISOString().split('T')[0]; }
function toDateStr(d) { return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; }
function weekRange(wb=0) {
  const today=todayPacific(), dow=dowPacific(), dsm=(dow+6)%7;
  const mon=addDays(today,-dsm-(wb*7)); return {start:mon, end:addDays(mon,6)};
}
function todayRange() { const d=todayPacific(); return {start:d,end:d}; }
function ytdRange() { const t=todayPacific(); return {start:`${t.split('-')[0]}-01-01`,end:t}; }

// ── Persistent cache ──────────────────────────────────────────
let _weekCache = {};
function loadWeekCache() {
  try { if(fs.existsSync(CACHE_FILE)){_weekCache=JSON.parse(fs.readFileSync(CACHE_FILE,'utf8'));console.log(`✓ ${Object.keys(_weekCache).length} cached weeks loaded`);} }
  catch(e){console.log('⚠ cache load:',e.message);_weekCache={};}
}
let _savePending=false;
function saveWeekCache() {
  if(_savePending) return; _savePending=true;
  setTimeout(()=>{_savePending=false;try{fs.writeFileSync(CACHE_FILE,JSON.stringify(_weekCache),'utf8');console.log(`✓ ${Object.keys(_weekCache).length} weeks saved`);}catch(e){console.log('⚠ cache save:',e.message);}},3000);
}
loadWeekCache();
function wck(id,ws){return `${id}:${ws}`;}
function isComplete(w){return w.end<todayPacific();}

// ── Flowhub API ───────────────────────────────────────────────
let _lastApi=0;
async function flowhubGet(path, params={}) {
  const gap=300-(Date.now()-_lastApi); if(gap>0) await sleep(gap); _lastApi=Date.now();
  const url=new URL(`${BASE}${path}`);
  Object.entries(params).forEach(([k,v])=>{if(v!=null)url.searchParams.set(k,String(v));});
  for(let a=0;a<5;a++){
    if(a>0){const d=Math.min(1500*Math.pow(2,a),20000);console.log(`  ↻ retry#${a} ${d}ms`);await sleep(d);}
    const res=await fetch(url.toString(),{headers:{'clientId':CLIENT_ID,'key':CLIENT_KEY,'Accept':'application/json','Content-Type':'application/json'}});
    if(res.status===429){console.log(`⚠ 429: ${path}`);continue;}
    if(res.status===500&&a<4){console.log(`⚠ 500: ${path}`);continue;}
    if(!res.ok){const b=await res.text().catch(()=>'');throw new Error(`Flowhub ${res.status} ${path}: ${b.slice(0,200)}`);}
    return res.json();
  }
  throw new Error(`Max retries: ${path}`);
}

// ── Locations ─────────────────────────────────────────────────
let _locations=null;
async function getLocations() {
  if(_locations) return _locations;
  const data=await flowhubGet('/v0/clientsLocations');
  const raw=Array.isArray(data)?data:(data.locations||data.data||[]);
  _locations=raw.filter(l=>{const n=(l.displayName||l.name||'').toLowerCase();return !EXCLUDED_KEYWORDS.some(kw=>n.includes(kw));})
    .map(l=>{const n=(l.displayName||l.name||'').toLowerCase();const c=STORE_CONFIG.find(s=>n.includes(s.match));
      return {importId:l.importId,id:c?.id||l.importId,name:c?.display||l.displayName||l.name,color:c?.color||'#888',rawName:l.displayName||l.name};});
  console.log('✓',_locations.length,'locations');return _locations;
}

// ── Fetch orders ──────────────────────────────────────────────
let _schemaLogged=false;
async function getOrdersForLocation(importId, startDate, endDate) {
  const start=startDate.split('T')[0], end=endDate.split('T')[0];
  let page=1, allOrders=[], total=0;
  while(true){
    try{
      const data=await flowhubGet(`/v1/orders/findByLocationId/${importId}`,{created_after:start,created_before:end,page_size:500,page,order_by:'asc'});
      const batch=data.orders||[];total=data.total||0;allOrders=allOrders.concat(batch);
      if(!_schemaLogged&&batch.length>0){_schemaLogged=true;console.log('ORDER KEYS:',Object.keys(batch[0]).join(', '));}
      if(allOrders.length>=total||batch.length<500) break; page++;
    }catch(err){console.error(`✗ ${importId.slice(0,8)} ${start}→${end} p${page}: ${err.message}`);break;}
  }
  return {total:allOrders.length, orders:allOrders};
}

// ── Summarize ─────────────────────────────────────────────────
function summarizeOrders(orders) {
  if(!orders||!orders.length) return {transaction_count:0,net_sales:0,gross_sales:0,avg_basket:0,total_items:0,categories:[],budtenders:[],customer_types:{rec:0,med:0}};
  let ns=0,gs=0,ti=0; const cm={},bm={},ct={rec:0,med:0};
  orders.forEach(o=>{
    if(o.voided===true||o.orderStatus==='voided') return;
    const tp=(o.customerType||'').toLowerCase(); if(tp.includes('med'))ct.med++;else ct.rec++;
    const bt=o.budtender||'Unknown'; if(!bm[bt])bm[bt]={name:bt,transactions:0,net_sales:0,items:0}; bm[bt].transactions++;
    let on=0,og=0,oi=0;
    (o.itemsInCart||[]).forEach(it=>{
      if(it.voided===true)return; const q=it.quantity||1; oi+=q;
      const lg=Number(it.totalPrice)||(Number(it.unitPrice||0)*q), ld=Number(it.totalDiscounts)||0;
      og+=lg; on+=(lg-ld);
      const cat=it.category||it.type||'Other';
      if(!cm[cat])cm[cat]={name:cat,net_sales:0,units:0,transactions:0};
      cm[cat].net_sales+=(lg-ld);cm[cat].units+=q;cm[cat].transactions++;
    });
    ns+=on;gs+=og;ti+=oi;bm[bt].net_sales+=on;bm[bt].items+=oi;
  });
  const tc=orders.length;
  return {transaction_count:tc,net_sales:round2(ns),gross_sales:round2(gs),avg_basket:round2(tc>0?ns/tc:0),total_items:ti,customer_types:ct,
    categories:Object.values(cm).sort((a,b)=>b.net_sales-a.net_sales).map(c=>({...c,net_sales:round2(c.net_sales)})),
    budtenders:Object.values(bm).map(b=>({...b,net_sales:round2(b.net_sales),avg_basket:round2(b.transactions?b.net_sales/b.transactions:0)})).sort((a,b)=>b.net_sales-a.net_sales)};
}

// ── Top products ──────────────────────────────────────────────
function extractTopProducts(orders, limit=15) {
  const m={};
  orders.forEach(o=>{if(o.voided)return;(o.itemsInCart||[]).forEach(it=>{
    if(it.voided)return;const nm=it.productName||it.title1||'Unknown',br=it.brand||'',cat=it.category||it.type||'Other',q=it.quantity||1,g=Number(it.totalPrice)||0,d=Number(it.totalDiscounts)||0,k=`${nm}__${br}`;
    if(!m[k])m[k]={name:nm,brand:br,category:cat,units_sold:0,net_sales:0,prices:[]};m[k].units_sold+=q;m[k].net_sales+=(g-d);if(it.unitPrice)m[k].prices.push(Number(it.unitPrice));
  });});
  return Object.values(m).map(p=>({...p,net_sales:round2(p.net_sales),avg_price:p.prices.length?round2(p.prices.reduce((a,b)=>a+b,0)/p.prices.length):0})).sort((a,b)=>b.net_sales-a.net_sales).slice(0,limit);
}

// ── Bucket orders into weeks ──────────────────────────────────
function bucketOrdersByWeek(orders, weeks) {
  const bk=weeks.map(w=>({week:w,orders:[]}));
  orders.forEach(o=>{const d=(o.createdAt||o.completedOn||'').split('T')[0];for(const b of bk){if(d>=b.week.start&&d<=b.week.end){b.orders.push(o);break;}}});
  return bk.map(b=>({week:b.week,summary:summarizeOrders(b.orders),error:null}));
}

// ══════════════════════════════════════════════════════════════
// CORE: getAllStoresWeeklyTrend — bulk fetch
// First load (empty cache): 7 API calls (1 per store, full range)
// Cached: 7 API calls (1 per store, current week only)
// ══════════════════════════════════════════════════════════════
async function getAllStoresWeeklyTrend(weeksBack=12) {
  const locs=await getLocations();
  const weeks=Array.from({length:weeksBack},(_,i)=>weekRange(weeksBack-1-i));
  const results=[];
  for(const loc of locs){
    const comp=weeks.filter(w=>isComplete(w));
    const cc=comp.filter(w=>_weekCache[wck(loc.importId,w.start)]).length;
    if(cc===comp.length){
      console.log(`  ${loc.name}: ${cc} cached → current week only`);
      const trend=weeks.map(w=>{const ck=wck(loc.importId,w.start);if(isComplete(w)&&_weekCache[ck])return _weekCache[ck];return{week:w,summary:null,error:null};});
      try{const cw=weeks[weeks.length-1];const{orders}=await getOrdersForLocation(loc.importId,cw.start,cw.end);trend[trend.length-1]={week:cw,summary:summarizeOrders(orders),error:null};}catch(e){console.error(`  ${loc.name} cw: ${e.message}`);}
      results.push({store:loc,trend});
    } else {
      console.log(`  ${loc.name}: ${cc}/${comp.length} cached → bulk fetch`);
      try{
        const{orders}=await getOrdersForLocation(loc.importId,weeks[0].start,weeks[weeks.length-1].end);
        console.log(`    → ${orders.length} orders`);
        const trend=bucketOrdersByWeek(orders,weeks);
        let nc=0;trend.forEach(e=>{if(isComplete(e.week)&&e.summary?.net_sales>0){_weekCache[wck(loc.importId,e.week.start)]=e;nc++;}});
        if(nc>0)saveWeekCache();
        results.push({store:loc,trend});
      }catch(e){console.error(`  ${loc.name} bulk: ${e.message}`);results.push({store:loc,trend:weeks.map(w=>({week:w,summary:null,error:e.message}))});}
    }
  }
  return results;
}

async function getWeeklyTrend(importId, weeksBack=12) {
  const weeks=Array.from({length:weeksBack},(_,i)=>weekRange(weeksBack-1-i));
  const comp=weeks.filter(w=>isComplete(w));
  if(comp.every(w=>_weekCache[wck(importId,w.start)])){
    const trend=weeks.map(w=>{const ck=wck(importId,w.start);if(isComplete(w)&&_weekCache[ck])return _weekCache[ck];return{week:w,summary:null,error:null};});
    try{const cw=weeks[weeks.length-1];const{orders}=await getOrdersForLocation(importId,cw.start,cw.end);trend[trend.length-1]={week:cw,summary:summarizeOrders(orders),error:null};}catch(e){}
    return trend;
  }
  const{orders}=await getOrdersForLocation(importId,weeks[0].start,weeks[weeks.length-1].end);
  const trend=bucketOrdersByWeek(orders,weeks);
  trend.forEach(e=>{if(isComplete(e.week)&&e.summary?.net_sales>0)_weekCache[wck(importId,e.week.start)]=e;});
  saveWeekCache(); return trend;
}

// ── Dashboard: 1 call per store ───────────────────────────────
async function getDashboardData() {
  const tw=weekRange(0),lw=weekRange(1),td=todayRange();
  const locs=await getLocations(); const storeData=[];
  for(const loc of locs){
    try{
      const lwCk=wck(loc.importId,lw.start);
      let lwS,twO,tdO;
      if(isComplete(lw)&&_weekCache[lwCk]){
        lwS=_weekCache[lwCk].summary;
        const{orders}=await getOrdersForLocation(loc.importId,tw.start,tw.end);
        twO=orders;tdO=orders.filter(o=>(o.createdAt||'').split('T')[0]===td.start);
        console.log(`  ${loc.name}: LW cached, TW ${orders.length}`);
      }else{
        const{orders}=await getOrdersForLocation(loc.importId,lw.start,tw.end);
        const lwO=orders.filter(o=>{const d=(o.createdAt||'').split('T')[0];return d>=lw.start&&d<=lw.end;});
        twO=orders.filter(o=>{const d=(o.createdAt||'').split('T')[0];return d>=tw.start&&d<=tw.end;});
        tdO=orders.filter(o=>(o.createdAt||'').split('T')[0]===td.start);
        lwS=summarizeOrders(lwO);
        console.log(`  ${loc.name}: bulk ${orders.length}`);
        if(isComplete(lw)&&lwS.net_sales>0){_weekCache[lwCk]={week:lw,summary:lwS,error:null};saveWeekCache();}
      }
      storeData.push({...loc,thisWeek:summarizeOrders(twO),lastWeek:lwS,today:summarizeOrders(tdO)});
    }catch(e){console.error(`  ${loc.name}: ${e.message}`);storeData.push({...loc,thisWeek:null,lastWeek:null,today:null});}
  }
  return {meta:{fetchedAt:new Date().toISOString(),dateRanges:{thisWeek:tw,lastWeek:lw,today:td,ytd:ytdRange()}},stores:storeData};
}

async function getAllStoresSales(startDate, endDate) {
  const locs=await getLocations();const r=[];
  for(const loc of locs){try{const{orders}=await getOrdersForLocation(loc.importId,startDate,endDate);r.push({store:loc,summary:summarizeOrders(orders),orders});}catch(e){r.push({store:loc,summary:null,orders:[],error:e.message});}}
  return r;
}

async function getRawOrderSample(importId) {
  const td=todayRange();
  const data=await flowhubGet(`/v1/orders/findByLocationId/${importId}`,{created_after:td.start,created_before:td.end,page_size:3,page:1});
  return {total:data.total,sample:(data.orders||[]).slice(0,2)};
}

async function getSingleDayVsDay(dow, weeksBack=4) {
  const locs=await getLocations(),today=todayPacific(),tdow=dowPacific();
  const dn=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  const dates=[];
  for(let w=0;w<weeksBack;w++){let db=(tdow-dow+7)%7;if(db===0&&w>0)db=7*w;else if(w>0)db+=7*w;const d=addDays(today,-db);if(d<=today)dates.push(d);}
  const dayData=[];
  for(const date of dates){
    console.log(`  DvD: ${dn[dow]} ${date}`);
    const sr=[];for(const loc of locs){try{const{orders}=await getOrdersForLocation(loc.importId,date,date);sr.push({store:loc,summary:summarizeOrders(orders)});}catch(e){sr.push({store:loc,summary:null,error:e.message});}}
    dayData.push({date,stores:sr});
  }
  return {dow,dayName:dn[dow],dates:dayData};
}

module.exports = {
  getLocations, getOrdersForLocation, summarizeOrders, extractTopProducts,
  getAllStoresSales, getWeeklyTrend, getAllStoresWeeklyTrend, getDashboardData,
  getRawOrderSample, getSingleDayVsDay,
  weekRange, todayRange, ytdRange, toDateStr, STORE_CONFIG,
};
