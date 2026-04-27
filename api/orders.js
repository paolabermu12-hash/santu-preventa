const https = require('https');

const STORE = process.env.SHOPIFY_STORE;
const TOKEN = process.env.SHOPIFY_TOKEN;

function shopifyRequest(path) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: STORE,
      path,
      method: 'GET',
      headers: {
        'X-Shopify-Access-Token': TOKEN,
        'Content-Type': 'application/json'
      }
    };
    let data = '';
    const req = https.request(options, (res) => {
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ body: JSON.parse(data), headers: res.headers }); }
        catch(e) { reject(new Error('Parse error: ' + data.substring(0,200))); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function getAllOrders() {
  let orders = [];
  // Solo 2026 — toda la preventa activa esta aqui
  let url = '/admin/api/2024-01/orders.json?status=any&limit=250&created_at_min=2026-01-01T00:00:00-03:00&fields=name,created_at,financial_status,fulfillment_status,tags,total_price,billing_address,line_items';
  while (url) {
    const { body, headers } = await shopifyRequest(url);
    if (body.orders) orders = orders.concat(body.orders);
    const link = headers['link'] || '';
    const nextMatch = link.match(/<([^>]+)>;\s*rel="next"/);
    if (nextMatch) {
      const u = new URL(nextMatch[1]);
      url = u.pathname + u.search;
    } else {
      url = null;
    }
  }
  return orders;
}

const ACTIVE = new Set(['34','35','36']);
const HISTORICAL = new Set(['32','33','31','30']);
const MANUAL = {'9616':'Contenedor 34','9629':'Contenedor 35','9931':'Contenedor 36'};
const EXCLUDE = new Set(['9811|300278','9554|301076']);

// Overrides por SKU cuando el nombre no tiene fecha O cuando un producto cambio de contenedor
const SKU_CONTAINER = {
  'PK300622': 'Contenedor 34',
  '301012':   'Contenedor 34',
  '302013':   'Contenedor 35',
  '300313':   'Contenedor 34',
  '301071':   'Contenedor 36',
  '300110':   'Contenedor 35',
  // Productos C35 que se movieron a C36 (pedido #9509 y futuros)
  'PK301000': 'Contenedor 36',  // Mesa de Centro Irregular Negro Sphere
  '300521':   'Contenedor 36',  // Set Café Tazas y Platos 230ml Lineas Plateado 6un
};

function assignContainer(orderName, tags, name, fin, ful, sku) {
  const id = orderName.replace('#','');
  if (MANUAL[id]) return MANUAL[id];
  if (sku && SKU_CONTAINER[sku]) return SKU_CONTAINER[sku];
  const found = [...tags.matchAll(/Contenedor\s*(\d+)/gi)].map(m=>m[1]);
  const active = found.filter(c=>ACTIVE.has(c));
  const hist = found.filter(c=>HISTORICAL.has(c));
  // Siempre intentar inferir por fecha del producto primero
  const nl=name.toLowerCase();
  if (['29 de abril','16 de abril','20 de marzo'].some(x=>nl.includes(x))) return 'Contenedor 34';
  if (['08 de junio','01 de junio','8 de junio','30 de mayo'].some(x=>nl.includes(x))) return 'Contenedor 35';
  if (nl.includes('22 de junio')) return 'Contenedor 36';

  // Si no hay fecha en el nombre, usar el tag si es unico
  if (active.length===1) return 'Contenedor '+active[0];
  if (active.length>1) return 'Sin Asignar';
  if (hist.length>0) return 'EXCLUIR';
  try { if (parseInt(id)<9000) return 'EXCLUIR'; } catch(e) {}
  if (ful==='fulfilled') return 'EXCLUIR';
  if (['voided','refunded','expired'].includes(fin)) return 'EXCLUIR';
  return 'Sin Asignar';
}

function cleanName(n) {
  return n.replace(/^PREVENTA\s*[-–]?\s*/i,'').replace(/\s*[-–]\s*\(.*?\)\s*$/,'').replace(/\s*\(.*?\)\s*$/,'').replace(/\s*[-–]\s*$/,'').trim();
}
function fmtDate(s) {
  try { const p=s.split('T')[0].split('-'); return p[2]+'/'+p[1]+'/'+p[0].substring(2); } catch(e){return s;}
}

function processOrders(orders) {
  const C34I={'301014':50,'301015':50,'302018':30,'300415A1':4,'300415B2':2,'301012':5,'301013':3,'301036':3,'300952':18,'PK300665':7,'PK300622':7,'300625':25,'300626':25,'300945':10,'300414A1':4,'300414B2':2,'300750':40,'300927':10,'300928':10,'300929':10,'300313':25,'300314':10,'300960':20,'300962':20,'300697':2,'300698':3,'300714':50,'PK302025':10,'PK302026':10,'300620':20,'300752':20,'300951':20};
  const C35I={'301031':8,'301032':8,'PK302106':10,'302103':10,'302104':2,'302077':5,'302078':5,'PK302101':15,'302100':10,'PK302070':10,'PK302095':15,'302152':10,'302153':10,'301061':10,'301063':10,'302080':10,'302081':10,'302082':10,'302120':30,'302121':30,'302112':30,'302084':20,'302085':20,'302150':20,'302151':20,'302066':20,'302067':20,'300253':30,'302007':20,'302008':20,'302009':20,'300680':10,'300658':10,'300426':15,'300427':15,'300941':10,'300930':10,'301060':10,'301062':10,'300993':10,'300911':24,'300912':24,'300913':24,'300914':24,'300915':24,'300916':24,'PK300973':20,'300411':10,'300110':10,'302013':10,'302142':20,'302143':20};
  const C36I={'300633':10,'300241':10,'300751':10,'300659':10,'301071':10,'300411':10,'300521':10,'300253':10,'PK300973':10,'302013':10};
  const inv = sku=>(C34I[sku]||0)+(C35I[sku]||0)+(C36I[sku]||0);

  const lines=[], revMap=new Map();
  for (const o of orders) {
    const on=o.name||'', oid=on.replace('#','');
    const tags=o.tags||'', fin=o.financial_status||'';
    const ful=o.fulfillment_status||'unfulfilled';
    const customer=o.billing_address?.name||'';
    const cat=o.created_at||'';
    revMap.set(on, parseFloat(o.total_price||0));
    for (const item of (o.line_items||[])) {
      const lname=item.name||item.title||'';
      if (!lname.toUpperCase().includes('PREVENTA')) continue;
      const sku=(item.sku||'').trim();
      if (EXCLUDE.has(oid+'|'+sku)) continue;
      const container=assignContainer(on,tags,lname,fin,ful,sku);
      if (container==='EXCLUIR') continue;
      lines.push({order:on,customer,product_full:lname,product_clean:cleanName(lname),
        sku,quantity:item.quantity||0,unit_price:parseFloat(item.price)||0,
        subtotal:(parseFloat(item.price)||0)*(item.quantity||0),
        financial_status:fin,fulfillment_status:ful,created_at:cat,date_fmt:fmtDate(cat),container});
    }
  }

  const seen=new Map();
  for (const l of lines) {
    const k=l.order+'|'+l.product_clean.substring(0,40);
    if (!seen.has(k)) seen.set(k,l);
  }
  const deduped=[...seen.values()].sort((a,b)=>b.created_at.localeCompare(a.created_at));

  const pm=new Map();
  for (const l of deduped) {
    const k=l.product_clean;
    if (!pm.has(k)) pm.set(k,{units:0,revenue:0,orders:new Set(),cb:{},prices:[],sku:''});
    const v=pm.get(k);
    v.units+=l.quantity;v.revenue+=l.subtotal;v.orders.add(l.order);
    v.cb[l.container]=(v.cb[l.container]||0)+l.quantity;
    if(l.unit_price>0)v.prices.push(l.unit_price);
    if(l.sku&&!v.sku)v.sku=l.sku;
  }
  const products=[...pm.entries()].map(([name,v])=>{
    const qc=v.sku?inv(v.sku):0;
    return {name,sku:v.sku,units:v.units,revenue:v.revenue,orders:v.orders.size,
      unit_price:v.prices.length?Math.round(v.prices.reduce((a,b)=>a+b,0)/v.prices.length):0,
      c34:v.cb['Contenedor 34']||0,c35:v.cb['Contenedor 35']||0,
      c36:v.cb['Contenedor 36']||0,na:v.cb['Sin Asignar']||0,
      q_cont:qc,disponible:qc>0?qc-v.units:null};
  }).sort((a,b)=>b.revenue-a.revenue);

  const mm=new Map();
  for (const l of deduped) {
    const m=l.created_at.substring(0,7);
    if(!mm.has(m))mm.set(m,{po:new Set()});
    mm.get(m).po.add(l.order);
  }
  const mN={'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'};
  const monthly=[...mm.entries()].sort().map(([m,d])=>{
    const po=d.po.size;
    const pr=[...d.po].reduce((s,o)=>s+(revMap.get(o)||0),0);
    return {month:(mN[m.substring(5,7)]||m.substring(5,7))+' '+m.substring(2,4),
      total_orders:po,preventa_orders:po,preventa_pct:100,
      total_revenue:pr,preventa_revenue:pr,preventa_revenue_pct:100};
  });

  const po=new Set(deduped.map(l=>l.order));
  const tr=orders.reduce((s,o)=>s+parseFloat(o.total_price||0),0);
  const pr=[...po].reduce((s,o)=>s+(revMap.get(o)||0),0);
  const sa=deduped.filter(l=>l.container==='Sin Asignar');
  const so=new Set(sa.map(l=>l.order));
  const cs={};
  for (const c of ['Contenedor 34','Contenedor 35','Contenedor 36','Sin Asignar']) {
    const ls=deduped.filter(l=>l.container===c);
    const os=new Set(ls.map(l=>l.order));
    cs[c]={orders:os.size,lines:ls.reduce((s,l)=>s+l.quantity,0),revenue:ls.reduce((s,l)=>s+l.subtotal,0)};
  }

  return {
    kpis:{total_orders:orders.length,total_revenue:tr,preventa_orders:po.size,
      preventa_pct_orders:orders.length?Math.round(po.size/orders.length*1000)/10:0,
      preventa_revenue:pr,preventa_pct_revenue:tr?Math.round(pr/tr*1000)/10:0,
      unassigned_orders:so.size,updated_at:new Date().toISOString()},
    container_stats:cs,monthly,lines:deduped,products
  };
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'public, max-age=300');
  res.setHeader('Content-Type', 'application/json');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (!STORE||!TOKEN) { res.status(500).json({error:'Missing env vars'}); return; }
  try {
    const orders = await getAllOrders();
    const data = processOrders(orders);
    res.status(200).json(data);
  } catch(err) {
    console.error('ERROR:', err.message);
    res.status(500).json({error:err.message});
  }
};
