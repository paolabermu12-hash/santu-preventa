const https = require('https');

const STORE = process.env.SHOPIFY_STORE;
const TOKEN = process.env.SHOPIFY_TOKEN;

function shopifyRequest(path) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: STORE, path, method: 'GET',
      headers: { 'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json' }
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
  let url = '/admin/api/2024-01/orders.json?status=any&limit=250&created_at_min=2026-01-01T00:00:00-03:00&fields=name,created_at,financial_status,fulfillment_status,tags,total_price,billing_address,line_items';
  while (url) {
    const { body, headers } = await shopifyRequest(url);
    if (body.orders) orders = orders.concat(body.orders);
    const link = headers['link'] || '';
    const nextMatch = link.match(/<([^>]+)>;\s*rel="next"/);
    if (nextMatch) { const u = new URL(nextMatch[1]); url = u.pathname + u.search; }
    else { url = null; }
  }
  return orders;
}

const ACTIVE = new Set(['34','35','36','37','38']);
const HISTORICAL = new Set(['32','33','31','30']);
const MANUAL = {'9616':'Contenedor 34','9629':'Contenedor 35','9931':'Contenedor 36'};
const EXCLUDE = new Set(['9811|300278','9554|301076']);

// === INVENTARIOS POR CONTENEDOR (FUENTE DE VERDAD FISICA) ===
// Se usan para:
//   (a) disponibilidad por producto
//   (b) desempate en pedidos multi-tag (si el SKU esta en un solo contenedor activo)
//   (c) deteccion de tags incorrectos en Shopify (alerta de reasignacion)
const C34I={'301014':50,'301015':50,'302018':30,'300415A1':4,'300415B2':2,'301012':5,'301013':3,'301036':3,'300952':18,'PK300665':7,'PK300622':7,'300625':25,'300626':25,'300945':10,'300414A1':4,'300414B2':2,'300750':40,'300927':10,'300928':10,'300929':10,'300313':25,'300314':10,'300960':20,'300962':20,'300697':2,'300698':3,'300714':50,'PK302025':10,'PK302026':10,'300620':20,'300752':20,'300951':20};
const C35I={'301031':8,'301032':8,'PK302106':10,'302103':10,'302104':2,'302077':5,'302078':5,'PK302101':15,'302100':10,'PK302070':10,'PK302095':15,'302152':10,'302153':10,'301061':10,'301063':10,'302080':10,'302081':10,'302082':10,'302120':30,'302121':30,'302112':30,'302084':20,'302085':20,'302150':20,'302151':20,'302066':20,'302067':20,'300253':30,'302007':20,'302008':20,'302009':20,'300680':10,'300658':10,'300426':15,'300427':15,'300941':10,'300930':10,'301060':10,'301062':10,'300993':10,'300911':24,'300912':24,'300913':24,'300914':24,'300915':24,'300916':24,'PK300973':20,'300411':10,'300110':10,'302013':10,'302142':20,'302143':20};
const C36I={'300658':40,'300411':30,'PK302076':45,'PK301000':20,'300015':48,'300358':40,'300910':40,'300357':20,'302013':5,'200437':40,'300110':15,'300714':50,'300278':6,'300966':10,'300967':4,'300968':5,'300969':5,'300970':5,'302179':24,'301017':30,'301018':30,'PK300676SKU':20,'302001':30,'300973':20,'302101':10,'300699':2,'300697':2,'302145':10,'302144':10,'302146':10,'300954':10,'300953':10,'301076':50,'301077':20,'300426':12};
const C37I={'302180':16,'302000':30,'302004':30,'300689':10,'300436':20,'300437':20,'302123':5,'302124':5,'302127':10,'302128':10,'302129':10,'302130':30,'300426':8,'300427':10,'300648':5,'300659':5,'301072':10,'301073':10,'300965':5,'300966':10,'300967':6,'300968':5,'300969':5,'300970':3,'300266':50,'300633':50,'301017':20,'301018':20,'300241':20,'300240':20,'300242':40,'PK302155':1,'300986':10,'302154':20,'300230':108,'300984':30,'301058':30,'301059':30,'300942':10,'300943':10,'300521':50,'302147':20,'300318':38,'301035':1,'300932':2,'300935':3,'300933':5,'300931':5,'300697':2,'302126':10,'302125':10,'302189':50};
const C38I={'301010':20,'PK300622':10,'300751':5,'301013':2};

const INV_BY_C = {
  'Contenedor 34': C34I, 'Contenedor 35': C35I, 'Contenedor 36': C36I,
  'Contenedor 37': C37I, 'Contenedor 38': C38I
};

function containersForSku(sku) {
  if (!sku) return [];
  return Object.keys(INV_BY_C).filter(c => INV_BY_C[c][sku]);
}

function assignContainer(orderName, tags, name, fin, ful, sku) {
  const id = orderName.replace('#','');
  if (MANUAL[id]) return MANUAL[id];

  const found = [...tags.matchAll(/Contenedor\s*(\d+)/gi)].map(m=>m[1]);
  const active = found.filter(c=>ACTIVE.has(c));
  const hist = found.filter(c=>HISTORICAL.has(c));

  // 1. Fecha en nombre del producto (desempata pedidos multi-contenedor)
  const nl = name.toLowerCase();
  if (['29 de abril','16 de abril','20 de marzo'].some(x=>nl.includes(x))) return 'Contenedor 34';
  if (['08 de junio','01 de junio','8 de junio','1 de junio','30 de mayo'].some(x=>nl.includes(x))) return 'Contenedor 35';
  if (['22 de junio','29 de junio'].some(x=>nl.includes(x))) return 'Contenedor 36';
  if (nl.includes('14 de julio')) return 'Contenedor 37';
  if (nl.includes('03 de agosto') || nl.includes('3 de agosto')) return 'Contenedor 38';

  // 2. Tag unico
  if (active.length===1) return 'Contenedor '+active[0];

  // 3. Multi-tag: desempate por inventario (SKU presente en solo uno de los activos)
  if (active.length>1) {
    if (sku) {
      const inOne = active.filter(c => INV_BY_C['Contenedor '+c] && INV_BY_C['Contenedor '+c][sku]);
      if (inOne.length === 1) return 'Contenedor '+inOne[0];
    }
    return 'Sin Asignar';
  }

  // 4. Fallbacks de exclusion
  if (hist.length>0) return 'EXCLUIR';
  try { if (parseInt(id)<9000) return 'EXCLUIR'; } catch(e) {}
  if (ful==='fulfilled') return 'EXCLUIR';
  if (['voided','refunded','expired'].includes(fin)) return 'EXCLUIR';
  return 'Sin Asignar';
}

// Validacion: compara la asignacion actual contra donde el SKU realmente esta.
// Solo genera alerta para pedidos pendientes (no preparados/enviados).
function validateAssignment(line) {
  if (!line.sku || line.sku.trim() === '') return null;
  if (line.fulfillment_status === 'fulfilled') return null;
  if (line.container === 'EXCLUIR') return null;

  const expected = containersForSku(line.sku);
  if (expected.length === 0) {
    return { type: 'unknown_sku', message: 'SKU no aparece en ninguna planilla' };
  }
  const labels = expected.map(c => c.replace('Contenedor ','C')).join(' o ');
  if (line.container === 'Sin Asignar') {
    return { type: 'suggest', suggested: expected, label: labels,
      message: 'SKU esta en ' + expected.join(', ') + ' segun planilla. Agregar tag en Shopify.' };
  }
  if (!expected.includes(line.container)) {
    return { type: 'mismatch', current: line.container, suggested: expected, label: labels,
      message: 'Tag actual: ' + line.container + '. SKU esta fisicamente en ' + expected.join(', ') };
  }
  return null;
}

function cleanName(n) {
  return n.replace(/^PREVENTA\s*[-–]?\s*/i,'').replace(/\s*[-–]\s*\(.*?\)\s*$/,'').replace(/\s*\(.*?\)\s*$/,'').replace(/\s*[-–]\s*$/,'').trim();
}
function fmtDate(s) {
  try { const p=s.split('T')[0].split('-'); return p[2]+'/'+p[1]+'/'+p[0].substring(2); } catch(e){return s;}
}

function processOrders(orders) {
  const inv = sku=>(C34I[sku]||0)+(C35I[sku]||0)+(C36I[sku]||0)+(C37I[sku]||0)+(C38I[sku]||0);
  const sumInv = m=>Object.values(m).reduce((s,n)=>s+n,0);

  const lines=[], revMap=new Map();
  for (const o of orders) {
    const on=o.name||'', oid=on.replace('#','');
    const tags=o.tags||'', fin=o.financial_status||'';
    const ful=o.fulfillment_status||'unfulfilled';
    const customer=o.billing_address?.name||'';
    const cat=o.created_at||'';
    revMap.set(on, parseFloat(o.total_price||0));
    for (const item of (o.line_items||[])) {
      const lname=[item.title,item.variant_title,item.name].filter(Boolean).join(' - ');
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

  // Adjuntar validacion
  for (const l of deduped) {
    const v = validateAssignment(l);
    if (v) l.validation = v;
  }

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
      c36:v.cb['Contenedor 36']||0,c37:v.cb['Contenedor 37']||0,
      c38:v.cb['Contenedor 38']||0,na:v.cb['Sin Asignar']||0,
      q_cont:qc,disponible:qc>0?qc-v.units:null};
  }).sort((a,b)=>b.revenue-a.revenue);

  const mm=new Map();
  for (const l of deduped) {
    const m=l.created_at.substring(0,7);
    if(!mm.has(m))mm.set(m,{po:new Set(),pp:new Set()});
    mm.get(m).po.add(l.order);
    if (l.fulfillment_status==='fulfilled') mm.get(m).pp.add(l.order);
  }
  const mN={'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'};
  const monthly=[...mm.entries()].sort().map(([m,d])=>{
    const po=d.po.size, pp=d.pp.size;
    const pr=[...d.po].reduce((s,o)=>s+(revMap.get(o)||0),0);
    return {month:(mN[m.substring(5,7)]||m.substring(5,7))+' '+m.substring(2,4),
      total_orders:po,preventa_orders:po,prepared_orders:pp,preventa_pct:100,
      total_revenue:pr,preventa_revenue:pr,preventa_revenue_pct:100};
  });

  // KPIs
  const po=new Set(deduped.map(l=>l.order));
  const prepO=new Set(deduped.filter(l=>l.fulfillment_status==='fulfilled').map(l=>l.order));
  const pendO=new Set([...po].filter(o=>!prepO.has(o)));
  const tr=orders.reduce((s,o)=>s+parseFloat(o.total_price||0),0);
  const pr=[...po].reduce((s,o)=>s+(revMap.get(o)||0),0);

  const sa=deduped.filter(l=>l.container==='Sin Asignar'&&l.fulfillment_status!=='fulfilled');
  const so=new Set(sa.map(l=>l.order));

  // Stats de validacion (solo pendientes, los preparados ya no son accionables)
  const mismatches = deduped.filter(l => l.validation && l.validation.type === 'mismatch');
  const suggestions = deduped.filter(l => l.validation && l.validation.type === 'suggest');
  const unknowns = deduped.filter(l => l.validation && l.validation.type === 'unknown_sku');
  const mOrders = new Set(mismatches.map(l=>l.order));
  const sOrders = new Set(suggestions.map(l=>l.order));
  const uOrders = new Set(unknowns.map(l=>l.order));

  const cs={};
  for (const c of ['Contenedor 34','Contenedor 35','Contenedor 36','Contenedor 37','Contenedor 38','Sin Asignar']) {
    const ls=deduped.filter(l=>l.container===c);
    const prepLs=ls.filter(l=>l.fulfillment_status==='fulfilled');
    const pendLs=ls.filter(l=>l.fulfillment_status!=='fulfilled');
    const os=new Set(ls.map(l=>l.order));
    const prepOs=new Set(prepLs.map(l=>l.order));
    const pendOs=new Set(pendLs.map(l=>l.order));
    cs[c]={
      orders:os.size, prepared_orders:prepOs.size, pending_orders:pendOs.size,
      lines:ls.reduce((s,l)=>s+l.quantity,0),
      prepared_lines:prepLs.reduce((s,l)=>s+l.quantity,0),
      pending_lines:pendLs.reduce((s,l)=>s+l.quantity,0),
      revenue:ls.reduce((s,l)=>s+l.subtotal,0),
      prepared_revenue:prepLs.reduce((s,l)=>s+l.subtotal,0),
      pending_revenue:pendLs.reduce((s,l)=>s+l.subtotal,0)
    };
  }

  return {
    kpis:{total_orders:orders.length,total_revenue:tr,
      preventa_orders:po.size,preventa_pct_orders:orders.length?Math.round(po.size/orders.length*1000)/10:0,
      preventa_revenue:pr,preventa_pct_revenue:tr?Math.round(pr/tr*1000)/10:0,
      pending_preventa_orders:pendO.size, prepared_preventa_orders:prepO.size,
      unassigned_orders:so.size,
      mismatch_lines:mismatches.length, mismatch_orders:mOrders.size,
      suggestion_lines:suggestions.length, suggestion_orders:sOrders.size,
      unknown_sku_lines:unknowns.length, unknown_sku_orders:uOrders.size,
      review_total:mOrders.size+sOrders.size,
      updated_at:new Date().toISOString()},
    container_stats:cs,
    container_inventory:{c34:sumInv(C34I),c35:sumInv(C35I),c36:sumInv(C36I),c37:sumInv(C37I),c38:sumInv(C38I)},
    monthly,lines:deduped,products
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
