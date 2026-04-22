module.exports = async (req, res) => {
  res.json({
    store: process.env.SHOPIFY_STORE || 'NO CONFIGURADO',
    token_prefix: process.env.SHOPIFY_TOKEN ? process.env.SHOPIFY_TOKEN.substring(0,10)+'...' : 'NO CONFIGURADO',
    token_length: process.env.SHOPIFY_TOKEN ? process.env.SHOPIFY_TOKEN.length : 0,
    node_version: process.version,
    env_keys: Object.keys(process.env).filter(k => k.startsWith('SHOPIFY'))
  });
};
