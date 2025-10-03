import { Client } from 'pg';

const SRC = 'uk_cf';
const PAGES = 3;
const RX = /\/notice\/([0-9a-fA-F-]{36})\b/gi;

const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search='';
const pg = new Client({ connectionString: u.toString(), ssl: { require:true, rejectUnauthorized:false } });

const pageUrl = (p) => `https://www.contractsfinder.service.gov.uk/Search/Results?p=${p}`;

(async () => {
  await pg.connect();
  const seen = new Set();

  for (let p = 1; p <= PAGES; p++) {
    const url = pageUrl(p);
    const r = await fetch(url, { headers: { 'User-Agent': 'uk-contracts-crawler/1.0' } });
    const html = await r.text();
    RX.lastIndex = 0;
    for (const m of html.matchAll(RX)) seen.add(m[1].toLowerCase());
    console.log(`page ${p} captured so far: ${seen.size} ids`);
  }

  const ids = [...seen];
  console.log('first 10 ids:', ids.slice(0,10).join(', '));

  const { rows } = await pg.query(
    `SELECT source_id FROM public.uk_staging_std WHERE source=$1 AND source_id = ANY($2::text[])`,
    [SRC, ids]
  );
  const have = new Set(rows.map(r => r.source_id));
  const missing = ids.filter(id => !have.has(id));

  console.log('total unique ids across pages 1-3:', ids.length);
  console.log('in staging already:', have.size);
  console.log('NOT in staging:', missing.length);
  console.log('sample missing (up to 10):', missing.slice(0,10).join(', '));

  await pg.end();
})().catch(e => { console.error(e.stack || e.message); process.exit(2); });
