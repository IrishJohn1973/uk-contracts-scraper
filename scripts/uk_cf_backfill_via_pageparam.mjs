import { Client } from 'pg';
import { setTimeout as wait } from 'node:timers/promises';

const SRC = 'uk_cf';
const PAGES = Number(process.argv[2] || '50');
const PAUSE_MS = 350;
const RX = /\/notice\/([0-9a-fA-F-]{36})\b/gi;

const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search='';
const pg = new Client({ connectionString: u.toString(), ssl: { require:true, rejectUnauthorized:false } });

const pageUrl = (p) => `https://www.contractsfinder.service.gov.uk/Search/Results?page=${p}`;

const insertIfMissing = async (source_id) => {
  const source_url = `https://www.contractsfinder.service.gov.uk/notice/${source_id}`;
  const uk_uid = `${SRC}:${source_id}`;
  const res = await pg.query(
    `INSERT INTO public.uk_staging_std (uk_uid, source, source_id, source_url, notice_type, nuts)
     SELECT $1,$2,$3,$4,'tender','UK'
     WHERE NOT EXISTS (SELECT 1 FROM public.uk_staging_std WHERE source=$2 AND source_id=$3)`,
    [uk_uid, SRC, source_id, source_url]
  );
  return res.rowCount === 1;
};

(async () => {
  await pg.connect();
  let total=0, ins=0, skip=0;

  for (let p=1; p<=PAGES; p++){
    const url = pageUrl(p);
    const r = await fetch(url, { headers: { 'User-Agent':'uk-contracts-scraper/1.0' } });
    if (!r.ok) { console.log(`page ${p}: HTTP ${r.status}`); continue; }
    const html = await r.text();

    RX.lastIndex = 0;
    const ids = new Set([...html.matchAll(RX)].map(m=>m[1].toLowerCase()));
    total += ids.size;

    for (const id of ids) (await insertIfMissing(id)) ? ins++ : skip++;
    console.log(`page ${p}: ids=${ids.size}, inserted=${ins}, skipped=${skip}`);
    await wait(PAUSE_MS);
  }

  console.log(`OK -> backfill via ?page=: pages=${PAGES}, ids_seen=${total}, inserted=${ins}, skipped=${skip}`);
  await pg.end();
})().catch(e=>{ console.error('ERROR ->', e.stack||e.message); process.exit(2); });
