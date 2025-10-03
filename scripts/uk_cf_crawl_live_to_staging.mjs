import { Client } from 'pg';
import { setTimeout as wait } from 'node:timers/promises';

const SRC = 'uk_cf';
const PAGES = Number(process.argv[2] || '50');      // how many results pages to crawl live
const PAUSE_MS = 400;                                // politeness delay between pages

const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search='';
const pg = new Client({ connectionString: u.toString(), ssl: { require:true, rejectUnauthorized:false } });

// Case-insensitive UUID capture: /notice/{uuid}
const RX = /\/notice\/([0-9a-fA-F-]{36})\b/gi;

// Build a stable, “newest first” results URL.
// (We keep it minimal and rely on their default filters; page param drives pagination.)
const pageUrl = (p) => `https://www.contractsfinder.service.gov.uk/Search/Results?p=${p}`;

const insertIfMissing = async (source_id) => {
  const source_url = `https://www.contractsfinder.service.gov.uk/notice/${source_id}`;
  const uk_uid = `${SRC}:${source_id}`;
  const res = await pg.query(
    `INSERT INTO public.uk_staging_std (uk_uid, source, source_id, source_url, notice_type, nuts)
     SELECT $1,$2,$3,$4,'tender','UK'
     WHERE NOT EXISTS (
       SELECT 1 FROM public.uk_staging_std WHERE source=$2 AND source_id=$3
     )`,
    [uk_uid, SRC, source_id, source_url]
  );
  return res.rowCount === 1;
};

(async () => {
  await pg.connect();
  let total = 0, inserted = 0, skipped = 0;

  for (let p = 1; p <= PAGES; p++) {
    const url = pageUrl(p);
    const r = await fetch(url, { headers: { 'User-Agent': 'uk-contracts-crawler/1.0' } });
    if (!r.ok) { console.error('WARN -> page', p, 'HTTP', r.status); continue; }
    const html = await r.text();

    RX.lastIndex = 0;
    const ids = new Set();
    for (const m of html.matchAll(RX)) ids.add(m[1].toLowerCase());

    total += ids.size;

    for (const id of ids) {
      const ins = await insertIfMissing(id);
      if (ins) inserted++; else skipped++;
    }

    console.log(`page ${p}: ids=${ids.size}, inserted=${inserted}, skipped=${skipped}`);
    await wait(PAUSE_MS);
  }

  console.log(`OK -> live crawl pages=${PAGES}, total_ids=${total}, inserted=${inserted}, skipped=${skipped}`);
  await pg.end();
})().catch(e => { console.error('ERROR ->', e.stack || e.message); process.exit(2); });
