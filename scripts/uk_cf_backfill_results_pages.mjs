#!/usr/bin/env node
import { setGlobalDispatcher, Agent } from "undici";
setGlobalDispatcher(new Agent({ connect: { rejectUnauthorized: false } }));
// Backfill notices from Contracts Finder listings using ?page= pagination.
// Usage: node scripts/uk_cf_backfill_results_pages.mjs 50
import { Client } from "pg";

const PAGES = parseInt(process.argv[2] || process.env.PAGES || "50", 10);
const DBURL = process.env.DATABASE_URL;
if (!DBURL) {
  console.error("ERROR: DATABASE_URL is not set");
  process.exit(1);
}

const NOTICE_RE = /\/notice\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/gi;

function unique(arr) { return [...new Set(arr)]; }

async function main() {
  const db = new Client({ connectionString: DBURL, ssl: { rejectUnauthorized: false } });
  await db.connect();
  let totalSeen = 0, totalInserted = 0, totalSkipped = 0;

  for (let p = 1; p <= PAGES; p++) {
    const url = `https://www.contractsfinder.service.gov.uk/Search/Results?page=${p}`;
    const res = await fetch(url, { headers: { "User-Agent": "uk-contracts-scraper/1.0" }});
    if (!res.ok) {
      console.error(`WARN: page ${p} HTTP ${res.status}`);
      continue;
    }
    const html = await res.text();
    const ids = unique([...html.matchAll(NOTICE_RE)].map(m => m[1]));
    totalSeen += ids.length;

    // Insert any missing source_id rows (idempotent, avoids ON CONFLICT by NOT EXISTS)
    let inserted = 0, skipped = 0;
    for (const id of ids) {
      const r = await db.query(
        `INSERT INTO public.uk_staging_std
           (uk_uid, source, source_id, source_url, notice_type, nuts)
         SELECT
           CONCAT('uk_cf:', $1),
           'uk_cf', $1,
           CONCAT('https://www.contractsfinder.service.gov.uk/notice/', $1),
           'tender',
           'UK'
         WHERE NOT EXISTS (
           SELECT 1 FROM public.uk_staging_std s
           WHERE s.source = 'uk_cf' AND s.source_id = $1
         )`,
        [id]
      );
      if (r.rowCount === 1) inserted++; else skipped++;
    }
    totalInserted += inserted; totalSkipped += skipped;
    console.log(`page ${p}: ids=${ids.length}, inserted=${inserted}, skipped=${skipped}`);
  }

  console.log(`OK -> backfill via ?page=: pages=${PAGES}, ids_seen=${totalSeen}, inserted=${totalInserted}, skipped=${totalSkipped}`);
  await db.end();
}

main().catch(e => { console.error(e); process.exit(1); });
