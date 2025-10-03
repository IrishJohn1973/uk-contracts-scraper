import { Client } from 'pg';
import { gunzipSync } from 'node:zlib';

const SRC = 'uk_cf';
const LIMIT_PAGES = Number(process.argv[2] || '100000');

const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search='';
const pg = new Client({ connectionString: u.toString(), ssl: { require:true, rejectUnauthorized:false } });

// Case-insensitive UUID capture: /notice/{uuid}
const RX = /\/notice\/([0-9a-fA-F-]{36})\b/gi;

(async () => {
  await pg.connect();
  try {
    // KEY FIX: take the latest html_gz per *page source_id* like 'results:p=NN'
    const { rows } = await pg.query(`
      SELECT DISTINCT ON (source_id) source_id, raw_id, url, html_gz
      FROM public.uk_raw_std
      WHERE source=$1
        AND kind='listing'
        AND html_gz IS NOT NULL
        AND source_id LIKE 'results:p=%'
      ORDER BY source_id, inserted_at DESC
      LIMIT $2
    `, [SRC, LIMIT_PAGES]);

    const ids = new Set();

    for (const r of rows) {
      let html = '';
      try { html = gunzipSync(r.html_gz).toString('utf8'); } catch { continue; }
      RX.lastIndex = 0;               // reset regex state per page
      for (const m of html.matchAll(RX)) ids.add(m[1].toLowerCase());
    }

    let inserted = 0, skipped = 0;
    for (const source_id of ids) {
      const source_url = `https://www.contractsfinder.service.gov.uk/notice/${source_id}`;
      const uk_uid = `${SRC}:${source_id}`;
      const res = await pg.query(
        `INSERT INTO public.uk_staging_std (uk_uid, source, source_id, source_url, notice_type, nuts)
         SELECT $1,$2,$3,$4,'tender','UK'
         WHERE NOT EXISTS (SELECT 1 FROM public.uk_staging_std WHERE source=$2 AND source_id=$3)`,
        [uk_uid, SRC, source_id, source_url]
      );
      if (res.rowCount === 1) inserted++; else skipped++;
    }

    console.log(`OK -> distinct results pages=${rows.length}; unique notice IDs=${ids.size}; inserted=${inserted}, skipped=${skipped}`);
  } catch (e) {
    console.error('ERROR -> ' + (e.stack || e.message));
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
