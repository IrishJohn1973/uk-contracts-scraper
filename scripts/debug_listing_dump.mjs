import { Client } from 'pg';
import { gunzipSync } from 'node:zlib';
import fs from 'node:fs';

const pg = new Client({ connectionString: process.env.DATABASE_URL, ssl: { require: true, rejectUnauthorized: false } });

(async () => {
  await pg.connect();
  const { rows } = await pg.query(`
    SELECT raw_id, url, html_gz
    FROM public.uk_raw_std
    WHERE source='uk_cf' AND kind='listing' AND html_gz IS NOT NULL
    ORDER BY inserted_at DESC
    LIMIT 1
  `);

  if (!rows.length) {
    console.error('No listing html_gz in RAW');
    process.exit(2);
  }

  const r = rows[0];
  const html = gunzipSync(r.html_gz).toString('utf8');
  const matches = [...html.matchAll(/\/notice\/[0-9a-fA-F-]{36}\b/g)];
  console.log('RAW_ID:', r.raw_id);
  console.log('URL:', r.url);
  console.log('notice-link count:', matches.length);
  fs.writeFileSync('debug-listing.html', html);
  await pg.end();
})();
