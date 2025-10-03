import { Client } from 'pg';
import { gunzipSync } from 'node:zlib';
import * as cheerio from 'cheerio';

const N = Number(process.argv[2] || '300');
const SRC = 'uk_cf';

const raw = process.env.DATABASE_URL;
if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search='';
const pg = new Client({ connectionString: u.toString(), ssl: { require:true, rejectUnauthorized:false } });

const clean = (s) => (s||'').replace(/\s+/g,' ').trim();

function pickBodyText($) {
  // Prefer the GOV.UK main content column if present
  const candidates = [
    '.govuk-main-wrapper',
    '.govuk-grid-column-two-thirds',
    '#content',
    'main',
    'body'
  ];
  for (const sel of candidates) {
    const el = $(sel);
    if (el.length) {
      const t = el.text();
      if (t && t.trim().length > 40) return clean(t);
    }
  }
  return clean($('body').text() || '');
}

(async () => {
  await pg.connect();
  try {
    const { rows } = await pg.query(`
      SELECT s.source_id, r.html_gz
      FROM public.uk_staging_std s
      JOIN LATERAL (
        SELECT html_gz
        FROM public.uk_raw_std r
        WHERE r.source=s.source AND r.source_id=s.source_id AND r.kind='detail'
        ORDER BY r.inserted_at DESC
        LIMIT 1
      ) r ON TRUE
      WHERE s.source=$1
      ORDER BY s.ingested_at DESC
      LIMIT $2
    `, [SRC, N]);

    let updated = 0;
    for (const row of rows) {
      if (!row.html_gz) continue;
      let html = '';
      try { html = gunzipSync(row.html_gz).toString('utf8'); } catch { continue; }
      const $ = cheerio.load(html);
      const text = pickBodyText($);
      if (!text) continue;

      // Only overwrite if empty or looks like raw HTML was stored
      await pg.query(`
        UPDATE public.uk_staging_std
        SET short_desc = CASE
            WHEN short_desc IS NULL OR short_desc='' OR short_desc ILIKE '<!DOCTYPE%'
              THEN LEFT($2, 2000)
            ELSE short_desc
          END
        WHERE source=$1 AND source_id=$3
      `, [SRC, text, row.source_id]);

      updated++;
    }
    console.log('OK → refreshed short_desc for', updated, 'items');
  } catch (e) {
    console.error('ERROR →', e.message);
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
