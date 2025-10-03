import { Client } from 'pg';
import { request } from 'undici';

const SRC  = 'uk_cf';
const BASE = 'https://www.contractsfinder.service.gov.uk';
const PAGES = Number(process.argv[2] || '5');   // how many pages to pull
const PER_PAGE_CAP = Number(process.argv[3] || '100'); // safety cap per page

// PG client (accept Supabase cert)
const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search = '';
const pg = new Client({ connectionString: u.toString(), ssl: { require: true, rejectUnauthorized: false } });

// crude but effective: catch /notice/<uuid> on results pages
const hrefRe = /\/notice\/[0-9a-f-]{30,}\b/gi;

(async () => {
  await pg.connect();
  let total = 0;
  try {
    for (let p = 1; p <= PAGES; p++) {
      const url = `${BASE}/Search/Results?p=${p}`;
      const res = await request(url, { method: 'GET' });
      const html = await res.body.text();

      const ids = [...new Set((html.match(hrefRe) || []).map(h => h.trim().split('/').pop()))]
        .slice(0, PER_PAGE_CAP);

      for (const sid of ids) {
        const noticeUrl = `${BASE}/notice/${sid}`;

        // RAW select-if-exists, else insert
        let rawRow = (await pg.query(
          'SELECT raw_id, source_id, url FROM public.uk_raw_std WHERE source=$1 AND source_id=$2',
          [SRC, sid]
        )).rows[0];

        if (!rawRow) {
          rawRow = (await pg.query(
            `INSERT INTO public.uk_raw_std
               (source,  source_id, url,        kind,      mime,       status_code, attachments, content_hash, notes)
             VALUES
               ($1,      $2,        $3,         'listing', 'text/html', 200,        '{}'::jsonb, NULL,         'found via paged results')
             RETURNING raw_id, source_id, url`,
            [SRC, sid, noticeUrl]
          )).rows[0];
        }

        // deterministic uk_uid = source:source_id
        const uk_uid = `${SRC}:${sid}`;
        const exists = await pg.query('SELECT 1 FROM public.uk_staging_std WHERE uk_uid=$1 LIMIT 1', [uk_uid]);
        if (exists.rowCount === 0) {
          await pg.query(
            `INSERT INTO public.uk_staging_std
               (uk_uid,  source,  source_id, source_url, raw_id, ingested_at,
                title,                           short_desc,           buyer_name, buyer_country, cpv_codes, published_at, deadline, notice_type, currency,
                value_min, value_max, value_text, contact_name, contact_email, nuts)
             VALUES
               ($1,      $2,      $3,        $4,       $5,    now(),
                '(listing) Contracts Finder notice', 'stub from results page', 'Unknown',  'GB',         NULL::text[], now(),       NULL,      'tender',   'GBP',
                NULL,     NULL,     NULL,      NULL,       NULL,        'UK')`,
            [uk_uid, SRC, rawRow.source_id, rawRow.url, rawRow.raw_id]
          );
        }
      }

      total += ids.length;
      console.log(`page ${p}: found ${ids.length} notices`);
    }
    console.log(`OK → processed ~${total} listings across ${PAGES} page(s)`);
  } catch (e) {
    console.error('ERROR →', e.message);
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
