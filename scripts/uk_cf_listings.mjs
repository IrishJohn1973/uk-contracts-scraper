import { Client } from 'pg';
import { request } from 'undici';

const SRC = 'uk_cf';
const BASE = 'https://www.contractsfinder.service.gov.uk';
const LIST = BASE + '/Search/Results';
const limit = Number(process.argv[2] || '10');

// PG client (accept Supabase cert)
const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search = '';
const pg = new Client({ connectionString: u.toString(), ssl: { require: true, rejectUnauthorized: false } });

const hrefRe = /\/notice\/[0-9a-f-]{30,}\b/gi;

(async () => {
  await pg.connect();
  try {
    const res = await request(LIST, { method: 'GET' });
    const html = await res.body.text();

    const hrefs = [...new Set((html.match(hrefRe) || []).map(h => h.trim()))].slice(0, limit);
    const rows = [];

    for (const h of hrefs) {
      const url = BASE + h;
      const sid = h.split('/').pop();

      // RAW: select-if-exists, else insert
      let rawRow = (await pg.query(
        'SELECT raw_id, source_id, url FROM public.uk_raw_std WHERE source=$1 AND source_id=$2',
        [SRC, sid]
      )).rows[0];

      if (!rawRow) {
        rawRow = (await pg.query(
          `INSERT INTO public.uk_raw_std
             (source,  source_id, url,  kind,      mime,       status_code, attachments, content_hash, notes)
           VALUES
             ($1,      $2,        $3,   'listing', 'text/html', 200,        '{}'::jsonb, NULL,         'found via results page')
           RETURNING raw_id, source_id, url`,
          [SRC, sid, url]
        )).rows[0];
      }

      // Deterministic uk_uid per source/source_id
      const uk_uid = `${SRC}:${sid}`;

      // STAGING: insert only if not present
      const exists = await pg.query(
        'SELECT 1 FROM public.uk_staging_std WHERE uk_uid=$1 LIMIT 1',
        [uk_uid]
      );
      if (exists.rowCount === 0) {
        await pg.query(
          `INSERT INTO public.uk_staging_std
             (uk_uid,  source,  source_id, source_url, raw_id, ingested_at,
              title,                           short_desc,          buyer_name, buyer_country, cpv_codes, published_at, deadline, notice_type, currency,
              value_min, value_max, value_text, contact_name, contact_email, nuts)
           VALUES
             ($1,      $2,      $3,        $4,       $5,    now(),
              '(listing) Contracts Finder notice', 'stub from listings', 'Unknown',  'GB',         NULL::text[], now(),      NULL,     'tender',   'GBP',
              NULL,     NULL,     NULL,      NULL,       NULL,        'UK')`,
          [uk_uid, SRC, rawRow.source_id, rawRow.url, rawRow.raw_id]
        );
      }

      rows.push({ source_id: sid, raw_id: rawRow.raw_id, uk_uid });
    }

    console.log('OK → processed', rows.length, 'listing(s)');
    console.log(rows.slice(0, 5));
  } catch (e) {
    console.error('ERROR →', e.message);
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
