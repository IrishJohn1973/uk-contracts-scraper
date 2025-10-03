import { Client } from 'pg';
import { request } from 'undici';
import { createHash } from 'node:crypto';

// PG client (accept Supabase cert)
const raw = process.env.DATABASE_URL;
if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search = '';
const pg = new Client({ connectionString: u.toString(), ssl: { require: true, rejectUnauthorized: false } });

const SRC = 'uk_cf';
const LISTING_URL = 'https://www.contractsfinder.service.gov.uk/';

(async () => {
  await pg.connect();
  try {
    // 1) Fetch a simple page (homepage is enough for smoke)
    const res = await request(LISTING_URL, { method: 'GET' });
    const status = res.statusCode;
    const html = await res.body.text();

    // 2) Insert RAW (allowed kind='listing')
    const contentHash = createHash('sha256').update(html).digest('hex');
    const { rows: [rawRow] } = await pg.query(`
      INSERT INTO public.uk_raw_std
        (source,  source_id, url,          kind,      mime,       status_code, attachments, content_hash, notes)
      VALUES
        ($1,      $2,        $3,           'listing', 'text/html', $4,         '{}'::jsonb, $5,           $6)
      RETURNING raw_id, url, source_id
    `, [SRC, `SMOKE-${Date.now()}`, LISTING_URL, status, contentHash, 'uk_cf homepage smoke']);

    // 3) Promote minimal STAGING row (matches schema)
    const { rows: [stageRow] } = await pg.query(`
      INSERT INTO public.uk_staging_std
        (uk_uid,                                     source,  source_id,     source_url,  raw_id,      ingested_at,
         title,            short_desc,     buyer_name,     buyer_country, cpv_codes, published_at, deadline, notice_type, currency,
         value_min, value_max, value_text, contact_name, contact_email, nuts)
      VALUES
        ($1,                                          $2,      $3,             $4,         $5,         now(),
         $6,               $7,             $8,             $9,            NULL::text[], now(),       NULL,    'tender',   'GBP',
         NULL,     NULL,     NULL,        NULL,       NULL,        $10)
      RETURNING uk_uid, raw_id
    `, [
      `${SRC}:${new Date().toISOString().replace(/\D/g,'').slice(0,14)}:${rawRow.raw_id}`,
      SRC, rawRow.source_id, rawRow.url, rawRow.raw_id,
      'UK Contracts — Smoke Notice',
      'Fetched homepage; full scraper TBD',
      'Example Buyer', 'GB', 'UK'
    ]);

    console.log('OK → RAW', rawRow);
    console.log('OK → STAGING', stageRow);
  } catch (e) {
    console.error('ERROR →', e.message);
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
