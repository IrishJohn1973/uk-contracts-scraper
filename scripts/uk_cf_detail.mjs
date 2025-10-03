import { Client } from 'pg';
import { request } from 'undici';

const SRC = 'uk_cf';
const BASE = 'https://www.contractsfinder.service.gov.uk';
const sid = process.argv[2];
if (!sid) { console.error('Usage: node scripts/uk_cf_detail.mjs <source_id>'); process.exit(1); }

const DETAIL = `${BASE}/notice/${sid}`;

// PG client (accept Supabase cert)
const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search = '';
const pg = new Client({ connectionString: u.toString(), ssl: { require: true, rejectUnauthorized: false } });

function extractTitle(html) {
  const m = html.match(/<title[^>]*>([^<]{1,200})<\/title>/i);
  return m ? m[1].trim() : '(no title)';
}
function descr(html) {
  const txt = html.replace(/\s+/g,' ').slice(0,240);
  return txt;
}

(async () => {
  await pg.connect();
  try {
    const res = await request(DETAIL, { method: 'GET' });
    const status = res.statusCode;
    const html = await res.body.text();

    // Insert RAW detail
    const { rows: [rawRow] } = await pg.query(`
      INSERT INTO public.uk_raw_std
        (source,  source_id, url,    kind,     mime,       status_code, attachments, content_hash, notes)
      VALUES
        ($1,      $2,        $3,     'detail', 'text/html', $4,         '{}'::jsonb, NULL,         'detail fetch')
      RETURNING raw_id
    `, [SRC, sid, DETAIL, status]);

    const title = extractTitle(html);
    const short_desc = descr(html);

    // Update STAGING with title/desc + link to latest raw detail row
    const { rowCount } = await pg.query(`
      UPDATE public.uk_staging_std
      SET title=$1, short_desc=$2, raw_id=$3, ingested_at=now()
      WHERE source=$4 AND source_id=$5
    `, [title, short_desc, rawRow.raw_id, SRC, sid]);

    console.log('OK → detail raw_id', rawRow.raw_id, 'updated staging rows:', rowCount);
  } catch (e) {
    console.error('ERROR →', e.message);
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
