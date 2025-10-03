import { Client } from 'pg';
import { request } from 'undici';
import { gzipSync } from 'node:zlib';

const SRC = 'uk_cf';
const BASE = 'https://www.contractsfinder.service.gov.uk';
const N = Number(process.argv[2] || '10');

// PG client (accept Supabase cert)
const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search = '';
const pg = new Client({ connectionString: u.toString(), ssl: { require: true, rejectUnauthorized: false } });

function extractTitle(html) {
  const m = html.match(/<title[^>]*>([^<]{1,200})<\/title>/i);
  return m ? m[1].trim() : '(no title)';
}
function descr(html) {
  return html.replace(/\s+/g,' ').slice(0,240);
}

(async () => {
  await pg.connect();
  try {
    const { rows: ids } = await pg.query(`
      SELECT source_id
      FROM public.uk_staging_std
      WHERE source=$1
      ORDER BY ingested_at DESC
      LIMIT $2
    `, [SRC, N]);

    let ok = 0;
    for (const { source_id: sid } of ids) {
      const url = `${BASE}/notice/${sid}`;
      const res = await request(url, { method: 'GET' });
      const status = res.statusCode;
      const html = await res.body.text();
      const html_gz = gzipSync(Buffer.from(html));

      const { rows: [rawRow] } = await pg.query(`
        INSERT INTO public.uk_raw_std
          (source,  source_id, url,  kind,     mime,       status_code, attachments, html_gz,       content_hash, notes)
        VALUES
          ($1,      $2,        $3,   'detail', 'text/html', $4,         '{}'::jsonb, $5,            NULL,         'backfill detail (gz)')
        RETURNING raw_id
      `, [SRC, sid, url, status, html_gz]);

      const title = extractTitle(html);
      const short_desc = descr(html);

      await pg.query(`
        UPDATE public.uk_staging_std
        SET title=$1, short_desc=$2, raw_id=$3, ingested_at=now()
        WHERE source=$4 AND source_id=$5
      `, [title, short_desc, rawRow.raw_id, SRC, sid]);

      ok++;
    }

    console.log('OK → backfilled details (gz) for', ok, 'items');
  } catch (e) {
    console.error('ERROR →', e.message);
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
