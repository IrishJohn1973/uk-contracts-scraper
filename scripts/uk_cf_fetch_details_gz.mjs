import { Client } from 'pg';
import { gzipSync } from 'node:zlib';

const SRC = 'uk_cf';
const LIMIT = Number(process.argv[2] || '2000');

// PG client (allow Supabase SSL)
const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search='';
const pg = new Client({ connectionString: u.toString(), ssl: { require:true, rejectUnauthorized:false } });

const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));

function buildDetailUrl(source_id, source_url) {
  if (source_url && /\/notice\//i.test(source_url)) return source_url;
  return `https://www.contractsfinder.service.gov.uk/notice/${source_id}`;
}

(async () => {
  await pg.connect();
  try {
    // pick recent STAGING items that don't yet have a detail RAW with html_gz
    const { rows: items } = await pg.query(`
      SELECT s.source_id, s.source_url
      FROM public.uk_staging_std s
      LEFT JOIN LATERAL (
        SELECT 1
        FROM public.uk_raw_std r
        WHERE r.source=s.source
          AND r.source_id=s.source_id
          AND r.kind='detail'
          AND r.html_gz IS NOT NULL
        ORDER BY r.inserted_at DESC
        LIMIT 1
      ) r ON TRUE
      WHERE s.source=$1
        AND r IS NULL
      ORDER BY s.ingested_at DESC
      LIMIT $2
    `, [SRC, LIMIT]);

    let ok=0, fail=0;
    for (const it of items) {
      const url = buildDetailUrl(it.source_id, it.source_url);
      try {
        const resp = await fetch(url, {
          headers: {
            'User-Agent': 'uk-contracts-scraper/1.0 (+https://example.local)',
            'Accept': 'text/html,application/xhtml+xml'
          }
        });
        const status = resp.status;
        const mime = resp.headers.get('content-type') || 'text/html';
        const html = await resp.text();
        const html_gz = gzipSync(Buffer.from(html, 'utf8'));

        await pg.query(
          `INSERT INTO public.uk_raw_std
             (source, source_id, url, kind, mime, status_code, html_gz, attachments, content_hash, notes)
           VALUES ($1,     $2,        $3,  'detail', $4,   $5,          $6,      '{}'::jsonb,  'devhash-fetch', 'uk_cf_fetch_details')
          `,
          [SRC, it.source_id, url, mime, status, html_gz]
        );
        ok++;
      } catch (e) {
        fail++;
        // keep going
      }
      await sleep(250); // be polite
    }

    console.log(`OK → fetched detail HTML for ${ok} item(s); failures: ${fail}; total processed: ${items.length}`);
  } catch (e) {
    console.error('ERROR →', e.message);
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
