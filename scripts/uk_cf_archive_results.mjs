import { Client } from 'pg';
import { request } from 'undici';
import { gzipSync } from 'node:zlib';

const SRC   = 'uk_cf';
const BASE  = 'https://www.contractsfinder.service.gov.uk/Search/Results';
const PAGES = Number(process.argv[2] || '5');

// PG client (accept Supabase cert)
const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search = '';
const pg = new Client({ connectionString: u.toString(), ssl: { require: true, rejectUnauthorized: false } });

(async () => {
  await pg.connect();
  try {
    let ok = 0;
    for (let p = 1; p <= PAGES; p++) {
      const url = BASE + '?p=' + p;
      const res = await request(url, { method: 'GET' });
      const status = res.statusCode;
      const html = await res.body.text();
      const html_gz = gzipSync(Buffer.from(html));

      await pg.query(
        `INSERT INTO public.uk_raw_std
           (source, source_id,     url,  kind,      mime,       status_code, attachments, html_gz,       content_hash, notes)
         VALUES
           ($1,     $2,            $3,   'listing', 'text/html', $4,         '{}'::jsonb, $5,            NULL,         'results page archive')`,
        [SRC, 'results:p=' + p, url, status, html_gz]
      );

      ok++;
      console.log('archived results page ' + p);
    }
    console.log('OK → archived ' + ok + ' results pages');
  } catch (e) {
    console.error('ERROR →', e.message);
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
