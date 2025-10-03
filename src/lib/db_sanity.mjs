import 'dotenv/config';
import { Client } from 'pg';
const url = process.env.DATABASE_URL;
if (!url) { console.error('ERROR: DATABASE_URL not set (.env)'); process.exit(1); }
const client = new Client({ connectionString: url, ssl: { require: true, rejectUnauthorized: false } });
(async () => {
  try {
    await client.connect();
    const r = await client.query('select version();');
    console.log('DB OK →', r.rows[0].version);
  } catch (e) {
    console.error('DB ERROR →', e.message);
    process.exit(2);
  } finally {
    await client.end();
  }
})();
