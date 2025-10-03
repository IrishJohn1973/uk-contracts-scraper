import { Client } from 'pg';

const raw = process.env.DATABASE_URL;
if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }

// Strip query params (e.g., sslmode=require) so our ssl object takes effect
const u = new URL(raw);
u.search = '';

const client = new Client({
  connectionString: u.toString(),
  ssl: { require: true, rejectUnauthorized: false },
});

(async () => {
  try {
    await client.connect();
    const r = await client.query('select current_database() as db, now() as ts;');
    console.log('DB OK →', r.rows[0]);
  } catch (e) {
    console.error('DB ERROR →', e.message);
    process.exit(2);
  } finally {
    await client.end();
  }
})();
