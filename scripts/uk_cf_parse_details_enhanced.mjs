#!/usr/bin/env node
// Minimal “enhanced” parser for UK Contracts Finder detail pages.
// It hydrates title / buyer_name / short_desc from archived HTML.
// Usage: node scripts/uk_cf_parse_details_enhanced.mjs 5000
import { Client } from "pg";
import zlib from "zlib";
import { load as cheerioLoad } from "cheerio";

const MAX = parseInt(process.argv[2] || process.env.MAX_DETAILS || "2000", 10);
const DBURL = process.env.DATABASE_URL;
if (!DBURL) { console.error("ERROR: DATABASE_URL is not set"); process.exit(1); }

function inflateMaybe(buf) {
  if (!buf) return "";
  try { return zlib.gunzipSync(buf).toString("utf8"); } catch { return ""; }
}

function trim240(s) {
  if (!s) return null;
  const t = s.replace(/\s+/g, " ").trim();
  return t ? t.slice(0, 240) : null;
}

function extractFields(html) {
  const $ = cheerioLoad(html);

  // title
  let title = $("h1").first().text().trim();
  if (!title) title = $("title").first().text().trim() || null;

  // buyer_name — Contracts Finder detail pages often have a “Organisation name” field
  let buyer = null;
  $("dt,th,label,strong").each((_, el) => {
    const key = $(el).text().toLowerCase().replace(/\s+/g, " ").trim();
    if (!buyer && key.includes("organisation name")) {
      // Prefer sibling dd/td text
      const dd = $(el).next("dd,td");
      const v = (dd.text() || "").trim();
      if (v) buyer = v;
    }
  });
  if (!buyer) {
    // Fallback: pick the first plausible organisation link text
    const guess = $("a[href*='/organization/'], a[href*='/organisation/']").first().text().trim();
    if (guess) buyer = guess;
  }

  // desc — conservative: body text trimmed to 240 chars (we already store gzipped HTML in RAW)
  const short_desc = trim240($("body").text());

  return { title: title || null, buyer_name: buyer || null, short_desc };
}

async function main() {
  const db = new Client({ connectionString: DBURL, ssl: { rejectUnauthorized: false } });
  await db.connect();

  // Pull recent detail HTML (join via raw_id we already set on staging)
  const { rows } = await db.query(
    `SELECT s.source_id, s.uk_uid, s.raw_id, r.html_gz
     FROM public.uk_staging_std s
     JOIN public.uk_raw_std r ON r.raw_id = s.raw_id
     WHERE s.source='uk_cf' AND r.kind='detail' AND r.html_gz IS NOT NULL
     ORDER BY s.ingested_at DESC
     LIMIT $1`, [MAX]
  );

  let updated = 0;
  for (const row of rows) {
    const html = inflateMaybe(row.html_gz);
    if (!html) continue;
    const fields = extractFields(html);

    // Skip if nothing meaningful
    if (!fields.title && !fields.buyer_name && !fields.short_desc) continue;

    const res = await db.query(
      `UPDATE public.uk_staging_std
         SET title = COALESCE(title, $2),
             buyer_name = COALESCE(buyer_name, $3),
             short_desc = COALESCE(short_desc, $4)
       WHERE source='uk_cf' AND source_id=$1`,
      [row.source_id, fields.title, fields.buyer_name, fields.short_desc]
    );
    if (res.rowCount > 0) updated += res.rowCount;
  }

  console.log(`OK → parsed (enhanced) ${rows.length} detail page(s); updated ${updated}`);
  await db.end();
}

main().catch(err => { console.error(err); process.exit(1); });
