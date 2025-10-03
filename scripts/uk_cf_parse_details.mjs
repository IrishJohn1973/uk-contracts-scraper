import { Client } from 'pg';
import { gunzipSync } from 'node:zlib';
import * as cheerio from 'cheerio';

const SRC = 'uk_cf';
const N   = Number(process.argv[2] || '300');

// PG client (accept Supabase cert)
const raw = process.env.DATABASE_URL; if (!raw) { console.error('No DATABASE_URL'); process.exit(1); }
const u = new URL(raw); u.search = '';
const pg = new Client({ connectionString: u.toString(), ssl: { require: true, rejectUnauthorized: false } });

// ---------- helpers ----------
const normKey = (s) => s.toLowerCase().replace(/\s+/g,' ').trim();
const txt = (html) => {
  const $ = cheerio.load('<root>'+ (html||'') +'</root>');
  return $('root').text().replace(/\s+/g,' ').trim();
};

const DATE_LABELS = [
  'published date','publication date','published','date published','date of publication',
  'closing date','deadline','closing time','response deadline','submission deadline','closing','closing date and time'
];

const BUYER_LABELS = [
  'organisation','organisation name','buyer','contracting authority','procuring entity'
];

const VALUE_LABELS = [
  'awarded value','value of contract','total value','estimated value','contract value',
  'estimated contract value','award value','contract value (exclusive of vat)','contract value (exclusive of vat) £'
];

const CPV_LABELS = [
  'cpv code','cpv codes','common procurement vocabulary'
];

function extractSummaryMap($) {
  const map = new Map();

  $('.govuk-summary-list__row').each((_, row) => {
    const key = normKey($(row).find('.govuk-summary-list__key').text());
    const val = txt($(row).find('.govuk-summary-list__value').html());
    if (key) map.set(key, val);
  });

  $('dt,th').each((_, el) => {
    const key = normKey($(el).text());
    if (!key) return;
    let val = '';
    const dd = $(el).next('dd');
    const td = $(el).next('td');
    if (dd.length) val = txt(dd.html() || dd.text());
    else if (td.length) val = txt(td.html() || td.text());
    if (val) map.set(key, val);
  });

  return map;
}

function findFirst(map, labels) {
  for (const label of labels) {
    if (map.has(label)) return map.get(label);
  }
  for (const [k,v] of map.entries()) {
    if (labels.some(l => k.includes(l))) return v;
  }
  return null;
}

function monthNum(mmm) {
  const ix = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].indexOf(String(mmm).slice(0,3).toLowerCase());
  return ix >= 0 ? ix+1 : 1;
}

function toIsoMaybe(s) {
  if (!s) return null;
  s = s.replace(/&nbsp;/g,' ').replace(/\u00A0/g,' ').trim();
  s = s.replace(/(\d)(am|pm)\b/i, '$1 $2');

  const mk = (y,m,d,hh,mm,ampm) => {
    let H = Number(hh ?? 12);
    const M = Number(mm ?? 0);
    if (ampm) {
      const ap = ampm.toLowerCase();
      if (ap === 'pm' && H < 12) H += 12;
      if (ap === 'am' && H === 12) H = 0;
    }
    if (hh == null && mm == null) { H = 12; }
    return new Date(Date.UTC(y, m-1, d, H, M, 0)).toISOString();
  };

  // DD Mon YYYY [HH:MM] [AM|PM]
  let m = s.match(/\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(20\d{2})(?:\s+(\d{1,2}):(\d{2})\s*(am|pm)?)?/i);
  if (m) return mk(Number(m[3]), monthNum(m[2]), Number(m[1]), m[4], m[5], m[6]);

  // DD Mon YYYY [HH][AM|PM]
  m = s.match(/\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(20\d{2})\s+(\d{1,2})\s*(am|pm)\b/i);
  if (m) return mk(Number(m[3]), monthNum(m[2]), Number(m[1]), m[4], '00', m[5]);

  // YYYY-MM-DD [HH:MM] [AM|PM]
  m = s.match(/\b(20\d{2})[-\/](\d{1,2})[-\/](\d{1,2})(?:\s+(\d{1,2}):(\d{2})\s*(am|pm)?)?/i);
  if (m) return mk(Number(m[1]), Number(m[2]), Number(m[3]), m[4], m[5], m[6]);

  // DD/MM/YYYY or DD-MM-YYYY [HH:MM] [AM|PM]
  m = s.match(/\b(\d{1,2})[-\/](\d{1,2})[-\/](20\d{2})(?:\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?)?/i);
  if (m) return mk(Number(m[3]), Number(m[2]), Number(m[1]), m[4], m[5], m[6]);

  return null;
}

function parseMoney(s) {
  if (!s) return { text:null, min:null, max:null };
  const m = s.match(/£\s*([\d,\s]+(?:\.\d{1,2})?)/);
  if (!m) return { text:null, min:null, max:null };
  const text = m[0].replace(/\s+/g,' ');
  const num = Number(m[1].replace(/[,\s]/g,''));
  return { text, min:null, max: Number.isFinite(num) ? num : null };
}

function noticeTypeHeuristic(html) {
  const h = html.toLowerCase();
  if (h.includes('contract award') || h.includes('awarded supplier') || h.includes('awarded to') || h.includes('winning supplier')) return 'award';
  return 'tender';
}

function regexNeighbor(html, labelList) {
  for (const lab of labelList) {
    const re = new RegExp(`${lab}\\s*</[^>]+>\\s*<[^>]*>([\\s\\S]{1,400}?)</`, 'i');
    const m = html.match(re);
    if (m) return txt(m[1]);
  }
  return null;
}

function findCpvsFrom(html, seed='') {
  const set = new Set();
  const src = (seed || '') + ' ' + html;
  let m; const re = /\b(\d{8})\b/g;
  while ((m = re.exec(src))) set.add(m[1]);
  return Array.from(set).slice(0,10);
}

// ---------- main ----------
(async () => {
  await pg.connect();
  try {
    const { rows: items } = await pg.query(`
      SELECT s.source_id, r.raw_id, r.html_gz
      FROM public.uk_staging_std s
      JOIN LATERAL (
        SELECT raw_id, html_gz
        FROM public.uk_raw_std r
        WHERE r.source=s.source AND r.source_id=s.source_id AND r.kind='detail'
        ORDER BY r.inserted_at DESC
        LIMIT 1
      ) r ON TRUE
      WHERE s.source=$1
      ORDER BY s.ingested_at DESC
      LIMIT $2
    `, [SRC, N]);

    let ok=0;
    for (const it of items) {
      if (!it.html_gz) continue;
      let html; try { html = gunzipSync(it.html_gz).toString('utf8'); } catch { continue; }
      const $ = cheerio.load(html, { decodeEntities: true });

      const map = extractSummaryMap($);

      // BUYER
      let buyer = findFirst(map, BUYER_LABELS) || regexNeighbor(html, BUYER_LABELS) || null;

      // DATES — labels first
      let published_raw = findFirst(map, ['published date','publication date','published','date published','date of publication']);
      let deadline_raw  = findFirst(map, ['closing date','deadline','closing time','response deadline','submission deadline','closing','closing date and time']);

      // Regex neighbours if blank
      if (!published_raw) published_raw = regexNeighbor(html, ['Published date','Publication date','Published','Date published','Date of publication']);
      if (!deadline_raw)  deadline_raw  = regexNeighbor(html, ['Closing date','Deadline','Closing time','Response deadline','Submission deadline','Closing','Closing date and time']);

      // “Closing: …” inline
      if (!deadline_raw) {
        let mC = html.match(/Closing:\s*<\/[^>]+>\s*<[^>]*>\s*([^<]{6,80})</i);
        if (mC) deadline_raw = mC[1];
      }

      // Hidden ISO in scripts / meta
      if (!published_raw || !deadline_raw) {
        const scripts = $('script').map((_,s)=>$(s).html()||'').get().join('\n');
        const iso = scripts.match(/\b20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b/g) || [];
        if (!published_raw && iso.length) published_raw = iso[0];
        if (!deadline_raw  && iso.length>1) deadline_raw = iso[1];
        if (!published_raw) {
          const metaPub = $('meta[property="article:published_time"]').attr('content') || $('meta[name="date"]').attr('content');
          if (metaPub) published_raw = metaPub;
        }
      }

      const published_at = toIsoMaybe(published_raw);
      const deadline     = toIsoMaybe(deadline_raw);

      // NOTICE TYPE
      let notice_type = noticeTypeHeuristic(html);

      // VALUE
      let value_field = findFirst(map, VALUE_LABELS)
        || regexNeighbor(html, ['Contract value','Estimated value','Value of contract','Total value','Awarded value']);
      const { text:value_text, min:value_min, max:value_max } = parseMoney(value_field || Array.from(map.values()).join(' ').slice(0, 100000));

      // CPVs
      const cpv_field = findFirst(map, CPV_LABELS) || '';
      const cpv_codes = findCpvsFrom(html, cpv_field);

      await pg.query(`
        UPDATE public.uk_staging_std
        SET buyer_name   = COALESCE(NULLIF($2,''), buyer_name),
            published_at = COALESCE($3::timestamptz, published_at),
            deadline     = COALESCE($4::timestamptz, deadline),
            notice_type  = COALESCE($5, notice_type),
            value_text   = COALESCE($6, value_text),
            value_min    = COALESCE($7, value_min),
            value_max    = COALESCE($8, value_max),
            cpv_codes    = CASE WHEN $9::text[] IS NULL OR array_length($9,1)=0 THEN cpv_codes ELSE $9 END,
            buyer_country= COALESCE(buyer_country, 'GB'),
            currency     = COALESCE(currency, 'GBP'),
            nuts         = COALESCE(nuts, 'UK')
      WHERE source=$1 AND source_id=$10
      `, [SRC, buyer, published_at, deadline, notice_type, value_text, value_min, value_max, cpv_codes, it.source_id]);

      ok++;
    }

    console.log('OK → parsed (hybrid)', ok, 'detail page(s)');
  } catch (e) {
    console.error('ERROR →', e.message);
    process.exit(2);
  } finally {
    await pg.end();
  }
})();
