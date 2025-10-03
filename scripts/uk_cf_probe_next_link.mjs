const headers = { 'User-Agent': 'uk-contracts-paginate-probe/1.0' };
const RX_LINK = /<a\s+[^>]*href="([^"]+)"[^>]*>([^<]+)<\/a>/gi;

(async () => {
  const url = 'https://www.contractsfinder.service.gov.uk/Search/Results';
  const r = await fetch(url, { headers });
  const html = await r.text();

  const links = [];
  let m;
  while ((m = RX_LINK.exec(html))) {
    const href = m[1];
    const text = m[2].trim().toLowerCase();
    if (text === 'next' || /^\d+$/.test(text)) {
      // only show onsite pagination links
      if (href.startsWith('/') || href.startsWith('https://www.contractsfinder.service.gov.uk/')) {
        links.push({text: m[2].trim(), href});
      }
    }
  }

  // unique + printable
  const seen = new Set();
  const uniq = links.filter(l => !seen.has(l.href) && seen.add(l.href));

  console.log('Found pagination-ish links (first 12):');
  for (const l of uniq.slice(0,12)) console.log(`  [${l.text}] -> ${l.href}`);
})();
