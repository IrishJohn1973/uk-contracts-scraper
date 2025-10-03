const RX = /\/notice\/([0-9a-fA-F-]{36})\b/gi;

const patterns = [
  (p)=>`https://www.contractsfinder.service.gov.uk/Search/Results?p=${p}`,
  (p)=>`https://www.contractsfinder.service.gov.uk/Search/Results?show=20&p=${p}`,
  (p)=>`https://www.contractsfinder.service.gov.uk/Search/Results?show=20&p=${p}&status=Open&source=Contracts%20Finder`,
];

const headers = { 'User-Agent': 'uk-contracts-paginate-probe/1.0' };

async function idsFor(url){
  const r = await fetch(url, { headers });
  const html = await r.text();
  const ids = new Set([...html.matchAll(RX)].map(m=>m[1].toLowerCase()));
  return { count: ids.size, ids };
}

(async () => {
  for (let i=0;i<patterns.length;i++){
    const make = patterns[i];
    const r1 = await idsFor(make(1));
    const r2 = await idsFor(make(2));
    const r3 = await idsFor(make(3));

    const union12 = new Set([...r1.ids, ...r2.ids]);
    const union123 = new Set([...union12, ...r3.ids]);

    const same12 = r1.count===r2.count && r1.count>0 && r2.count>0 &&
                   [...r1.ids].every(x=>r2.ids.has(x));
    const same23 = r2.count===r3.count && r2.count>0 && r3.count>0 &&
                   [...r2.ids].every(x=>r3.ids.has(x));

    console.log(`PATTERN ${i+1}`);
    console.log(` p1 ids=${r1.count}, p2 ids=${r2.count}, p3 ids=${r3.count}`);
    console.log(` union(p1..p3)=${union123.size}`);
    console.log(` p1==p2? ${same12}, p2==p3? ${same23}`);
    console.log(` sample p1: ${[...r1.ids].slice(0,5).join(', ')}`);
    console.log('---');
  }
})().catch(e=>{ console.error(e.stack||e.message); process.exit(2); });
