/**
 * shared.js — Engine v2
 * ════════════════════════════════════════════════════════════════
 *  • Arabic normalization (alef/hamza/ta-marbuta) so أحياء=احياء
 *  • Multi-token AND search (all words must match)
 *  • Category detection on normalized text — pre-compiled at load
 *  • DocumentFragment + rAF render; animation only on first 40 cards
 *  • Theme injection via CSS custom properties (per-page colors)
 *  • Debounce 150 ms for instant feel
 * ════════════════════════════════════════════════════════════════
 */

/* ── Arabic normalizer ───────────────────────────────────────────────────── */
function normalizeAr(s) {
  return s
    .replace(/[أإآٱ]/g, 'ا')
    .replace(/ؤ/g, 'و')
    .replace(/ئ/g, 'ي')
    .replace(/ة/g, 'ه')
    .replace(/ى/g, 'ي')
    .toLowerCase();
}

/* ── Debounce ────────────────────────────────────────────────────────────── */
function debounce(fn, ms = 150) {
  let t;
  return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

/* ── Format bytes ────────────────────────────────────────────────────────── */
function fmtSize(b) {
  if (!b) return '---';
  return b < 1_048_576 ? (b / 1024).toFixed(1) + ' KB' : (b / 1_048_576).toFixed(1) + ' MB';
}

/* ── Clean display name (noise words removed) ────────────────────────────── */
function cleanFileName(raw) {
  return raw
    .replace(/قناة\s*/g, '')
    .replace(/تمريضيانو\s*/g, '')
    .replace(/[\-_]+/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

/* ── Category detector — compiled once at startup ────────────────────────── */
function makeDetector(mapping) {
  const compiled = Object.entries(mapping).map(([cat, keys]) => [
    cat, keys.map(normalizeAr),
  ]);
  return function detect(normName) {
    for (const [cat, keys] of compiled) {
      if (keys.some(k => normName.includes(k))) return cat;
    }
    return 'أخري';
  };
}

/* ── Preprocess all files once ───────────────────────────────────────────── */
function processFiles(files, detect) {
  return files.map(f => {
    const display = cleanFileName(f.name);
    const norm    = normalizeAr(display);
    return { ...f, display, category: detect(norm), norm };
  });
}

/* ── Multi-token AND filter ──────────────────────────────────────────────── */
function filterFiles(list, cat, query) {
  const tokens = normalizeAr(query).trim().split(/\s+/).filter(Boolean);
  return list.filter(f => {
    if (cat !== 'الكل' && f.category !== cat) return false;
    return tokens.every(t => f.norm.includes(t));
  });
}

/* ── Render (DocumentFragment, rAF, capped animation) ───────────────────── */
function renderCards(container, files) {
  requestAnimationFrame(() => {
    const frag = document.createDocumentFragment();
    if (!files.length) {
      const d = document.createElement('div');
      d.className   = 'empty-state';
      d.textContent = 'لا توجد ملفات في هذا القسم.. جرب كلمة بحث أخرى';
      frag.appendChild(d);
    } else {
      files.forEach((f, i) => {
        const c = document.createElement('div');
        c.className = 'file-card';
        if (i >= 40) c.style.animation = 'none';
        else         c.style.animationDelay = (i * 0.028) + 's';
        c.innerHTML =
          `<div class="file-type-icon">📄</div>` +
          `<div class="file-name">${f.display}</div>` +
          `<div class="file-meta">` +
            `<span class="file-badge cat-badge">القسم: ${f.category}</span>` +
            `<span class="file-badge secondary">📦 ${fmtSize(f.size)}</span>` +
          `</div>` +
          `<div class="file-actions">` +
            `<a href="/api/stream/${f.id}" target="_blank" class="btn btn-preview">👁 معاينة</a>` +
            `<a href="/api/stream/${f.id}?dl=1" class="btn btn-download">⬇ تحميل</a>` +
          `</div>`;
        frag.appendChild(c);
      });
    }
    container.innerHTML = '';
    container.appendChild(frag);
  });
}

/* ── API ─────────────────────────────────────────────────────────────────── */
async function fetchFiles() {
  const r = await fetch('/api/files');
  if (!r.ok) throw new Error('HTTP ' + r.status);
  return r.json();
}
async function triggerRefresh(btn) {
  const orig = btn.innerHTML;
  btn.innerHTML = '⌛'; btn.disabled = true;
  try {
    const r = await fetch('/api/refresh', { method: 'POST' });
    const d = await r.json();
    return d.files || [];
  } finally { btn.innerHTML = orig; btn.disabled = false; }
}

/* ── Theme applicator ────────────────────────────────────────────────────── */
function applyTheme({ primary, activeBg, activeShadow, headerGrad, badgeBg, dlShadow }) {
  const s = document.documentElement.style;
  s.setProperty('--pg',  primary);       // page-primary color
  s.setProperty('--pab', activeBg);      // cat active background
  s.setProperty('--pas', activeShadow);  // cat active shadow
  s.setProperty('--phg', headerGrad);    // header overlay gradient
  s.setProperty('--pbb', badgeBg);       // badge background
  s.setProperty('--pds', dlShadow);      // download btn shadow
}

/* ── GradeApp ────────────────────────────────────────────────────────────── */
const GradeApp = {
  init(CONFIG) {
    if (CONFIG.theme) applyTheme(CONFIG.theme);

    let all = [], currentCat = 'الكل';
    const detect    = makeDetector(CONFIG.mapping);
    const list      = document.getElementById('fileList');
    const search    = document.getElementById('searchInput');
    const refreshBtn= document.getElementById('refreshBtn');
    const catCards  = document.querySelectorAll('.cat-card');

    const render = () => renderCards(list, filterFiles(all, currentCat, search.value));

    const setCat = cat => {
      currentCat = cat;
      catCards.forEach(c => c.classList.toggle('active', c.dataset.cat === cat));
      render();
    };

    catCards.forEach(c => c.addEventListener('click', () => setCat(c.dataset.cat)));
    search.addEventListener('input', debounce(render, 150));

    refreshBtn.addEventListener('click', async () => {
      try { all = processFiles(await triggerRefresh(refreshBtn), detect); render(); }
      catch { alert('فشلت المزامنة!'); }
    });

    /* About modal */
    const modal = document.getElementById('aboutModal');
    if (modal) {
      window.toggleAbout = () => {
        const card = modal.querySelector('.about-card');
        if (modal.style.display === 'flex') {
          card.classList.remove('show');
          setTimeout(() => (modal.style.display = 'none'), 300);
        } else {
          modal.style.display = 'flex';
          setTimeout(() => card.classList.add('show'), 10);
        }
      };
      modal.addEventListener('click', e => { if (e.target === modal) toggleAbout(); });
    }

    /* Load */
    list.innerHTML = '<div class="empty-state" style="opacity:.35">جارٍ التحميل...</div>';
    fetchFiles()
      .then(files => { all = processFiles(files, detect); render(); })
      .catch(() => {
        list.innerHTML = '<div class="empty-state">فشل تحميل الملفات. تحقق من الاتصال.</div>';
      });
  },
};
