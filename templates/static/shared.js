/**
 * shared.js — Engine v2.2 (Refined & Optimized)
 * ════════════════════════════════════════════════════════════════
 * • Performance: DocumentFragment + rAF + Animation Capping
 * • UX: Drag-to-Scroll + Click Prevention + Skeleton Loaders
 * • Search: Multi-token Arabic Normalization
 * ════════════════════════════════════════════════════════════════
 */

/* ── 1. Configuration & Constants ── */
const path = window.location.pathname;
const GROUP = (typeof CONFIG !== 'undefined' && CONFIG.group) 
    ? CONFIG.group 
    : (path.includes('.html') ? path.split('/').pop().replace('.html','') : path.split('/').filter(Boolean).pop());

/* ── 2. Helpers (Normalization & Performance) ── */
const normalizeAr = s => s ? s.replace(/[أإآٱ]/g, 'ا').replace(/ؤ/g, 'و').replace(/ئ/g, 'ي').replace(/ة/g, 'ه').replace(/ى/g, 'ي').toLowerCase() : '';
const debounce = (fn, ms = 150) => { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; };
const fmtSize = b => !b ? '---' : b < 1048576 ? (b / 1024).toFixed(1) + ' KB' : (b / 1048576).toFixed(1) + ' MB';

const getIcon = (cat) => {
    const icons = { 
        'امتحانات': '📝', 'أطفال': '👶', 'نسا': '🤰', 
        'إسعافات': '🚑', 'صحة': '🏘️', 'نفسية': '🧠', 
        'ترمنولوجي': '📖', 'ثقافية': '📚', 'الكل': '🏥' 
    };
    return icons[cat] || '📄';
};

/* ── 3. Core Engine Functions ── */
function renderCards(container, files) {
    requestAnimationFrame(() => {
        const frag = document.createDocumentFragment();
        
        if (!files.length) {
            container.innerHTML = '<div class="empty-state">لا توجد ملفات حالياً.. جرب كلمة بحث أخرى</div>';
            return;
        }

        files.forEach((f, i) => {
            const c = document.createElement('div');
            c.className = 'file-card';
            
            // Performance: Cap animations to first 40 cards to save CPU/GPU
            if (i >= 40) c.style.animation = 'none';
            else c.style.animationDelay = `${i * 0.028}s`;

            c.innerHTML = `
                <div class="file-type-icon">${getIcon(f.category)}</div>
                <div class="file-name">${f.display}</div>
                <div class="file-meta">
                    <span class="file-badge cat-badge" style="background:var(--pbb)">${f.category}</span>
                    <span class="file-badge secondary">📦 ${fmtSize(f.size)}</span>
                </div>
                <div class="file-actions">
                    <a href="/api/stream/${f.id}?group=${GROUP}" target="_blank" class="btn btn-preview">👁 معاينة</a>
                    <a href="/api/stream/${f.id}?group=${GROUP}&dl=1" class="btn btn-download" style="box-shadow: 0 4px 10px var(--pds)">⬇ تحميل</a>
                </div>`;
            frag.appendChild(c);
        });
        
        container.innerHTML = '';
        container.appendChild(frag);
    });
}

/* ── 4. Interaction (Horizontal Drag Logic) ── */
function initDragScroll() {
    const slider = document.querySelector('.cat-grid');
    if (!slider) return;

    let isDown = false, startX, scrollLeft, moved = false;

    slider.addEventListener('mousedown', (e) => {
        isDown = true;
        moved = false;
        slider.classList.add('active-drag');
        startX = e.pageX - slider.offsetLeft;
        scrollLeft = slider.scrollLeft;
    });

    slider.addEventListener('mousemove', (e) => {
        if (!isDown) return;
        const x = e.pageX - slider.offsetLeft;
        const walk = (x - startX) * 2; // Scroll Speed
        if (Math.abs(walk) > 5) moved = true; // Identify as drag if moved > 5px
        slider.scrollLeft = scrollLeft - walk;
    });

    const stopDrag = () => { 
        isDown = false; 
        slider.classList.remove('active-drag'); 
    };
    
    slider.addEventListener('mouseup', stopDrag);
    slider.addEventListener('mouseleave', stopDrag);

    // Prevent accidental clicks on categories while dragging
    slider.addEventListener('click', (e) => {
        if (moved) {
            e.preventDefault();
            e.stopImmediatePropagation();
        }
    }, true);
}

/* ── 5. Main Application Logic ── */
const GradeApp = {
    init(CONFIG) {
        // Theme injection via CSS variables
        if (CONFIG.theme) {
            const s = document.documentElement.style;
            s.setProperty('--pg', CONFIG.theme.primary);
            s.setProperty('--pab', CONFIG.theme.activeBg);
            s.setProperty('--pas', CONFIG.theme.activeShadow);
            s.setProperty('--pbb', CONFIG.theme.badgeBg);
            s.setProperty('--pds', CONFIG.theme.dlShadow);
        }

        // Pre-compile category detection for faster filtering
        const compiledMapping = Object.entries(CONFIG.mapping).map(([cat, keys]) => [
            cat, keys.map(normalizeAr)
        ]);

        const detect = (norm) => {
            for (const [cat, keys] of compiledMapping) {
                if (keys.some(k => norm.includes(k))) return cat;
            }
            return 'أخري';
        };

        let all = [], currentCat = 'الكل';
        const list = document.getElementById('fileList');
        const search = document.getElementById('searchInput');
        const catCards = document.querySelectorAll('.cat-card');

        const render = () => {
            const query = normalizeAr(search.value).trim();
            const tokens = query.split(/\s+/).filter(Boolean);
            
            const filtered = all.filter(f => {
                if (currentCat !== 'الكل' && f.category !== currentCat) return false;
                return tokens.every(t => f.norm.includes(t));
            });
            renderCards(list, filtered);
        };

        // UI Events
        catCards.forEach(c => c.addEventListener('click', () => {
            currentCat = c.dataset.cat;
            catCards.forEach(card => card.classList.toggle('active', card.dataset.cat === currentCat));
            render();
        }));

        search.addEventListener('input', debounce(render, 150));

        // Initial Setup
        list.innerHTML = Array(6).fill('<div class="skeleton-card"></div>').join('');
        initDragScroll();

        // Data Fetching
        fetch(`/api/files?group=${GROUP}`)
            .then(r => r.json())
            .then(files => {
                all = files.map(f => {
                    const display = f.name.replace(/قناة\s*|تمريضيانو\s*|[\-_]+/g, ' ').trim();
                    const norm = normalizeAr(display);
                    return { ...f, display, category: detect(norm), norm };
                });
                render();
            })
            .catch(() => {
                list.innerHTML = '<div class="empty-state">فشل تحميل الملفات. تحقق من الاتصال بالخادم.</div>';
            });
    }
};