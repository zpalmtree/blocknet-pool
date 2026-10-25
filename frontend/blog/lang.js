// Language menu memory and the "this page is available in <language>" banner, ported from the
// seineminer.com site. An external file because the CSP forbids inline scripts; loaded at the top
// of <body> so the banner exists before first paint. English pages carry the offers as a JSON
// block: a saved choice wins, otherwise the first supported browser language decides.
(function () {
  var KEY = 'bntpool:language:preference:v1';
  function remember(code) { try { localStorage.setItem(KEY, code); } catch (e) {} }
  document.addEventListener('click', function (e) {
    var a = e.target.closest && e.target.closest('.langs a[hreflang]');
    if (a) remember(a.getAttribute('hreflang'));
  });
  var data = document.getElementById('blog-lang-offers');
  if (!data) return; // Translated pages keep their language.
  var cfg = JSON.parse(data.textContent);
  var offers = cfg.offers;
  function preferred(languages) {
    for (var i = 0; i < languages.length; i++) {
      var n = String(languages[i]).toLowerCase();
      for (var code in offers) if (code.toLowerCase() === n) return code;
      var base = n.split('-')[0];
      if (base === 'en') return null;
      if (base === 'zh') return /(?:^|-)(?:hant|tw|hk|mo)(?:-|$)/.test(n) ? 'zh-Hant' : 'zh-Hans';
      if (base === 'pt') return 'pt-BR';
      if (offers[base]) return base;
    }
    return null;
  }
  var target = null;
  try {
    var saved = localStorage.getItem(KEY);
    target = saved ? (offers[saved] ? saved : null) : preferred(navigator.languages || [navigator.language || '']);
  } catch (e) { return; } // Unreadable storage: no banner.
  if (!target) return;
  var o = offers[target];
  var bar = document.createElement('aside');
  bar.className = 'lang-suggest';
  bar.setAttribute('dir', 'ltr');
  var text = document.createElement('span');
  text.lang = o.lang; text.dir = o.dir; text.textContent = o.prompt;
  var go = document.createElement('a');
  go.className = 'btn primary'; go.href = o.href + location.search + location.hash;
  go.lang = o.lang; go.dir = o.dir; go.textContent = o.action;
  go.addEventListener('click', function () { remember(target); });
  var stay = document.createElement('button');
  stay.type = 'button'; stay.className = 'btn'; stay.textContent = cfg.keepEnglish;
  stay.addEventListener('click', function () { remember('en'); bar.remove(); });
  var actions = document.createElement('span');
  actions.className = 'lang-suggest-actions';
  actions.append(go, stay);
  bar.append(text, actions);
  document.body.prepend(bar);
})();
