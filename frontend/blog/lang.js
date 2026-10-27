// Language menu memory and automatic language selection, ported from the seineminer.com site. An
// external file because the CSP forbids inline scripts; loaded at the top of <body> so the switch
// happens before the English page renders. English pages list their translations as a JSON block:
// a saved choice wins, otherwise the first supported browser language decides.
(function () {
  var KEY = 'bntpool:language:preference:v1';
  function remember(code) { try { localStorage.setItem(KEY, code); } catch (e) {} }
  document.addEventListener('click', function (e) {
    var a = e.target.closest && e.target.closest('.langs a[hreflang]');
    if (a) remember(a.getAttribute('hreflang'));
    document.querySelectorAll('.langs[open]').forEach(function (menu) {
      if (!menu.contains(e.target)) menu.open = false;
    });
  });
  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Escape') return;
    document.querySelectorAll('.langs[open]').forEach(function (menu) {
      if (menu.contains(document.activeElement)) menu.querySelector('summary').focus();
      menu.open = false;
    });
  });
  var data = document.getElementById('blog-lang-offers');
  if (!data) return; // Translated pages keep their language.
  var offers = JSON.parse(data.textContent).offers;
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
  } catch (e) {
    target = preferred(navigator.languages || [navigator.language || '']);
  }
  if (target && offers[target]) location.replace(offers[target] + location.search + location.hash);
})();
