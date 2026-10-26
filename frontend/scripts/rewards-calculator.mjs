import { t } from './blog-lib.mjs';
const esc = value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

export function calculatorHtml(locale) {
  const text = key => esc(t(locale, key));
  const runtimeKeys = ['calcEmpty', 'calcInvalid', 'calcLoading', 'calcReady', 'calcError', 'calcStale'];
  const messages = JSON.stringify(Object.fromEntries(runtimeKeys.map(key => [key, t(locale, key)]))).replace(/</g, '\\u003c');
  return `<section id="mining-calculator" class="calculator" aria-labelledby="calculator-title">
<h2 id="calculator-title">${text('calcTitle')}</h2>
<p>${text('calcIntro')}</p>
<noscript><p>${text('calcNoScript')}</p></noscript>
<fieldset disabled><legend class="calc-sr-only">${text('calcTitle')}</legend>
<div class="calc-fields"><div><label for="calc-hashrate">${text('calcHashrate')}</label><input id="calc-hashrate" data-calc="hashrate" type="text" inputmode="decimal" autocomplete="off" dir="ltr" aria-describedby="calc-hint" placeholder="0"></div>
<div><label for="calc-units">${text('calcUnit')}</label><select id="calc-units" data-calc="units" dir="ltr"><option value="1">H/s</option><option value="1000">kH/s</option><option value="1000000">MH/s</option></select></div>
<div><label for="calc-fee">${text('calcMinerFee')}</label><input id="calc-fee" data-calc="minerFee" type="text" inputmode="decimal" autocomplete="off" value="1" dir="ltr" aria-describedby="calc-fee-help calc-hint"></div></div>
<p id="calc-fee-help" class="calc-note">${text('calcFeeHelp')}</p>
</fieldset>
<p id="calc-hint" data-calc="hint" role="status">${text('calcEmpty')}</p>
<div data-calc="result" aria-live="polite" hidden><table class="calc-results"><thead><tr><th scope="col">BNT</th><th scope="col">${text('calcGross')}</th><th scope="col">${text('calcNet')}</th></tr></thead><tbody>
<tr><th scope="row">${text('calcDay')}</th><td><bdi data-calc="day">—</bdi></td><td><bdi data-calc="netDay">—</bdi></td></tr>
<tr><th scope="row">${text('calcWeek')}</th><td><bdi data-calc="week">—</bdi></td><td><bdi data-calc="netWeek">—</bdi></td></tr>
</tbody></table></div>
<dl class="calc-inputs">${[['calcNetwork','network'],['calcHeight','height'],['calcReward','reward'],['calcPoolFee','poolFee']].map(([key, field]) => `<div><dt>${text(key)}</dt><dd><bdi data-calc="${field}">—</bdi></dd></div>`).join('')}</dl>
<div class="calc-refresh"><p data-calc="status" role="status"></p><button type="button" data-calc="refresh" class="btn" disabled>${text('calcRefresh')}</button></div>
<p class="calc-note">${text('calcAssumptions')}</p><p class="calc-note">${text('calcSource')}</p>
<script type="application/json" data-calculator-messages>${messages}</script>
</section>`;
}
