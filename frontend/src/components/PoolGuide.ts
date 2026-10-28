// Shared by the dashboard and the initial HTML, including every supported locale.
import { createElement as h } from 'react';

export function PoolGuide({ t, localizePath }: {
  t: (key: string) => string;
  localizePath: (path: string) => string;
}) {
  const blog = localizePath('/blog/');
  return h('details', { className: 'pool-guide card section', id: 'pool-guide' },
    h('summary', null, t('m466')),
    h('div', { className: 'pool-guide-body' },
      h('h2', null, t('m450')),
      h('p', null, t('m451')),
      h('a', { href: localizePath('/start') }, t('m002')),
      h('h2', null, t('m476')),
      h('p', null, t('m479')),
      h('ul', { className: 'help-links' },
        h('li', null, h('a', { href: `${blog}how-pplns-payouts-work/` }, `PPLNS · ${t('m004')}`)),
        h('li', null, h('a', { href: `${localizePath('/start')}#pool-information` }, `${t('m472')} · ${t('m474')}`)),
        h('li', null, h('a', { href: `${blog}estimate-blocknet-mining-rewards/#mining-calculator` }, t('rewardsCalculator'))),
        h('li', null, h('a', { href: `${blog}track-your-miner-on-bntpool/` }, t('trackWorker'))),
      ),
    ),
  );
}
