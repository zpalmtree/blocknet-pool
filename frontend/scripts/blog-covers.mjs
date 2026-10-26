// Language-neutral editorial illustrations and real product captures, shared by all locales.
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync } from 'node:fs';
import sharp from 'sharp';
import { fileURLToPath } from 'node:url';

const imageDir = new URL('../blog/images/', import.meta.url);
const line = (d, color = '#70c793', width = 3, extra = '') => `<path d="${d}" fill="none" stroke="${color}" stroke-width="${width}" stroke-linecap="round" stroke-linejoin="round" ${extra}/>`;
const circle = (x, y, r, fill, extra = '') => `<circle cx="${x}" cy="${y}" r="${r}" fill="${fill}" ${extra}/>`;
const rect = (x, y, w, h, fill, rx = 16, extra = '') => `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${rx}" fill="${fill}" ${extra}/>`;
const text = (x, y, value, size = 32, fill = '#123c2a', extra = '') => `<text x="${x}" y="${y}" fill="${fill}" font-family="DejaVu Sans, sans-serif" font-size="${size}" ${extra}>${value}</text>`;
const cube = (x, y, size = 100) => `<g transform="translate(${x} ${y}) scale(${size / 100})"><path d="M0 -56 64 -20 0 16 -64 -20Z" fill="#a2e8ad"/><path d="M-64 -20 0 16V90L-64 54Z" fill="#48a66c"/><path d="M0 16 64 -20V54L0 90Z" fill="#237a4e"/>${line('M0 -56V16M-64 -20 0 16 64 -20', '#d4f6c7', 2)}</g>`;
const miner = (x, y, scale = 1) => `<g transform="translate(${x} ${y}) scale(${scale})">${rect(-60, -42, 120, 80, '#f0f5df', 12)}${rect(-49, -31, 98, 54, '#103c2a', 5)}${line('M-32 9V-5M-16 9V-18M0 9V-9M16 9V-22M32 9V-13', '#9de5ad', 6)}${line('M0 38V55M-25 56H25', '#8dc4a0', 7)}</g>`;
const check = (x, y, scale = 1) => `<g transform="translate(${x} ${y}) scale(${scale})">${circle(0, 0, 29, '#b8ee9d')}${line('M-12 0 -3 9 14 -10', '#16432e', 5)}</g>`;
function frame(body, dark = false) {
  const bg = dark ? '#0b2920' : '#edf2e5';
  return `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="675" viewBox="0 0 1200 675"><defs><pattern id="grid" width="48" height="48" patternUnits="userSpaceOnUse"><path d="M48 0H0V48" fill="none" stroke="${dark ? '#315142' : '#d5dfca'}" stroke-width="1" opacity=".45"/></pattern><linearGradient id="area" x2="0" y2="1"><stop stop-color="#58ac72" stop-opacity=".3"/><stop offset="1" stop-color="#58ac72" stop-opacity="0"/></linearGradient></defs>${rect(0, 0, 1200, 675, bg, 0)}${rect(0, 0, 1200, 675, 'url(#grid)', 0)}${body}</svg>`;
}

function rewardSchedule() {
  const points = Array.from({ length: 49 }, (_, month) => [150 + month * 18.75, 515 - (month === 48 ? 2 : 721.25093035 * Math.exp(-.75 * month / 12) + 2) / 723.25093035 * 350]);
  let path = `M${points[0].join(' ')}`;
  for (let i = 1; i < points.length; i++) path += `H${points[i][0]}V${points[i][1]}`;
  return frame(`${rect(86, 72, 1028, 530, '#fbfcf5', 28)}${[0, 1, 2, 3].map(i => line(`M150 ${165 + i * 116.7}H1050`, '#dce5d4', 2)).join('')}<path d="${path}V515H150Z" fill="url(#area)"/>${line(path, '#24824a', 7)}${text(150, 126, 'BNT', 28)}${[0, 12, 24, 36, 48].map(n => text(150 + n * 18.75, 558, n, 24, '#6b8068', 'text-anchor="middle"')).join('')}${text(176, 205, '723.25', 44)}${circle(1050, 514, 9, '#24824a')}${text(985, 480, '2.0', 32)}`);
}
function miningPool() {
  const positions = [[210, 190], [220, 450], [480, 110], [480, 565], [985, 190], [985, 465]];
  return frame(`${positions.map(([x, y]) => line(`M${x} ${y}Q600 ${y} 600 320`, '#4a795e', 3)).join('')}${circle(600, 325, 150, '#153e2d', 'stroke="#315e43" stroke-width="2"')}${positions.map(([x, y]) => miner(x, y, .85)).join('')}${cube(600, 300, 135)}${check(680, 420, .8)}`, true);
}
function checkpoints() {
  return frame(`${line('M135 450H1065', '#8fae8b', 8)}${Array.from({ length: 8 }, (_, i) => cube(155 + i * 128, 420, .65 * 100)).join('')}${line('M155 330Q600 -20 1051 330', '#258652', 6, 'stroke-dasharray="14 14"')}${[155, 539, 1051].map(x => check(x, 380, .8)).join('')}${rect(485, 90, 230, 205, '#153e2d', 26)}${line('M600 125V235M559 195 600 236 641 195M548 260H652', '#b8ee9d', 12)}`);
}
function fairLaunch() {
  return frame(`${line('M170 335H1035', '#4a795e', 4)}${circle(180, 335, 70, '#173e2d', 'stroke="#72a782" stroke-width="3"')}${text(180, 358, '0', 66, '#c7efad', 'text-anchor="middle"')}${cube(535, 300, 155)}${cube(825, 320, 85)}${cube(1045, 325, 58)}${[-1, 1].map(sign => line(`M535 ${sign < 0 ? 190 : 478}V${sign < 0 ? 100 : 565}`, '#41664c', 3)).join('')}${[[375, 155], [695, 155], [375, 520], [695, 520]].map(([x, y]) => `${line(`M${x} ${y} 535 335`, '#41664c', 3)}${circle(x, y, 13, '#c4eb9c')}`).join('')}`, true);
}
function pplns() {
  return frame(`${rect(78, 110, 730, 430, '#f9fcf3', 24)}${Array.from({ length: 6 }, (_, i) => `${rect(100 + i * 115, 140, 100, 360, i % 2 ? '#e2ecd6' : '#edf3e4', 12)}${Array.from({ length: 4 }, (_, j) => circle(150 + i * 115, 192 + j * 85, 13 + ((i + j) % 3) * 4, ['#388b59', '#90bd68', '#c4dca6'][(i + j) % 3])).join('')}`).join('')}${line('M110 575H795M110 564V586M795 564V586', '#629569', 3)}${text(450, 625, '6h', 28, '#406448', 'text-anchor="middle"')}${line('M820 325H920', '#478b5d', 5)}${cube(1010, 285, 100)}${check(1010, 470)}`);
}
function poolSolo() {
  return frame(`${line('M600 90V585', '#42624c', 3, 'stroke-dasharray="9 13"')}${[145, 300, 455].map(x => `${miner(x, 170, .75)}${line(`M${x} 230 300 410`, '#619979', 3)}`).join('')}${miner(900, 170, 1.1)}${line('M900 240V400', '#619979', 3)}${cube(300, 435, 110)}${cube(900, 435, 110)}`, true);
}
function hashrate() {
  return frame(`${rect(110, 90, 980, 495, '#f9fcf3', 30)}${[0, 1, 2, 3].map(i => line(`M150 ${210 + i * 88}H1050`, '#dbe5d4', 2)).join('')}${text(165, 167, 'H/s', 43)}${line('M155 445 225 445 255 405 287 450 325 285 355 430 390 345 430 380 470 210 505 350 550 300 585 325 625 170 665 300 705 250 750 275 795 195 840 250 885 205 935 230 985 160 1040 190', '#25864d', 7)}${circle(1040, 190, 10, '#25864d')}${Array.from({ length: 16 }, (_, i) => rect(163 + i * 55, 523, 35, 15, i < 12 ? '#9bc583' : '#dfe8d6', 4)).join('')}`);
}
function estimate() {
  return frame(`${rect(115, 100, 590, 475, '#f9fcf3', 28)}${text(220, 260, 'H/s', 68)}${line('M195 295H405', '#2d6d43', 4)}${text(220, 385, 'H/s', 68)}${text(465, 332, '×', 80)}${text(500, 425, '288', 32, '#789475')}${line('M735 335H835M805 310 835 335 805 360', '#82ba87', 6)}${circle(965, 335, 110, '#b7e29a')}${circle(965, 335, 91, 'none', 'stroke="#639b59" stroke-width="3"')}${text(965, 354, 'BNT', 51, '#204b30', 'text-anchor="middle"')}${line('M180 485H650', '#d4dfca', 3)}${text(185, 535, '≈', 40, '#508459')}${[0, 1, 2, 3, 4, 5, 6].map(i => rect(300 + i * 45, 527 - i * 7, 24, 15 + i * 7, '#a5c88b', 4)).join('')}`, true);
}

function poolLuck() {
  return frame(`${rect(90, 85, 1020, 505, '#fbfcf5', 28)}${line('M560 145V535', '#789475', 3, 'stroke-dasharray="10 10"')}${[50, 100, 200].map((n, i) => `${text(155, 225 + i * 135, `${n}%`, 38)}${rect(300, 175 + i * 135, n * 2.6, 70, ['#a2d69b', '#58ac72', '#237a4e'][i], 10)}${cube(340 + n * 2.6, 205 + i * 135, 33)}`).join('')}`);
}

function shareOutcomes() {
  return frame(`${miner(185, 337, 1.25)}${line('M275 337H395', '#629569', 5)}${rect(405, 200, 220, 275, '#f9fcf3', 24)}${[260, 337, 414].map(y => `${circle(515, y, 20, '#6aa77a')}`).join('')}${line('M635 337H745M745 170V505M745 170H910M745 337H910M745 505H910', '#629569', 4)}${check(975, 170, 1.35)}${circle(975, 337, 39, '#dfe8d6')}${line('M957 319 993 355M993 319 957 355', '#52725b', 5)}${circle(975, 505, 39, '#e8e0b9')}${circle(975, 505, 24, 'none', 'stroke="#786d48" stroke-width="3"')}${line('M975 488V505L987 513', '#786d48', 4)}`);
}

function difficultyTargets() {
  return frame(`${rect(100, 150, 460, 375, '#f9fcf3', 24)}${rect(640, 150, 460, 375, '#f9fcf3', 24)}${Array.from({length: 6}, (_, row) => Array.from({length: 8}, (_, col) => circle(155 + col * 50, 215 + row * 48, 9, '#bbcfb0')).join('')).join('')}${Array.from({length: 6}, (_, row) => Array.from({length: 8}, (_, col) => circle(695 + col * 50, 215 + row * 48, 9, '#bbcfb0')).join('')).join('')}${circle(330, 337, 110, '#b8e8a4', 'fill-opacity=".6" stroke="#237a4e" stroke-width="4"')}${circle(870, 337, 48, '#b8e8a4', 'fill-opacity=".6" stroke="#237a4e" stroke-width="4"')}${line('M575 337H625M614 326 625 337 614 348', '#629569', 4)}`);
}

function stratumConversation() {
  const arrow = (x1, x2, y) => line(`M${x1} ${y}H${x2}M${x2 + (x2 > x1 ? -14 : 14)} ${y-12}L${x2} ${y}L${x2 + (x2 > x1 ? -14 : 14)} ${y+12}`, '#629569', 5);
  return frame(`${miner(195, 120, 1.1)}${rect(520, 60, 160, 120, '#123c2a', 18)}${[85,120,155].map(y=>line(`M545 ${y}H655`, '#b8ee9d', 5)).join('')}${cube(1000, 105, 78)}${[195,600,1000].map(x=>line(`M${x} 205V590`, '#a7bda0', 3, 'stroke-dasharray="8 10"')).join('')}${arrow(210,585,250)}${arrow(985,615,310)}${arrow(585,210,370)}${arrow(210,585,430)}${arrow(600,985,490)}${arrow(585,210,550)}${[250,370,430,550].map((y,i)=>text(395,y-15,['login','job','submit','ok'][i],24,'#123c2a','text-anchor="middle"')).join('')}`);
}

function confirmationTimeline() {
  return frame(`${rect(75, 95, 1050, 470, '#f9fcf3', 26)}${line('M175 280H1010M990 263 1010 280 990 297', '#629569', 5)}${[175,370,565,760].map((x,i)=>`${cube(x,250,65)}${text(x,380,['0','1','…','60'][i],35,'#123c2a','text-anchor="middle"')}`).join('')}${line('M850 160V430','#82a67b',3,'stroke-dasharray="8 10"')}${check(975,250,1.5)}${rect(915,345,120,80,'#dbe8d2',14)}${line('M935 370h80m-80 25h50','#629569',4)}${line('M175 485H760M175 475V495M760 475V495','#a7bda0',3)}`);
}

function miningFeeStages() {
  return frame(`${rect(70, 100, 1060, 475, '#f9fcf3', 24)}${miner(215,250,1.1)}${cube(600,230,85)}${check(1000,255,1.5)}${line('M310 260H475M455 243 475 260 455 277M720 260H890M870 243 890 260 870 277','#629569',5)}${text(215,400,'(1 − m)',42,'#123c2a','text-anchor="middle"')}${text(600,400,'(1 − p)',42,'#123c2a','text-anchor="middle"')}${text(1000,400,'BNT',42,'#123c2a','text-anchor="middle"')}${line('M215 475H1000M215 463V487M1000 463V487','#a7bda0',3)}`);
}

function multipleComputers() {
  return frame(`${[180,337,495].map((y,i)=>`${miner(210,y,.8)}${text(115,y+12,String(i+1),28)}${line(`M275 ${y}H390L520 337`,'#629569',4)}`).join('')}${cube(600,300,105)}${line('M710 337H865M845 320 865 337 845 354','#629569',5)}${rect(900,245,220,175,'#f9fcf3',22)}${rect(1020,295,100,75,'#c8dfb5',12)}${circle(1050,332,10,'#237a4e')}${check(1005,485,.8)}`);
}

function changeAddress() {
  const wallet = (x,y,shade) => `${rect(x,y,220,140,shade,20)}${rect(x+125,y+45,95,60,'#d9e9cc',10)}${circle(x+155,y+75,9,'#237a4e')}`;
  return frame(`${miner(190,337,1.3)}${line('M285 337H450M450 337V190H695','#a7bda0',5,'stroke-dasharray="10 10"')}${line('M450 337V490H695M675 470 695 490 675 510','#237a4e',7)}${wallet(740,120,'#d5dfca')}${wallet(740,420,'#b8ee9d')}${text(850,315,'1',36,'#6b8068','text-anchor="middle"')}${text(850,610,'2',36,'#123c2a','text-anchor="middle"')}${rect(1010,145,95,95,'#fbfcf5',12)}${[170,193,216].map(y=>line(`M1030 ${y}H1085`,'#789475',4)).join('')}${check(1055,490,1.15)}`);
}

function payoutWalletTrace() {
 return frame(`${rect(70,200,245,285,'#fbfcf5',22)}${[250,295,340,385].map(y=>line(`M105 ${y}H275`,'#87ac8d',5)).join('')}${check(193,440,.65)}${line('M340 337H460M440 317 460 337 440 357','#237a4e',6)}${cube(590,295,110)}${line('M720 337H840M820 317 840 337 820 357','#237a4e',6)}${rect(880,250,240,175,'#d5e8c8',22)}${rect(1010,300,110,75,'#fbfcf5',12)}${circle(1040,337,10,'#237a4e')}${line('M920 485c90 65 190 5 175-45m-22 12 22-12 14 24','#629569',5)}${check(1000,165,1.2)}`);
}

const illustrations = {
  'pool-payout-missing-from-wallet': payoutWalletTrace,
  'change-mining-wallet-address': changeAddress,
  'mine-multiple-computers-one-wallet': multipleComputers,
  'mining-pool-fees-vs-miner-fees': miningFeeStages,
  'block-confirmations-mining-rewards': confirmationTimeline,
  'stratum-mining-protocol': stratumConversation,
  'mining-difficulty-explained': difficultyTargets,
  'accepted-rejected-stale-shares': shareOutcomes,
  'mining-pool-luck-explained': poolLuck,
  'blocknet-block-reward-schedule': rewardSchedule,
  'blocknet-checkpoints-fast-sync': checkpoints,
  'blocknet-fair-launch': fairLaunch,
  'estimate-blocknet-mining-rewards': estimate,
  'how-pplns-payouts-work': pplns,
  'pool-vs-solo-mining-blocknet': poolSolo,
  'what-is-a-mining-pool': miningPool,
  'what-is-hashrate': hashrate,
};
const screenshots = {
  'mine-blocknet-on-bntpool': 'seine-dashboard.webp',
  'track-your-miner-on-bntpool': 'pool-dashboard.png',
};

export async function buildCovers(slugs, output) {
  mkdirSync(output, { recursive: true });
  const covers = new Map();
  for (const slug of slugs) {
    const screenshot = screenshots[slug];
    if (!screenshot && !illustrations[slug]) throw new Error(`Add an editorial cover for ${slug} in blog-covers.mjs`);
    const source = screenshot ? readFileSync(new URL(screenshot, imageDir)) : Buffer.from(illustrations[slug]());
    const rendered = await sharp(source).resize(1200, 675, { fit: screenshot ? 'contain' : 'fill', background: screenshot === 'seine-dashboard.webp' ? '#0b1217' : '#edf2e5' }).png().toBuffer();
    const version = createHash('sha256').update(rendered).digest('hex').slice(0, 12);
    const filename = `${slug}-${version}`;
    await Promise.all([
      sharp(rendered).webp({ quality: 88 }).toFile(fileURLToPath(new URL(`${filename}.webp`, output))),
      sharp(rendered).resize(600, 338).webp({ quality: 82 }).toFile(fileURLToPath(new URL(`${filename}-small.webp`, output))),
      sharp(rendered).jpeg({ quality: 88 }).toFile(fileURLToPath(new URL(`${filename}.jpg`, output))),
    ]);
    covers.set(slug, { src: `/blog/covers/${filename}.webp`, small: `/blog/covers/${filename}-small.webp`, social: `/blog/covers/${filename}.jpg` });
  }
  if (slugs.includes('mining-pool-luck-explained')) {
    await sharp(readFileSync(new URL('pool-luck-history.png', imageDir)))
      .resize(1200).jpeg({ quality: 88 })
      .toFile(fileURLToPath(new URL('pool-luck-history-20261026.jpg', output)));
  }
  return covers;
}
