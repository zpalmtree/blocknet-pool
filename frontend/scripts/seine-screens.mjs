// Trims the raw export from seine-screens-export.rs into src/data/seine-screens.json:
// blank rows around each screen and default-coloured trailing spaces are dropped.
import { readFileSync, writeFileSync } from 'node:fs';

const [input] = process.argv.slice(2);
if (!input) throw new Error('usage: node frontend/scripts/seine-screens.mjs <raw-export.json>');
const raw = JSON.parse(readFileSync(input, 'utf8'));
const text = (line) => line.map((run) => run[0]).join('');
const blank = (line) => text(line).trim() === '' && line.every((run) => run[2] === null);
const screens = {};
for (const [name, screen] of Object.entries(raw)) {
  let lines = screen.lines;
  while (lines.length && blank(lines[0])) lines = lines.slice(1);
  while (lines.length && blank(lines[lines.length - 1])) lines = lines.slice(0, -1);
  lines = lines.map((line) => {
    const runs = line.map((run) => [...run]);
    while (runs.length && runs[runs.length - 1][0].trim() === '' && runs[runs.length - 1][2] === null) runs.pop();
    if (runs.length && runs[runs.length - 1][2] === null) runs[runs.length - 1][0] = runs[runs.length - 1][0].trimEnd();
    return runs;
  });
  screens[name] = { cols: screen.cols, rows: lines.length, lines };
}
writeFileSync(new URL('../src/data/seine-screens.json', import.meta.url), JSON.stringify(screens));
