import { Fragment, type CSSProperties, type ReactNode } from 'react';

/** One styled run of terminal text: text, colour, background (or null) and bold. */
type Run = [string, string, string | null, boolean];

export interface TerminalCapture {
  cols: number;
  rows: number;
  lines: Run[][];
}

// Box-drawing characters are drawn as CSS lines filling their cell, so borders join exactly
// whatever font is loaded. Each name lists the arms that leave the cell's centre.
const BOX: Record<string, string> = {
  '─': 'h', '│': 'v', '┌': 'rd', '┐': 'ld', '└': 'ru', '┘': 'lu',
  '├': 'vr', '┤': 'vl', '┬': 'hd', '┴': 'hu', '┼': 'hv',
};

/** Splits text so ASCII stays text and every other glyph sits in a one-column cell. */
function cells(text: string): ReactNode[] {
  const out: ReactNode[] = [];
  let ascii = '';
  const chars = Array.from(text);
  for (let index = 0; index < chars.length; index += 1) {
    const char = chars[index];
    if (char.charCodeAt(0) < 0x80) { ascii += char; continue; }
    if (ascii) { out.push(ascii); ascii = ''; }
    const box = BOX[char];
    if (box === 'h') {
      let length = 1;
      while (chars[index + length] === '─') length += 1;
      out.push(<span key={index} className="term-box term-box-h" style={{ '--n': length } as CSSProperties} />);
      index += length - 1;
    } else if (box) {
      out.push(<span key={index} className={`term-box term-box-${box}`} />);
    } else {
      out.push(<span key={index} className="term-cell">{char}</span>);
    }
  }
  if (ascii) out.push(ascii);
  return out;
}

function Lines({ capture }: { capture: TerminalCapture }) {
  return (
    <>
      {capture.lines.map((line, row) => (
        <Fragment key={row}>
          {line.map(([text, color, background, bold], index) => (
            <span key={index} style={{ color, background: background ?? undefined, fontWeight: bold ? 700 : undefined }}>{cells(text)}</span>
          ))}
          {row < capture.lines.length - 1 ? '\n' : null}
        </Fragment>
      ))}
    </>
  );
}

/**
 * A seine screen drawn as text from seine's own terminal renderer, so it stays sharp at any size.
 * Phones get a narrower capture of the same screen.
 */
export function SeineScreen({ desktop, mobile, caption }: { desktop: TerminalCapture; mobile: TerminalCapture; caption?: string }) {
  return (
    <figure className="seine-screen">
      <div className="seine-window" aria-hidden="true">
        <div className="seine-window-bar"><i /><i /><i /><span>seine</span></div>
        <pre className="seine-term seine-term-desktop" style={{ '--cols': desktop.cols } as CSSProperties}><Lines capture={desktop} /></pre>
        <pre className="seine-term seine-term-mobile" style={{ '--cols': mobile.cols } as CSSProperties}><Lines capture={mobile} /></pre>
      </div>
      {caption && <figcaption>{caption}</figcaption>}
    </figure>
  );
}
