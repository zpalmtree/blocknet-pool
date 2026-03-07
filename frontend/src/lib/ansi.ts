import type { CSSProperties } from 'react';

export interface ParsedAnsiSegment {
  text: string;
  style?: CSSProperties;
}

interface AnsiStyleState {
  fg: string | null;
  bg: string | null;
  bold: boolean;
  dim: boolean;
  italic: boolean;
  underline: boolean;
  strike: boolean;
  inverse: boolean;
}

const ESC = '\u001b';
const DEFAULT_ANSI_STATE: AnsiStyleState = {
  fg: null,
  bg: null,
  bold: false,
  dim: false,
  italic: false,
  underline: false,
  strike: false,
  inverse: false,
};

const ANSI_STANDARD_COLORS = [
  'var(--log-ansi-black)',
  'var(--log-ansi-red)',
  'var(--log-ansi-green)',
  'var(--log-ansi-yellow)',
  'var(--log-ansi-blue)',
  'var(--log-ansi-magenta)',
  'var(--log-ansi-cyan)',
  'var(--log-ansi-white)',
];

const ANSI_BRIGHT_COLORS = [
  'var(--log-ansi-bright-black)',
  'var(--log-ansi-bright-red)',
  'var(--log-ansi-bright-green)',
  'var(--log-ansi-bright-yellow)',
  'var(--log-ansi-bright-blue)',
  'var(--log-ansi-bright-magenta)',
  'var(--log-ansi-bright-cyan)',
  'var(--log-ansi-bright-white)',
];

const ANSI_256_LEVELS = [0, 95, 135, 175, 215, 255];
const ansi256ColorCache = new Map<number, string>();

export function parseAnsiLine(line: string): ParsedAnsiSegment[] {
  const segments: ParsedAnsiSegment[] = [];
  let state = { ...DEFAULT_ANSI_STATE };
  let buffer = '';
  let index = 0;

  const flush = () => {
    if (!buffer) return;
    const text = sanitizeLogText(buffer);
    buffer = '';
    if (!text) return;
    segments.push({ text, style: styleFromState(state) });
  };

  while (index < line.length) {
    if (line[index] !== ESC) {
      buffer += line[index];
      index += 1;
      continue;
    }

    flush();

    if (index + 1 >= line.length) {
      index += 1;
      continue;
    }

    const next = line[index + 1];
    if (next === '[') {
      const sequenceEnd = findCsiEnd(line, index + 2);
      if (sequenceEnd < 0) {
        index += 2;
        continue;
      }

      if (line[sequenceEnd] === 'm') {
        state = applySgrSequence(state, line.slice(index + 2, sequenceEnd));
      }
      index = sequenceEnd + 1;
      continue;
    }

    if (next === ']' || next === 'P' || next === 'X' || next === '^' || next === '_') {
      const sequenceEnd = findStringSequenceEnd(line, index + 2);
      index = sequenceEnd >= 0 ? sequenceEnd : line.length;
      continue;
    }

    index += 2;
  }

  flush();

  return segments;
}

function findCsiEnd(input: string, start: number): number {
  for (let index = start; index < input.length; index += 1) {
    const code = input.charCodeAt(index);
    if (code >= 0x40 && code <= 0x7e) {
      return index;
    }
  }
  return -1;
}

function findStringSequenceEnd(input: string, start: number): number {
  for (let index = start; index < input.length; index += 1) {
    if (input[index] === '\u0007') {
      return index + 1;
    }
    if (input[index] === ESC && input[index + 1] === '\\') {
      return index + 2;
    }
  }
  return -1;
}

function applySgrSequence(current: AnsiStyleState, rawParams: string): AnsiStyleState {
  const next = { ...current };
  const params = parseSgrParameters(rawParams);

  for (let index = 0; index < params.length; index += 1) {
    const code = params[index];

    if (code === 0) {
      Object.assign(next, DEFAULT_ANSI_STATE);
      continue;
    }

    if (code === 38 || code === 48) {
      const extended = parseExtendedColor(params, index + 1);
      if (extended.consumed > 0) {
        if (extended.color !== undefined) {
          if (code === 38) {
            next.fg = extended.color;
          } else {
            next.bg = extended.color;
          }
        }
        index += extended.consumed;
      }
      continue;
    }

    switch (code) {
      case 1:
        next.bold = true;
        break;
      case 2:
        next.dim = true;
        break;
      case 3:
        next.italic = true;
        break;
      case 4:
        next.underline = true;
        break;
      case 7:
        next.inverse = true;
        break;
      case 9:
        next.strike = true;
        break;
      case 21:
      case 22:
        next.bold = false;
        next.dim = false;
        break;
      case 23:
        next.italic = false;
        break;
      case 24:
        next.underline = false;
        break;
      case 27:
        next.inverse = false;
        break;
      case 29:
        next.strike = false;
        break;
      case 39:
        next.fg = null;
        break;
      case 49:
        next.bg = null;
        break;
      default:
        if (code >= 30 && code <= 37) {
          next.fg = ANSI_STANDARD_COLORS[code - 30];
        } else if (code >= 40 && code <= 47) {
          next.bg = ANSI_STANDARD_COLORS[code - 40];
        } else if (code >= 90 && code <= 97) {
          next.fg = ANSI_BRIGHT_COLORS[code - 90];
        } else if (code >= 100 && code <= 107) {
          next.bg = ANSI_BRIGHT_COLORS[code - 100];
        }
        break;
    }
  }

  return next;
}

function parseSgrParameters(rawParams: string): number[] {
  if (!rawParams) {
    return [0];
  }

  return rawParams
    .split(/[;:]/)
    .map((part) => {
      if (part === '') return 0;
      const value = Number.parseInt(part, 10);
      return Number.isFinite(value) ? value : null;
    })
    .filter((value): value is number => value != null);
}

function parseExtendedColor(
  params: number[],
  start: number
): {
  consumed: number;
  color?: string | null;
} {
  const mode = params[start];
  if (mode === 5) {
    const colorIndex = params[start + 1];
    if (!Number.isInteger(colorIndex)) {
      return { consumed: 0 };
    }
    return {
      consumed: 2,
      color: ansi256Color(colorIndex),
    };
  }

  if (mode === 2) {
    const red = params[start + 1];
    const green = params[start + 2];
    const blue = params[start + 3];
    if (![red, green, blue].every(Number.isInteger)) {
      return { consumed: 0 };
    }
    return {
      consumed: 4,
      color: rgbColor(red, green, blue),
    };
  }

  return { consumed: 0 };
}

function ansi256Color(index: number): string | undefined {
  if (!Number.isInteger(index) || index < 0 || index > 255) {
    return undefined;
  }

  const cached = ansi256ColorCache.get(index);
  if (cached) {
    return cached;
  }

  let color: string;
  if (index < 8) {
    color = ANSI_STANDARD_COLORS[index];
  } else if (index < 16) {
    color = ANSI_BRIGHT_COLORS[index - 8];
  } else if (index < 232) {
    const paletteIndex = index - 16;
    const red = ANSI_256_LEVELS[Math.floor(paletteIndex / 36)];
    const green = ANSI_256_LEVELS[Math.floor((paletteIndex % 36) / 6)];
    const blue = ANSI_256_LEVELS[paletteIndex % 6];
    color = rgbColor(red, green, blue);
  } else {
    const value = 8 + (index - 232) * 10;
    color = rgbColor(value, value, value);
  }

  ansi256ColorCache.set(index, color);
  return color;
}

function rgbColor(red: number, green: number, blue: number): string {
  return `rgb(${clampColor(red)}, ${clampColor(green)}, ${clampColor(blue)})`;
}

function clampColor(value: number): number {
  return Math.max(0, Math.min(255, value));
}

function styleFromState(state: AnsiStyleState): CSSProperties | undefined {
  const style: CSSProperties = {};
  const baseFg = state.fg;
  const baseBg = state.bg;

  if (state.inverse) {
    style.color = baseBg ?? 'var(--log-bg)';
    style.backgroundColor = baseFg ?? 'var(--log-text)';
  } else {
    if (baseFg) style.color = baseFg;
    if (baseBg) style.backgroundColor = baseBg;
  }

  if (state.bold) {
    style.fontWeight = 700;
  }
  if (state.dim) {
    style.opacity = 0.72;
  }
  if (state.italic) {
    style.fontStyle = 'italic';
  }
  if (state.underline || state.strike) {
    style.textDecorationLine = [state.underline ? 'underline' : '', state.strike ? 'line-through' : '']
      .filter(Boolean)
      .join(' ');
  }

  return Object.keys(style).length ? style : undefined;
}

function sanitizeLogText(text: string): string {
  return text.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '');
}
