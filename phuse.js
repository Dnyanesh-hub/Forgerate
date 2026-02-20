'use strict';

/**
 * convertPhuse.js
 * Converts phuse.xlsx → phuse_output.json
 *
 * Usage:
 *   npm install xlsx
 *   node convertPhuse.js phuse.xlsx
 *   node convertPhuse.js phuse.xlsx output.json   ← custom output path
 */

const XLSX = require('xlsx');
const fs   = require('fs');
const path = require('path');

// ─── 1. CONSTANTS ─────────────────────────────────────────────────────────────

const ROMAN_NUMERALS = new Set([
  'i','ii','iii','iv','v','vi','vii','viii','ix','x',
  'xi','xii','xiii','xiv','xv','xvi','xvii','xviii','xix','xx',
  'xxi','xxii','xxiii','xxiv','xxv',
]);

const LABOUR_ROLES = [
  'plumber 1st class', 'plumber 2nd class',
  'plumber i class',   'plumber ii class',
  'man mazdoor',       'mazdoor',
  'mason 1st class',   'mason 2nd class',
  'mason i class',     'mason ii class',
  'fitter i class',    'fitter ii class',
  'fitters i class',   'fitters ii class',
  'carpenter 1st class','carpenter 2nd class',
];

const UNIT_MAP = {
  rm: 'rm', rmt: 'rm', 'rmt.': 'rm', metre: 'rm', meter: 'rm',
  mtrs: 'rm', mtr: 'rm', m: 'rm',
  kg: 'kg', kgs: 'kg',
  quintal: 'q', q: 'q',
  cum: 'cum', 'cu.m': 'cum', cft: 'cft',
  sqm: 'sqm', 'sq.m': 'sqm',
  day: 'day', days: 'day',
  nos: 'nos', no: 'nos', 'no.': 'nos', each: 'nos',
  litre: 'litre', ltr: 'litre', 'lts.': 'litre', lts: 'litre', lt: 'litre',
  joint: 'joint', joints: 'joint',
  ls: 'ls', 'l.s': 'ls',
  mt: 'mt', hour: 'hour', hr: 'hour',
};

// ─── 2. HELPERS ───────────────────────────────────────────────────────────────

const s    = v => (v == null ? '' : String(v).trim());
const isNum = v => {
  if (v == null) return false;
  if (typeof v === 'number') return !isNaN(v);
  return /^-?\d+(\.\d+)?$/.test(String(v).trim());
};
const toFloat = v => {
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
};
const normUnit = raw => {
  const k = s(raw).toLowerCase().replace(/\.$/, '');
  return k ? (UNIT_MAP[k] ?? k) : null;
};

// ─── 3. ROW CLASSIFIER ────────────────────────────────────────────────────────
// Reads 8-column row → returns string type

const RE_PHE      = /^PHE-[A-Z]+-\d+[A-Z]?$/;
const RE_ALPHA_UP = /^[A-Z]$/;
const RE_RATE     = /rate\s+per/i;
const RE_WEIGHT   = /weight\s*(of|=)/i;
const RE_COMP     = /^\s*\([a-z]\)/i;
const RE_NOTE     = /^note\s*:/i;
const RE_SUNDRIES = /sundries/i;

function classifyRow(row, rowIndex) {
  const [c0, c1, c2, c3, c4] = row;
  const s0 = s(c0), s1 = s(c1), s2 = s(c2);

  if (rowIndex <= 4)                                         return 'FILE_HEADER';
  if (!s0 && !s1 && !s2 && !s(c3) && !s(c4))               return 'EMPTY';
  if (s0.toLowerCase().includes('common data'))              return 'COMMON_DATA_HEADER';
  if (RE_PHE.test(s0))                                       return 'PRIMARY_ITEM';
  if (s1 && ROMAN_NUMERALS.has(s1.toLowerCase()))            return 'SUB_ITEM_ROMAN';
  if (s1 && RE_ALPHA_UP.test(s1))                            return 'SUB_ITEM_ALPHA';
  if (s2 && RE_RATE.test(s2))                                return 'RATE_FORMULA';
  if (s2 && RE_WEIGHT.test(s2))                              return 'WEIGHT_CALC';
  if (s2 && RE_SUNDRIES.test(s2) && isNum(c4))               return 'SUNDRY';
  if (s2 && s(c3) && isNum(c4))                              return 'INPUT_ROW';
  if (s2 && s(c3) && !isNum(c4))                             return 'INPUT_NO_QTY';
  if (s2 && RE_COMP.test(s2))                                return 'COMPUTATION';
  if (s2 && RE_NOTE.test(s2))                                return 'NOTE';
  if (s2)                                                    return 'TEXT';
  return 'OTHER';
}

// ─── 4. FIELD EXTRACTORS ──────────────────────────────────────────────────────

const RE_DIA    = /(\d+(?:\.\d+)?)\s*mm\s+dia/i;
const RE_WT     = /=\s*(\d+(?:\.\d+)?)\s*kgs?\b/i;
const RE_SPAN   = /(\d+)\s*m\s+length/i;
const RE_RATE_F = /rate\s+per\s+(\w+(?:\s+\w+)?)\s*(?:=\s*([a-z]\/(\d+)))?/i;

function extractDiameter(text) {
  const m = text.match(RE_DIA);
  if (!m) return null;
  const v = parseFloat(m[1]);
  return (v >= 15 && v <= 2000) ? v : null;
}

function extractWeight(text) {
  const mw = text.match(RE_WT);
  if (!mw) return null;
  const ms = text.match(RE_SPAN);
  return {
    value:  parseFloat(mw[1]),
    unit:   'kg',
    span_m: ms ? parseInt(ms[1], 10) : null,
  };
}

function extractRateFormula(text) {
  const m = text.match(RE_RATE_F);
  if (!m) return null;
  return {
    per:     m[1].trim().toLowerCase(),
    formula: m[2] || null,
    divisor: m[3] ? parseInt(m[3], 10) : null,
  };
}

function getLabourRole(desc) {
  const lower = desc.toLowerCase().trim();
  return LABOUR_ROLES.find(r => lower.startsWith(r)) ?? null;
}

// ─── 5. CHUNK FACTORIES ───────────────────────────────────────────────────────

function makePrimary(row, rowIndex) {
  return {
    index_code:      s(row[0]),
    serial_no:       row[1],
    description:     s(row[2]).replace(/\s{2,}/g, ' '),
    row:             rowIndex,
    notes:           [],
    sub_items:       [],
    direct_labour:   [],
    direct_material: [],
  };
}

function makeSubItem(row, rowIndex) {
  const desc = s(row[2]).trim();
  return {
    sub_index:    s(row[1]),
    description:  desc,
    row:          rowIndex,
    diameter_mm:  extractDiameter(desc),
    weight_kg:    null,
    rate_formula: null,
    output_unit:  null,
    labour:       [],
    material:     [],
    testing:      [],
    sundry:       [],
  };
}

function makeInput(row, rowIndex) {
  const desc = s(row[2]);
  const role = getLabourRole(desc);
  const inp = {
    description:     desc,
    unit:            s(row[3]) || null,
    unit_normalized: normUnit(row[3]),
    quantity:        toFloat(row[4]),
    row:             rowIndex,
  };
  if (role) inp.labour_role = role;
  return inp;
}

// ─── 6. STATE MACHINE ─────────────────────────────────────────────────────────

function convertToJSON(filePath) {
  // Read Excel
  const wb   = XLSX.readFile(filePath, { raw: true, cellDates: false });
  const ws   = wb.Sheets[wb.SheetNames[0]];
  const raw  = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null, raw: true });

  // Pad every row to 8 columns
  const rows = raw.map(r => { while (r.length < 8) r.push(null); return r; });

  const chunks = [];
  let primary  = null;
  let subItem  = null;

  const closeSub = () => {
    if (subItem && primary) primary.sub_items.push(subItem);
    subItem = null;
  };
  const closePrimary = () => {
    closeSub();
    if (primary) chunks.push(primary);
    primary = null;
  };

  rows.forEach((row, i) => {
    const rowIndex = i + 1;           // 1-based, matches Excel row numbers
    const type     = classifyRow(row, rowIndex);
    const desc     = s(row[2]);

    switch (type) {

      // ── New primary item ──────────────────────────────────────────────────
      case 'PRIMARY_ITEM':
        closePrimary();
        primary = makePrimary(row, rowIndex);
        break;

      case 'COMMON_DATA_HEADER':
        closePrimary();
        primary = {
          index_code: '__COMMON__', serial_no: null,
          description: 'Common Data - Shared Base Analysis',
          row: rowIndex, notes: [], sub_items: [],
          direct_labour: [], direct_material: [],
        };
        break;

      // ── New sub-item (roman i/ii/iii or capital A/B) ──────────────────────
      case 'SUB_ITEM_ROMAN':
      case 'SUB_ITEM_ALPHA':
        closeSub();
        subItem = makeSubItem(row, rowIndex);
        break;

      // ── Input rows: route to labour / material / testing ──────────────────
      case 'INPUT_ROW': {
        const target = subItem || primary;
        if (!target) break;
        const inp  = makeInput(row, rowIndex);
        const low  = desc.toLowerCase();
        const isTesting = low.includes('test') || low.includes('hydraulic') || low.includes('pressure test');
        if (isTesting) {
          (target.testing ?? target.direct_material).push(inp);
        } else if (inp.labour_role) {
          (target.labour   ?? target.direct_labour).push(inp);
        } else {
          (target.material ?? target.direct_material).push(inp);
        }
        break;
      }

      // ── Material listed with no quantity (e.g. "White lead") ─────────────
      case 'INPUT_NO_QTY': {
        const target = subItem || primary;
        if (!target) break;
        const list = target.material ?? target.direct_material;
        list.push({
          description:     desc,
          unit:            s(row[3]) || null,
          unit_normalized: normUnit(row[3]),
          quantity:        null,
          row:             rowIndex,
        });
        break;
      }

      // ── Sundries (e.g. "Add sundries @ 1%", factor 0.01) ─────────────────
      case 'SUNDRY':
        if (subItem) {
          subItem.sundry.push({
            description: desc,
            factor:      toFloat(row[4]),
            row:         rowIndex,
          });
        }
        break;

      // ── "Weight of 5m length = 85.5 kgs" ─────────────────────────────────
      case 'WEIGHT_CALC': {
        const w = extractWeight(desc);
        if (w && subItem && !subItem.weight_kg) subItem.weight_kg = w;
        break;
      }

      // ── "Rate per metre = c/5" ────────────────────────────────────────────
      case 'RATE_FORMULA': {
        const rf = extractRateFormula(desc);
        if (rf && subItem) {
          subItem.rate_formula = rf;
          subItem.output_unit  = rf.per;
        }
        break;
      }

      // ── Notes: "Note : The labour charges…" ──────────────────────────────
      case 'NOTE':
        if (primary) primary.notes.push({ text: desc, row: rowIndex });
        break;

      // ── Everything else (computation rows, text labels) ───────────────────
      // Silently skip — they are formula labels like "(a) Labour:", "(c) Total = a+b"
      default:
        break;
    }
  });

  closePrimary(); // flush the last open item

  // ── Build final output shape ───────────────────────────────────────────────
  const primaries = chunks.filter(c => c.index_code !== '__COMMON__');

  return {
    source:           path.basename(filePath),
    sheet:            wb.SheetNames[0],
    parsed_at:        new Date().toISOString(),
    total_items:      primaries.length,
    total_sub_items:  primaries.reduce((n, c) => n + c.sub_items.length, 0),
    items:            chunks,
  };
}

// ─── 7. CLI RUNNER ────────────────────────────────────────────────────────────

const inputFile  = process.argv[2];
const outputFile = process.argv[3] ?? inputFile?.replace(/\.xlsx$/i, '_output.json');

if (!inputFile) {
  console.error('Usage: node convertPhuse.js <phuse.xlsx> [output.json]');
  process.exit(1);
}

console.log(`Reading: ${inputFile}`);
const result = convertToJSON(inputFile);
fs.writeFileSync(outputFile, JSON.stringify(result, null, 2), 'utf8');

console.log(`Done.`);
console.log(`  Primary items : ${result.total_items}`);
console.log(`  Sub-items     : ${result.total_sub_items}`);
console.log(`  Output        : ${outputFile}`);