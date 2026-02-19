/**
 * Public Health SSR Excel Parser
 * ================================
 * Parses the publichealth.xlsx into a structured JSON format
 * suitable for database storage.
 *
 * Usage:
 *   npm install xlsx
 *   node parsePublicHealth.js
 *
 * Output: publichealth_parsed.json
 */

const XLSX = require('xlsx');
const fs = require('fs');

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

function cleanStr(val) {
  if (val == null) return null;
  return String(val).trim().replace(/\s+/g, ' ') || null;
}

function parseRate(val) {
  if (val == null) return null;
  if (typeof val === 'number') return val;
  const str = String(val).trim();
  if (str === '' || str === '-') return null;
  const num = parseFloat(str);
  return isNaN(num) ? str : num; // keep as string if non-numeric (e.g. "As per Common SSR")
}

function isNoteRow(desc) {
  if (!desc) return false;
  const upper = desc.toUpperCase();
  return upper.startsWith('NOTE') || desc.trim().startsWith('//');
}

function isSectionHeader(sno, desc, unit, rate) {
  // A section header has an integer (or compound like "3. a.") in sno and no rate
  if (!sno && !desc) return false;
  if (typeof sno === 'number' && Number.isInteger(sno) && !unit && rate == null) return true;
  // Compound like "8. a.", "9. b.", "18 a.", "11a", "41 b", "3. a."
  if (sno && typeof sno === 'string' && /^\d+/.test(sno.trim()) && !unit && rate == null) return true;
  return false;
}

function isSubHeader(sno, desc, unit, rate) {
  // Sub-headers: sno is single letter (a, b, c…) with no rate, but desc describes a pipe type
  if (sno && typeof sno === 'string' && /^[a-zA-Z]$/.test(sno.trim()) && !unit && rate == null) return true;
  return false;
}

function isDiameterRow(desc, unit, rate) {
  // Rows whose desc is a number (pipe diameter in mm) with a rate
  if (!desc) return false;
  const stripped = String(desc).trim();
  return /^\d+(\s*mm)?$/.test(stripped) || /^(Upto|From|Beyond)/i.test(stripped) ||
    /mm\s*(to\s*\d+mm)?$/.test(stripped) ||
    /^\d+\s*(mm|MLD)?\s*to\s*\d+/i.test(stripped) ||
    /^(10 mm|25 mm|65 mm|80 mm|\d+\.?\d*\s*Mld)/i.test(stripped);
}

// ─────────────────────────────────────────────
// SECTION METADATA  (maps item number → category)
// ─────────────────────────────────────────────

const SECTION_CATEGORIES = {
  1:  'Labour Rates',
  2:  'Earth Work',
  3:  'Rock Cutting & Blasting',
  4:  'Loading & Unloading',
  5:  'Loading & Unloading',
  6:  'Loading & Unloading',
  7:  'Loading & Unloading',
  '8a': 'Pipe Laying',
  '8b': 'Pipe Laying',
  '9a': 'Pipe Jointing',
  '9b': 'Pipe Jointing',
  10: 'Pipe Jointing',
  '11a': 'RCC Pipe Laying',
  '11b': 'RCC Pipe Laying',
  12: 'GI / PVC / HDPE Pipe Laying',
  13: 'AC Pressure Pipe Laying',
  14: 'AC Pressure Pipe Jointing',
  15: 'Stoneware Pipe Laying',
  16: 'PVC Pipe Laying & Testing',
  17: 'Valve Labour',
  '18a': 'Air Valve Labour',
  '18b': 'Air Valve Labour',
  19: 'Fire Hydrant Labour',
  20: 'Pipe Uprooting',
  21: 'RCC Pipe Uprooting',
  22: 'GI/PVC/HDPE Pipe Removal',
  23: 'Pipe Cutting',
  24: 'Pipe Cutting',
  25: 'Drilling & Tapping',
  26: 'Road Cutting',
  27: 'Dewatering',
  28: 'Shoring & Strutting',
  29: 'Barricading',
  30: 'Underwater Excavation',
  31: 'Infiltration Gallery',
  32: 'Infiltration Gallery',
  33: 'Centering & Scaffolding',
  34: 'Lift & Delift',
  35: 'Fixtures Labour',
  36: 'Sanitary Fixtures Labour',
  37: 'Sanitary Fixtures Labour',
  38: 'Sanitary Fixtures Labour',
  39: 'Sanitary Fixtures Labour',
  40: 'Trench Refilling',
  '41a': 'Miscellaneous',
  '41b': 'Miscellaneous',
  42: 'Silt Removal',
  43: 'Pipe Conveyance',
  44: 'Pipe Conveyance',
  45: 'Pipe Conveyance',
  46: 'Pipe Conveyance',
  47: 'Pipe Conveyance',
  48: 'Well Sinking',
  49: 'Well Sinking',
  50: 'Well Excavation',
  51: 'OHSR / ELSR Rates (Kilo Litres)',
  52: 'OHSR / ELSR Rates (Litres)',
  53: 'Filtration Plant Construction',
};

// ─────────────────────────────────────────────
// MAIN PARSER
// ─────────────────────────────────────────────

function parsePublicHealthXLSX(filePath) {
  const workbook = XLSX.readFile(filePath);
  const ws = workbook.Sheets[workbook.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

  const result = {
    title: 'Public Health Items - Schedule of Standard Rates',
    year: '2005-06',
    source_file: filePath.split('/').pop(),
    parsed_at: new Date().toISOString(),
    sections: [],
  };

  let currentSection = null;
  let currentSubSection = null;
  let sectionIdCounter = 0;
  let itemIdCounter = 0;

  // Tracks multi-row description accumulation for split cells
  let pendingDesc = null;
  let pendingDescTarget = null; // 'section' | 'item'

  for (let i = 2; i < rows.length; i++) { // skip header rows
    const [, rawSno, rawDesc, rawUnit, rawRate] = rows[i];

    const sno  = rawSno  != null ? (typeof rawSno === 'string' ? rawSno.trim() : rawSno) : null;
    const desc = cleanStr(rawDesc);
    const unit = cleanStr(rawUnit);
    const rate = parseRate(rawRate);

    // ── SKIP blank rows ──
    if (!sno && !desc && !unit && rate == null) continue;

    // ── SKIP pure NOTE / continuation rows ──
    if (!sno && unit == null && rate == null && isNoteRow(desc)) continue;
    // Multi-line note continuation (indented whitespace)
    if (!sno && unit == null && rate == null && desc && desc.startsWith(' ')) continue;

    // ── SECTION HEADER (integer sno, no rate) ──
    if (isSectionHeader(sno, desc, unit, rate)) {
      sectionIdCounter++;
      const snoKey = String(sno).replace(/\.\s*/g, '').replace(/\s+/g, '').toLowerCase();

      currentSection = {
        id: sectionIdCounter,
        item_no: sno,
        item_key: snoKey,
        category: SECTION_CATEGORIES[sno] || SECTION_CATEGORIES[snoKey] || 'General',
        title: desc,
        unit: unit || null,
        rate: rate,
        sub_sections: [],
        items: [],
        notes: [],
      };
      currentSubSection = null;
      result.sections.push(currentSection);
      continue;
    }

    // ── SUB-SECTION HEADER (single letter sno, no rate) ──
    if (isSubHeader(sno, desc, unit, rate) && currentSection) {
      currentSubSection = {
        sub_id: sno.trim(),
        description: desc,
        items: [],
      };
      currentSection.sub_sections.push(currentSubSection);
      continue;
    }

    // ── SIMPLE ITEM with rate (flat items like 11a, 11b, simple rows) ──
    if (sno && /^(11[ab]|41[ab]|18[ab])$/i.test(String(sno).replace(/[\s.]/g, ''))) {
      sectionIdCounter++;
      const snoKey = String(sno).replace(/[\s.]/g, '').toLowerCase();
      currentSection = {
        id: sectionIdCounter,
        item_no: sno,
        item_key: snoKey,
        category: SECTION_CATEGORIES[snoKey] || 'General',
        title: desc,
        unit: unit,
        rate: rate,
        sub_sections: [],
        items: [],
        notes: [],
      };
      currentSubSection = null;
      result.sections.push(currentSection);
      continue;
    }

    // ── DIAMETER / RANGE ROWS → become items ──
    if (currentSection && unit && rate != null) {
      itemIdCounter++;
      const item = {
        id: itemIdCounter,
        section_item_no: currentSection.item_no,
        dimension: desc ? desc.replace(/mm$/i, '').trim() : null,
        unit: unit.trim(),
        rate: rate,
        rate_type: typeof rate === 'number' ? 'numeric' : 'formula',
      };
      if (currentSubSection) {
        currentSubSection.items.push(item);
      } else {
        currentSection.items.push(item);
      }
      continue;
    }

    // ── ROWS with rate but no unit (e.g. section-level flat rates) ──
    if (currentSection && rate != null && !unit) {
      // These are usually scaffolding sub-items (item 33) where unit is on previous row
      const lastItem = currentSection.items[currentSection.items.length - 1];
      if (lastItem && !lastItem.rate) {
        lastItem.rate = rate;
      } else {
        itemIdCounter++;
        currentSection.items.push({
          id: itemIdCounter,
          section_item_no: currentSection.item_no,
          dimension: desc,
          unit: null,
          rate: rate,
          rate_type: typeof rate === 'number' ? 'numeric' : 'formula',
        });
      }
      continue;
    }

    // ── DESCRIPTIVE / HEADER ROWS inside a section (no rate) ──
    // These become sub-section context or are absorbed as notes
    if (currentSection && desc && !sno) {
      // "DIAMETER OF PIPE ... in mm" type headers → skip (just context)
      if (/DIAMETER OF PIPE/i.test(desc) || /DIA (in|of) (mm|pipe)/i.test(desc)) continue;
      if (/G\.I\. PIPES/i.test(desc) || /PVC\/HDPE pipes/i.test(desc)) {
        // becomes a new sub-section
        currentSubSection = {
          sub_id: 'auto_' + (currentSection.sub_sections.length + 1),
          description: desc,
          items: [],
        };
        currentSection.sub_sections.push(currentSubSection);
        continue;
      }
    }
  }

  return result;
}

// ─────────────────────────────────────────────
// RUN & WRITE OUTPUT
// ─────────────────────────────────────────────

const inputFile  = process.argv[2] || 'publichealth.xlsx';
const outputFile = process.argv[3] || 'publichealth_parsed.json';

try {
  const parsed = parsePublicHealthXLSX(inputFile);
  fs.writeFileSync(outputFile, JSON.stringify(parsed, null, 2), 'utf-8');

  console.log(`  Parsing complete!`);
  console.log(`   Sections found : ${parsed.sections.length}`);
  const totalItems = parsed.sections.reduce((sum, s) => {
    const direct = s.items.length;
    const nested = s.sub_sections.reduce((a, ss) => a + ss.items.length, 0);
    return sum + direct + nested;
  }, 0);
  console.log(`   Rate items     : ${totalItems}`);
  console.log(`   Output written : ${outputFile}`);
} catch (err) {
  console.error('  Error:', err.message);
  process.exit(1);
}