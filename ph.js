/**
 * excel-to-json.js
 * ─────────────────────────────────────────────────────────────────
 * Production-ready converter for pipe-rate schedule Excel files
 * (BIS No.3114/85 format) → structured JSON.
 *
 * Usage:
 *   node excel-to-json.js                        # uses input.xlsx in same folder
 *   node excel-to-json.js my-file.xlsx           # custom input
 *   node excel-to-json.js my-file.xlsx out.json  # custom input + output
 *
 * Install dependency:
 *   npm install xlsx
 * ─────────────────────────────────────────────────────────────────
 */

"use strict";

const XLSX = require("xlsx");
const fs   = require("fs");
const path = require("path");

// ═══════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════

const CONFIG = {
  department:  "Public Health",
  inputFile:   process.argv[2] || "input.xlsx",
  outputFile:  process.argv[3] || "output.json",
  sheetIndex:  0,          // which sheet to parse (0 = first)
};

// ═══════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════

/**
 * Normalise raw item numbers from the Excel cell.
 * "8. a." → "8a" | "8. b." → "8b" | 10 → "10"
 */
function normaliseItemNo(raw) {
  return String(raw)
    .replace(/\s+/g, "")   // remove all spaces
    .replace(/\./g, "")    // remove dots
    .toLowerCase();        // "8a", "8b", "9a", "10" ...
}

/**
 * Detect whether a cell value looks like an item-number.
 * Matches: "8. a.", "8. b.", "9. a.", "9. b.", 10, "10" etc.
 */
function isItemNo(value) {
  if (value === null || value === undefined) return false;
  const s = String(value).trim();
  if (/^\d+$/.test(s)) return true;
  if (/^\d+\.\s*[a-z]\.$/.test(s.toLowerCase())) return true;
  return false;
}

/**
 * Detect "DIAMETER OF PIPE" sub-header rows — skip them.
 */
function isDiameterHeader(value) {
  if (typeof value !== "string") return false;
  return value.trim().toUpperCase().startsWith("DIAMETER OF PIPE");
}

/**
 * Detect a data row:
 *   col A = null, col B = number (diameter), col C = string (unit), col D = number (rate)
 */
function isDataRow(col1, col2, col3, col4) {
  return (
    (col1 === null || col1 === undefined) &&
    typeof col2 === "number" &&
    typeof col3 === "string" &&
    typeof col4 === "number"
  );
}

/**
 * Detect a section-header row:
 *   col A = item-no, col B = title text, col C & D = empty
 */
function isSectionHeader(col1, col2, col3, col4) {
  return (
    isItemNo(col1) &&
    col2 !== null &&
    col2 !== undefined &&
    (col3 === null || col3 === undefined) &&
    (col4 === null || col4 === undefined)
  );
}

// ═══════════════════════════════════════════════════════════════
// PARSER
// ═══════════════════════════════════════════════════════════════

function parseSheet(sheet) {
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: null });

  const items   = [];
  let   current = null;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];

    const col1 = row[0] ?? null;
    const col2 = row[1] ?? null;
    const col3 = row[2] ?? null;
    const col4 = row[3] ?? null;

    // ── Case 1: New section header ───────────────────────
    if (isSectionHeader(col1, col2, col3, col4)) {
      if (current) items.push(current);
      current = {
        itemNo: normaliseItemNo(col1),
        title:  String(col2).trim(),
        unit:   null,
        rates:  [],
      };
      continue;
    }

    // ── Case 2: DIAMETER sub-header → skip ───────────────
    if (isDiameterHeader(col2)) {
      continue;
    }

    // ── Case 3: Data row ─────────────────────────────────
    if (isDataRow(col1, col2, col3, col4) && current) {
      if (current.unit === null) {
        current.unit = col3.trim();
      }
      current.rates.push({
        diameter: col2,
        rate:     parseFloat(col4.toFixed(2)),
      });
      continue;
    }

    // All other rows (blanks, merged-cell artefacts) are ignored
  }

  if (current) items.push(current);   // flush last section

  return items;
}

// ═══════════════════════════════════════════════════════════════
// VALIDATION
// ═══════════════════════════════════════════════════════════════

function validate(items) {
  const warnings = [];

  items.forEach((item) => {
    if (!item.title)
      warnings.push(`[${item.itemNo}] Missing title.`);

    if (!item.unit)
      warnings.push(`[${item.itemNo}] Missing unit.`);

    if (item.rates.length === 0)
      warnings.push(`[${item.itemNo}] No rate entries found.`);

    item.rates.forEach((r, idx) => {
      if (typeof r.diameter !== "number" || r.diameter <= 0)
        warnings.push(`[${item.itemNo}] Entry #${idx + 1}: invalid diameter "${r.diameter}".`);
      if (typeof r.rate !== "number" || r.rate < 0)
        warnings.push(`[${item.itemNo}] Entry #${idx + 1}: invalid rate "${r.rate}".`);
    });
  });

  return warnings;
}

// ═══════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════

function main() {
  const inputPath  = path.resolve(CONFIG.inputFile);
  const outputPath = path.resolve(CONFIG.outputFile);

  if (!fs.existsSync(inputPath)) {
    console.error(`ERROR: Input file not found → ${inputPath}`);
    process.exit(1);
  }

  console.log(`Reading  : ${inputPath}`);

  let workbook;
  try {
    workbook = XLSX.readFile(inputPath);
  } catch (err) {
    console.error(`ERROR: Could not read Excel file.\n${err.message}`);
    process.exit(1);
  }

  const sheetName = workbook.SheetNames[CONFIG.sheetIndex];
  if (!sheetName) {
    console.error(`ERROR: Sheet index ${CONFIG.sheetIndex} does not exist in workbook.`);
    process.exit(1);
  }

  console.log(`Sheet    : "${sheetName}"`);
  const sheet = workbook.Sheets[sheetName];

  const items    = parseSheet(sheet);
  const warnings = validate(items);

  if (warnings.length > 0) {
    console.warn("\nValidation warnings:");
    warnings.forEach((w) => console.warn("  WARN: " + w));
    console.warn("");
  }

  const output = {
    department:  CONFIG.department,
    source:      path.basename(inputPath),
    generatedAt: new Date().toISOString(),
    totalItems:  items.length,
    items,
  };

  try {
    fs.writeFileSync(outputPath, JSON.stringify(output, null, 2), "utf8");
  } catch (err) {
    console.error(`ERROR: Could not write output file.\n${err.message}`);
    process.exit(1);
  }

  console.log(`\nDone! Parsed ${items.length} item(s):\n`);
  items.forEach((item) => {
    const preview = item.title.length > 60 ? item.title.slice(0, 60) + "..." : item.title;
    console.log(`  [${item.itemNo.padEnd(4)}]  ${String(item.rates.length).padStart(2)} rates  (${item.unit})  ${preview}`);
  });

  const totalEntries = items.reduce((sum, i) => sum + i.rates.length, 0);
  console.log(`\nTotal entries : ${totalEntries}`);
  console.log(`Output written: ${outputPath}\n`);
}

main();