const xlsx = require("xlsx");
const fs = require("fs");
const path = require("path");

// ---------- helpers ----------
function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function isCategoryRow(row) {
  const text = row.join(" ").toLowerCase();

  return (
    text.includes("lowering") &&
    text.includes("pipes") &&
    (text.includes("ci") || text.includes("di")) &&
    text.length > 80
  );
}

// ---------- setup ----------
const INPUT_FILE = path.join(__dirname, "../input/PH.xls");
const OUTPUT_DIR = path.join(__dirname, "../data");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "pipe_rates.json");

ensureDir(OUTPUT_DIR);

// ---------- read excel ----------
const workbook = xlsx.readFile(INPUT_FILE);
const sheet = workbook.Sheets[workbook.SheetNames[0]];
const rows = xlsx.utils.sheet_to_json(sheet, { header: 1 });

// ---------- extraction ----------
let pipeRates = [];

let currentCategory = null;
let currentMaterial = null;

rows.forEach((row, index) => {
  if (!row || row.length === 0) return;

  const rowText = row.join(" ").toLowerCase();

  // 1️⃣ Detect category block
  if (isCategoryRow(row)) {
    currentCategory = row.join(" ").trim();

    if (rowText.includes("ci") && rowText.includes("di")) {
      currentMaterial = "CI/DI";
    } else if (rowText.includes("ci")) {
      currentMaterial = "CI";
    } else if (rowText.includes("di")) {
      currentMaterial = "DI";
    }

    return;
  }

  // 2️⃣ Detect pipe size row (e.g. 80 mm dia)
  const diaMatch = rowText.match(/(\d+)\s*mm/);
  if (!diaMatch || !currentCategory) return;

  const diameter = Number(diaMatch[1]);

  // 3️⃣ Extract unit (usually m / mtrs)
  const unit =
    row.find(v => typeof v === "string" && v.toLowerCase().includes("m")) ||
    "meter";

  // 4️⃣ Extract rate (LAST numeric value in row)
  const numericValues = row.filter(v => typeof v === "number");
  if (numericValues.length === 0) return;

  const rate = numericValues[numericValues.length - 1];

  pipeRates.push({
    material: currentMaterial,
    diameter_mm: diameter,
    unit,
    rate,
    category: currentCategory,
    source_row: index + 1
  });
});

// ---------- write output ----------
fs.writeFileSync(OUTPUT_FILE, JSON.stringify(pipeRates, null, 2));
console.log("✅ Pipe rates extracted successfully");
