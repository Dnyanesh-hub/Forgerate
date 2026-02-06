const fs = require("fs");
const path = require("path");

const raw = JSON.parse(
  fs.readFileSync(path.join(__dirname, "../data/raw/PH_raw.json"))
);

let clean = {
  project: {
    name: "PH Project",
    sanctioned_year: null,
    sanctioned_amount: null,
    estimated_completion_year: null
  },
  material_usage: []
};

raw.sheets.forEach(sheet => {
  sheet.rows.forEach(row => {
    const text = Object.values(row.raw_cells)
      .join(" ")
      .toString()
      .toLowerCase();

    // Example: detect sanctioned amount
    if (text.includes("sanctioned")) {
      const match = text.match(/\d{6,}/);
      if (match) {
        clean.project.sanctioned_amount = Number(match[0]);
      }
    }

    // Detect year-wise steel usage (your example logic)
    if (text.includes("ton") && text.includes("k")) {
      const yearMatch = text.match(/20\d{2}/);
      const qtyMatch = text.match(/(\d+)\s*ton/);
      const rateMatch = text.match(/(\d+)\s*k/);

      if (yearMatch && qtyMatch && rateMatch) {
        clean.material_usage.push({
          material: "Steel",
          unit: "ton",
          year: Number(yearMatch[0]),
          quantity: Number(qtyMatch[1]),
          rate_per_ton: Number(rateMatch[1]) * 1000,
          source_row: row.row_number
        });
      }
    }
  });
});

fs.writeFileSync(
  path.join(__dirname, "../data/clean/PH_clean.json"),
  JSON.stringify(clean, null, 2)
);

console.log("✅ CLEAN JSON generated");
