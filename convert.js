const fs = require("fs");

// 1️ Read original file
const rawData = JSON.parse(
  fs.readFileSync("originalNested.json", "utf8")
);

const year = rawData.year;
const result = [];

const sections = rawData.sections;

// 2️ Skilled Workmen
for (const subCategory in sections.Skilled_Workmen) {
  sections.Skilled_Workmen[subCategory].forEach(item => {
    result.push({
      year,
      category: "Skilled",
      subCategory: subCategory.replace("_", " "),
      description: item.description,
      unit: item.unit,
      rate: item.rate
    });
  });
}

// 3️ Semi-Skilled
sections.Semi_Skilled_Workmen.forEach(item => {
  result.push({
    year,
    category: "Semi-Skilled",
    subCategory: null,
    description: item.description,
    unit: item.unit,
    rate: item.rate
  });
});

// 4️ Unskilled
sections.Unskilled_Workmen.forEach(item => {
  result.push({
    year,
    category: "Unskilled",
    subCategory: null,
    description: item.description,
    unit: item.unit,
    rate: item.rate
  });
});

// 5️ Conveyance
sections.Other_Conveyance_Items.forEach(item => {
  result.push({
    year,
    category: "Conveyance",
    subCategory: null,
    description: item.description,
    unit: item.unit,
    rate: item.rate
  });
});

// 6️ Save flattened JSON
fs.writeFileSync(
  "flattened.json",
  JSON.stringify(result, null, 2)
);

console.log(" Conversion completed. flattened.json created.");
