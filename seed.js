// const mongoose = require("mongoose");
// const LabourRate = require("./models/labourRates.js");
// const data = require("./flattened.json");

// async function seedDatabase() {
//   try {
//     // Connect to MongoDB
//     await mongoose.connect("mongodb://127.0.0.1:27017/labourDB");
//     console.log("✅ MongoDB Connected");

//     // Optional: Clear existing data
//     await LabourRate.deleteMany();
//     console.log("🗑 Old data removed");

//     // Insert new data
//     await LabourRate.insertMany(data);
//     console.log("🚀 Data inserted successfully");

//     process.exit();
//   } catch (error) {
//     console.error("❌ Error:", error);
//     process.exit(1);
//   }
// }

// // seedDatabase();
// const mongoose = require("mongoose");
// const LabourRate = require("./models/labourRates.js");
// const data = require("./flattened.json");

// async function seedDatabase() {
//   try {
//     // Connect to MongoDB
//     await mongoose.connect("mongodb://127.0.0.1:27017/labourDB");
//     console.log("✅ MongoDB Connected");

//     // Optional: Clear existing data
//     await LabourRate.deleteMany();
//     console.log("🗑 Old data removed");

//     // Insert new data
//     await LabourRate.insertMany(data);
//     console.log("🚀 Data inserted successfully");

//     process.exit();
//   } catch (error) {
//     console.error("❌ Error:", error);
//     process.exit(1);
//   }
// }

// seedDatabase();
const XLSX = require("xlsx");
const mongoose = require("mongoose");
const LabourRate = require("./models/labourRates.js");

// 🔹 Connect MongoDB
mongoose
  .connect("mongodb://127.0.0.1:27017/labourDB")
  .then(() => console.log("✅ MongoDB Connected"))
  .catch((err) => {
    console.error("❌ MongoDB Connection Error:", err);
    process.exit(1);
  });

async function importExcel() {
  try {
    // 🔹 Read Excel
    const workbook = XLSX.readFile("labourrates.xlsx");
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });

    let currentSection = null;
    let currentSubSection = null;
    const finalData = [];

    for (let row of rows) {
      const description = row[2] ? String(row[2]).trim() : "";
      const unit = row[3] ? String(row[3]).trim() : "";
      const rate = row[4];

      if (!description) continue;

      const lowerDesc = description.toLowerCase();

      // 🔹 Detect Sections (Match Enum Exactly)
      if (lowerDesc.includes("skilled workmen")) {
        currentSection = "Skilled";
        currentSubSection = null;
        continue;
      }

      if (lowerDesc.includes("semi-skilled")) {
        currentSection = "Semi-Skilled";
        currentSubSection = null;
        continue;
      }

      if (lowerDesc.includes("unskilled")) {
        currentSection = "Unskilled";
        currentSubSection = null;
        continue;
      }

      if (lowerDesc.includes("other conveyance")) {
        currentSection = "Conveyance";
        currentSubSection = null;
        continue;
      }

      // 🔹 Detect Subsections
      if (lowerDesc.includes("first class")) {
        currentSubSection = "First Class";
        continue;
      }

      if (lowerDesc.includes("second class")) {
        currentSubSection = "Second Class";
        continue;
      }

      if (lowerDesc.includes("operator")) {
        currentSubSection = "Operator";
        continue;
      }

      // 🔹 Skip invalid rows
      if (!currentSection) continue;
      if (rate === undefined || rate === null) continue;
      if (typeof rate !== "number") continue;

      // 🔹 Push Clean Record
      finalData.push({
        year: "2005-06",
        category: currentSection,
        subCategory: currentSubSection || null,
        description,
        unit,
        rate,
      });
    }

    // 🔹 Remove Old Data (Optional but Recommended for Seeding)
    await LabourRate.deleteMany({});
    console.log("🗑 Old data removed");

    // 🔹 Insert New Data
    await LabourRate.insertMany(finalData);
    console.log(`🚀 ${finalData.length} records inserted successfully`);

    process.exit();
  } catch (error) {
    console.error("❌ Import Failed:", error);
    process.exit(1);
  }
}

importExcel();
