/**
 * Public Health SSR → MongoDB Direct Importer
 * =============================================
 * Reads publichealth.xlsx and stores structured data directly into MongoDB
 * using the best schema design for querying and maintenance.
 *
 * SETUP:
 *   npm install xlsx mongodb dotenv
 *
 * ENV (.env file):
 *   MONGO_URI=mongodb://localhost:27017
 *   DB_NAME=public_health_ssr
 *
 * RUN:
 *   node storeToMongoDB.js
 *   node storeToMongoDB.js ./publichealth.xlsx   ← custom path
 */

require('dotenv').config();
const XLSX     = require('xlsx');
const { MongoClient, ObjectId } = require('mongodb');

// ─────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────
const MONGO_URI  = process.env.MONGO_URI  || 'mongodb://localhost:27017';
const DB_NAME    = process.env.DB_NAME    || 'public_health_ssr';
const INPUT_FILE = process.argv[2]        || 'publichealth.xlsx';

// ─────────────────────────────────────────────────────────────
// CATEGORY MAP
// ─────────────────────────────────────────────────────────────
const CATEGORY_MAP = {
  '1':   'Labour Rates',
  '2':   'Earth Work',
  '3':   'Rock Cutting & Blasting',
  '3a':  'Rock Cutting & Blasting',
  '3b':  'Rock Cutting & Blasting',
  '3c':  'Rock Cutting & Blasting',
  '3d':  'Rock Cutting & Blasting',
  '3e':  'Rock Cutting & Blasting',
  '3f':  'Rock Cutting & Blasting',
  '4':   'Loading & Unloading',
  '5':   'Loading & Unloading',
  '6':   'Loading & Unloading',
  '7':   'Loading & Unloading',
  '8a':  'Pipe Laying',
  '8b':  'Pipe Laying',
  '9a':  'Pipe Jointing',
  '9b':  'Pipe Jointing',
  '10':  'Pipe Jointing',
  '11a': 'RCC Pipe Laying',
  '11b': 'RCC Pipe Laying',
  '12':  'GI/PVC/HDPE Pipe Laying',
  '13':  'AC Pressure Pipe Laying',
  '14':  'AC Pressure Pipe Jointing',
  '15':  'Stoneware Pipe Laying',
  '16':  'PVC Pipe Laying & Testing',
  '17':  'Valve Installation Labour',
  '18a': 'Air Valve Labour',
  '18b': 'Kinetic Air Valve Labour',
  '19':  'Fire Hydrant Labour',
  '20':  'CI/DI Pipe Uprooting',
  '21':  'RCC Pipe Uprooting',
  '22':  'GI/PVC/HDPE Pipe Removal',
  '23':  'CI/DI Pipe Cutting',
  '24':  'AC Pipe Cutting',
  '25':  'Drilling & Tapping',
  '26':  'Road Surface Cutting',
  '27':  'Dewatering',
  '28':  'Shoring & Strutting',
  '29':  'Barricading & Watching',
  '30':  'Underwater Trench Excavation',
  '31':  'Infiltration Gallery',
  '32':  'Infiltration Gallery',
  '33':  'Centering & Scaffolding',
  '34':  'Lift & Delift of Materials',
  '35':  'Fixtures Labour',
  '36':  'Sanitary Fixtures Labour',
  '37':  'Sanitary Fixtures Labour',
  '38':  'Sanitary Fixtures Labour',
  '39':  'Sanitary Fixtures Labour',
  '40':  'Trench Refilling',
  '41a': 'Isolated Scattered Works',
  '41b': 'Repairs to Mains',
  '42':  'Silt & Sludge Removal',
  '43':  'Pipe Conveyance',
  '44':  'Pipe Conveyance',
  '45':  'Pipe Conveyance',
  '46':  'Pipe Conveyance',
  '47':  'Pipe Conveyance',
  '48':  'Well Sinking',
  '49':  'Well Sinking',
  '50':  'Open Well Excavation',
  '51':  'OHSR/ELSR Rates (Kilo Litres)',
  '52':  'OHSR/ELSR Rates (Litres)',
  '53':  'Rapid Gravity Filtration Plant',
};

// ─────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────
const clean = (v) => (v == null ? null : String(v).trim().replace(/\s+/g, ' ') || null);

const parseRate = (v) => {
  if (v == null) return null;
  if (typeof v === 'number') return v;
  const s = String(v).trim();
  if (!s) return null;
  const n = parseFloat(s);
  return isNaN(n) ? s : n;
};

const snoKey = (sno) =>
  sno == null ? null : String(sno).replace(/[\s.]+/g, '').toLowerCase();

const isSectionHeader = (sno, unit, rate) => {
  if (sno == null) return false;
  if (typeof sno === 'number') return unit == null && rate == null;
  return /^\d+\s*\.?\s*[a-z]?\.?$/i.test(String(sno).trim()) && unit == null && rate == null;
};

const isSimpleCompound = (sno) => {
  if (!sno) return false;
  return /^(8a|8b|9a|9b|11a|11b|18a|18b|41a|41b)$/i.test(
    String(sno).replace(/[\s.]+/g, '').toLowerCase()
  );
};

const isSubSection = (sno, unit, rate) =>
  sno != null &&
  /^[a-zA-Z]$/.test(String(sno).trim()) &&
  unit == null &&
  rate == null;

const isSkip = (desc) => {
  if (!desc) return false;
  return (
    /^DIAMETER OF PIPE/i.test(desc) ||
    /^DIA (in|of)/i.test(desc) ||
    /^NOTE\b/i.test(desc) ||
    /^\s{4,}/.test(desc)
  );
};

// ─────────────────────────────────────────────────────────────
// PARSE EXCEL → In-memory document array
// ─────────────────────────────────────────────────────────────
function parseExcel(filePath) {
  const workbook = XLSX.readFile(filePath);
  const ws = workbook.Sheets[workbook.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

  const sections = [];
  let currentSection = null;
  let currentSubSection = null;

  for (let i = 2; i < rows.length; i++) {
    const [, rawSno, rawDesc, rawUnit, rawRate] = [...rows[i], null, null, null, null, null];

    const sno  = rawSno != null ? (typeof rawSno === 'string' ? rawSno.trim() : rawSno) : null;
    const desc = clean(rawDesc);
    const unit = clean(rawUnit);
    const rate = parseRate(rawRate);
    const key  = snoKey(sno);

    if (!sno && !desc && !unit && rate == null) continue;

    // ── COMPOUND ITEMS (8a, 8b, 9a, 9b, 11a, 11b…)
    if (sno && isSimpleCompound(sno)) {
      currentSection = buildSection(sno, key, desc, unit, rate);
      currentSubSection = null;
      sections.push(currentSection);
      continue;
    }

    // ── MAIN SECTION HEADER
    if (isSectionHeader(sno, unit, rate)) {
      currentSection = buildSection(sno, key, desc, unit, rate);
      currentSubSection = null;
      sections.push(currentSection);
      continue;
    }

    // ── SUB-SECTION (letter a/b/c)
    if (isSubSection(sno, unit, rate) && currentSection) {
      currentSubSection = { sub_id: String(sno).trim(), description: desc, items: [] };
      currentSection.sub_sections.push(currentSubSection);
      continue;
    }

    // ── AUTO SUB-SECTION (GI PIPES:, PVC/HDPE… headers inside item 12)
    if (currentSection && !sno && desc && !isSkip(desc)) {
      if (/G\.I\. PIPES|PVC.HDPE pipes/i.test(desc)) {
        currentSubSection = {
          sub_id: 'auto_' + (currentSection.sub_sections.length + 1),
          description: desc,
          items: [],
        };
        currentSection.sub_sections.push(currentSubSection);
        continue;
      }
    }

    // ── RATE ITEM (has unit + rate)
    if (currentSection && unit && rate != null) {
      const item = buildRateItem(desc, unit, rate);
      if (currentSubSection) currentSubSection.items.push(item);
      else currentSection.items.push(item);
      continue;
    }

    // ── RATE-ONLY ROW (item 33 scaffolding — rate in col E, desc in col C)
    if (currentSection && rate != null && !unit && !sno) {
      const target = currentSubSection ? currentSubSection.items : currentSection.items;
      if (desc && target.length === 0) {
        target.push(buildRateItem(desc, null, rate));
      } else if (target.length > 0 && target[target.length - 1].rate == null) {
        target[target.length - 1].rate = rate;
      } else {
        target.push(buildRateItem(desc, null, rate));
      }
    }
  }

  return sections;
}

function buildSection(sno, key, desc, unit, rate) {
  return {
    item_no:      typeof sno === 'number' ? sno : String(sno),
    item_key:     key,
    category:     CATEGORY_MAP[key] || CATEGORY_MAP[String(sno)] || 'General',
    title:        desc,
    unit:         unit,
    rate:         rate,
    rate_type:    rate == null ? null : (typeof rate === 'number' ? 'numeric' : 'formula'),
    sub_sections: [],
    items:        [],
  };
}

function buildRateItem(desc, unit, rate) {
  return {
    dimension:  desc ? desc.replace(/\s*mm$/i, '').trim() : null,
    unit:       unit ? unit.trim() : null,
    rate:       rate,
    rate_type:  typeof rate === 'number' ? 'numeric' : 'formula',
  };
}

// ─────────────────────────────────────────────────────────────
// MONGODB SCHEMA DESIGN
// ─────────────────────────────────────────────────────────────
/*
  COLLECTION: ssr_sections
  ─────────────────────────
  Each section is one document. Rate items are embedded as arrays.
  This is optimal because:
  - Sections are always queried together with their items
  - Items are never queried standalone (always in context of section)
  - Array sizes are bounded (max ~30 diameter entries per section)
  - Atomic updates per section

  DOCUMENT SHAPE:
  {
    _id: ObjectId,
    metadata: { title, year, source_file, imported_at },
    item_no: 1,
    item_key: "1",
    category: "Labour Rates",
    title: "RATES OF LABOUR...",
    unit: null,
    rate: null,
    rate_type: null,
    sub_sections: [
      {
        sub_id: "a",
        description: "G.I. PIPES",
        items: [ { dimension, unit, rate, rate_type }, ... ]
      }
    ],
    items: [ { dimension, unit, rate, rate_type }, ... ]
  }
*/

// ─────────────────────────────────────────────────────────────
// STORE TO MONGODB
// ─────────────────────────────────────────────────────────────
async function storeToMongoDB(sections) {
  const client = new MongoClient(MONGO_URI);

  try {
    await client.connect();
    console.log('  Connected to MongoDB');

    const db = client.db(DB_NAME);

    // ── 1. METADATA collection (one document per import)
    const metaCol = db.collection('ssr_metadata');
    const metaDoc = {
      title:       'Public Health Items - Schedule of Standard Rates',
      year:        '2005-06',
      source_file: INPUT_FILE,
      imported_at: new Date(),
      total_sections: sections.length,
      total_items: sections.reduce((sum, s) => {
        return sum + s.items.length +
          s.sub_sections.reduce((a, ss) => a + ss.items.length, 0);
      }, 0),
    };
    const metaResult = await metaCol.insertOne(metaDoc);
    const importId = metaResult.insertedId;
    console.log(`  Metadata stored (importId: ${importId})`);

    // ── 2. SECTIONS collection — drop old data first (idempotent import)
    const secCol = db.collection('ssr_sections');
    await secCol.deleteMany({ import_id: importId });

    // Attach import_id + metadata ref to each section doc
    const docs = sections.map(sec => ({
      import_id:   importId,
      metadata: {
        title:       metaDoc.title,
        year:        metaDoc.year,
        source_file: metaDoc.source_file,
        imported_at: metaDoc.imported_at,
      },
      ...sec,
    }));

    const insertResult = await secCol.insertMany(docs, { ordered: true });
    console.log(`📦  Inserted ${insertResult.insertedCount} section documents`);

    // ── 3. CREATE INDEXES for common query patterns
    await secCol.createIndex({ item_key: 1 },            { unique: false });
    await secCol.createIndex({ category: 1 }),
    await secCol.createIndex({ 'metadata.year': 1 });
    await secCol.createIndex({ 'items.rate': 1 });
    await secCol.createIndex({ 'sub_sections.items.rate': 1 });
    // Text index for full-text search on section titles
    await secCol.createIndex(
      { title: 'text', 'sub_sections.description': 'text' },
      { name: 'text_search_idx' }
    );
    console.log('  Indexes created');

    // ── 4. PRINT SAMPLE QUERIES
    printSampleQueries(DB_NAME);

    return { importId, sectionsInserted: insertResult.insertedCount };

  } finally {
    await client.close();
    console.log('🔌  Connection closed');
  }
}

// ─────────────────────────────────────────────────────────────
// SAMPLE QUERIES GUIDE
// ─────────────────────────────────────────────────────────────
function printSampleQueries(dbName) {
  console.log(`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  SAMPLE MONGODB QUERIES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 1. Get rate for CI/DI pipe laying, 300mm diameter (S/S ends)
db.ssr_sections.findOne(
  { item_key: "8a" },
  { items: { $elemMatch: { dimension: "300" } } }
)

// 2. Get all sections in a category
db.ssr_sections.find({ category: "Pipe Jointing" })

// 3. Get all numeric rates for stoneware pipe laying (item 15)
db.ssr_sections.findOne(
  { item_key: "15" },
  { title: 1, items: 1 }
)

// 4. Find sections where rate > 100 (direct items)
db.ssr_sections.find({ "items.rate": { $gt: 100 } })

// 5. Full text search
db.ssr_sections.find({ $text: { $search: "air valve" } })

// 6. Get all GI pipe rates for 50mm diameter
db.ssr_sections.aggregate([
  { $match: { item_key: "12" } },
  { $unwind: "$sub_sections" },
  { $match: { "sub_sections.sub_id": "a" } },  // "a" = GI PIPES
  { $unwind: "$sub_sections.items" },
  { $match: { "sub_sections.items.dimension": "50" } },
  { $project: { "sub_sections.items": 1, _id: 0 } }
])

// 7. List all categories
db.ssr_sections.distinct("category")

// 8. Get all sections for year 2005-06
db.ssr_sections.find({ "metadata.year": "2005-06" })
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);
}

// ─────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────
async function main() {
  console.log(`\n  Public Health SSR → MongoDB Importer`);
  console.log(`   File   : ${INPUT_FILE}`);
  console.log(`   DB     : ${MONGO_URI} / ${DB_NAME}\n`);

  console.log('  Parsing Excel file...');
  const sections = parseExcel(INPUT_FILE);
  console.log(`   Found ${sections.length} sections`);

  console.log('  Storing to MongoDB...');
  const result = await storeToMongoDB(sections);

  console.log(`\n  Done! ${result.sectionsInserted} sections stored.`);
  console.log(`   importId: ${result.importId}\n`);
}

main().catch(err => {
  console.error('  Fatal error:', err);
  process.exit(1);
});