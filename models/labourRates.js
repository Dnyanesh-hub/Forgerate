const mongoose = require("mongoose");

const labourRateSchema = new mongoose.Schema(
  {
    year: { type: String, required: true },

    category: {
      type: String,
      required: true,
      enum: ["Skilled", "Semi-Skilled", "Unskilled", "Conveyance"]
    },

    subCategory: { type: String, default: null },

    description: { type: String, required: true },

    unit: { type: String, required: true },

    rate: { type: Number, required: true }
  },
  { timestamps: true }
);

module.exports = mongoose.model("LabourRate", labourRateSchema);
