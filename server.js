const dotenv = require("dotenv");
const express = require("express");
const mysql = require("mysql2/promise");

dotenv.config();
const app = express();
const PORT = process.env.PORT || 4000;

// DB connection pool
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 5,
});

// Health check
app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "report-service" });
});

// List jobs
app.get("/jobs", async (req, res) => {
  const [rows] = await pool.query(
    "SELECT * FROM report_jobs ORDER BY created_at DESC LIMIT 20"
  );
  res.json(rows);
});

// Get single job
app.get("/jobs/:id", async (req, res) => {
  const [rows] = await pool.query("SELECT * FROM report_jobs WHERE id = ?", [
    req.params.id,
  ]);
  if (rows.length === 0) {
    return res.status(404).json({ error: "Job not found" });
  }
  res.json(rows[0]);
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
