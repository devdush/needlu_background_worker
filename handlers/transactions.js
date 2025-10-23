const fs = require("fs/promises");
const path = require("path");
const os = require("os");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

const s3 = new S3Client({ region: "ap-south-1" });
const BUCKET = process.env.S3_BUCKET;

module.exports = async function handleTransactionsCsv(jobId, pool) {
  console.log(jobId);
  const [rows] = await pool.query(
    "SELECT params FROM report_jobs WHERE id = ?",
    [jobId]
  );
  let params = rows[0]?.params ?? {};
  if (typeof params === "string") {
    try {
      params = JSON.parse(params);
    } catch (err) {
      console.error("Failed to parse params JSON:", err);
    }
  }

  const [headers, values] = params.columnNames
    .split("$")
    .map((p) => p.split(","));
  // console.log("headers", headers);
  // console.log("values", values);

  const [data] = await pool.query(params.query);
  //console.log(data);
  const csvLines = [];
  csvLines.push(headers.join(","));

  for (const row of data) {
    const line = values
      .map((col) => (row[col] !== undefined ? row[col] : ""))
      .join(",");
    csvLines.push(line);
  }
  const csvContent = csvLines.join("\n");

  const filename = `report-${jobId}-${Date.now()}.csv`;
  const filepath = path.join(os.tmpdir(), filename);
  await fs.writeFile(filepath, csvContent, "utf8");

  const fileStream = await fs.readFile(filepath);
  const key = `reports/${filename}`;
  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: fileStream,
      ContentType: "text/csv",
    })
  );

  await pool.query(
    "UPDATE report_jobs SET status='completed', s3_key=? WHERE id=?",
    [key, jobId]
  );
  await fs.unlink(filepath);
};
