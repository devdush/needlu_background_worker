const fs = require("fs/promises");
const path = require("path");
const os = require("os");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

const s3 = new S3Client({ region: "ap-south-1" });
const BUCKET = process.env.S3_BUCKET;

module.exports = async function handlePayrollCsv(jobId, pool) {
  try {
    console.log(jobId);
    const [rows] = await pool.query(
      "SELECT params FROM report_jobs WHERE id = ?",
      [jobId]
    );
    const params = rows[0] ? JSON.parse(rows[0].params) : {};
    const [data] = await pool.query(
      " SELECT t766.c4822, t766.c4823, t766.c4824, t767.c4826,ROUND((t766.c4824 / 21) * t767.c4826, 2) AS monthly_salary FROM t766 JOIN t767 ON t766.c4823 = t767.c4827"
    );
    const csvLines = ["ID,Name,Basic,Worked Days, monthly Salary"];
    for (const r of data) {
      console.log(data);
      csvLines.push(
        `${r.c4822},${r.c4823},${r.c4824},${r.c4826},${r.monthly_salary}`
      );
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
  } catch (err) {
    console.error(`❌ Job ${jobId} failed`, err);
    await pool.query(
      "UPDATE report_jobs SET status='failed', error = ? WHERE id = ?",
      [String(err), jobId]
    );
    return false;
  }
};
