const dotenv = require("dotenv");
const {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} = require("@aws-sdk/client-sqs");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const mysql = require("mysql2/promise");
const fs = require("fs/promises");
const path = require("path");
const os = require("os");

dotenv.config();

const REGION = process.env.AWS_REGION;
const QUEUE_URL = process.env.SQS_QUEUE_URL;
const BUCKET = process.env.S3_BUCKET;

const sqs = new SQSClient({ region: "eu-north-1" });
const s3 = new S3Client({ region: "ap-south-1" });

async function dbPool() { 
  return mysql.createPool({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT ? parseInt(process.env.DB_PORT) : 3306,
    user: process.env.DB_USER,
    password: process.env.DB_PASS ?? "", // ensure empty string if undefined
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 5,
  });
}

async function processMessage(message, pool) {
  console.log("📩 Raw message body:", message.Body);
  const body = JSON.parse(message.Body);
  const jobId = body.job_id;

  console.log(`Processing job ${jobId}`);

  // Mark job as processing
  await pool.query("UPDATE report_jobs SET status='processing' WHERE id = ?", [
    jobId, 
  ]);

  try {
    const [rows] = await pool.query(
      "SELECT params FROM report_jobs WHERE id = ?",
      [jobId]
    );
    const params = rows[0] ? JSON.parse(rows[0].params) : {};
    console.log("Haritha", params);

    const [data] = await pool.query(params.query);

    const csvLines = ["form_IDTEST,form_name"];
    for (const r of data) {
      console.log("R",r)
      csvLines.push(`${r.form_ID},${r.form_name}`);
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
      "UPDATE report_jobs SET status='completed', s3_key = ? WHERE id = ?",
      [key, jobId]
    );

    await fs.unlink(filepath);

    console.log(`✅ Job ${jobId} completed`);
    return true;
  } catch (err) {
    console.error(`❌ Job ${jobId} failed`, err);
    await pool.query(
      "UPDATE report_jobs SET status='failed', error = ? WHERE id = ?",
      [String(err), jobId]
    );
    return false;
  }
}

async function run() {
  const pool = await dbPool();

  try {
    await pool.query("SELECT 1");
    console.log("✅ MySQL connection OK");
  } catch (err) {
    console.error("❌ MySQL connection failed", err);
    process.exit(1);
  }

  while (true) {
    try {
      const res = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: QUEUE_URL,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 20,
          VisibilityTimeout: 300,
        })
      );

      if (!res.Messages || res.Messages.length === 0) continue;

      for (const m of res.Messages) {
        const ok = await processMessage(m, pool);
        if (ok) {
          await sqs.send(
            new DeleteMessageCommand({
              QueueUrl: QUEUE_URL,
              ReceiptHandle: m.ReceiptHandle,
            })
          );
        } else {
          console.log("Job failed — it will retry or go to DLQ");
        }
      }
    } catch (err) {
      console.error("Worker loop error", err);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

run().catch((err) => console.error("Fatal worker error", err));
