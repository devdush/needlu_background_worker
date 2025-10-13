require("dotenv").config();
const {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} = require("@aws-sdk/client-sqs");
const mysql = require("mysql2/promise");

// --- Registry of job handlers ---
const handlers = {
  transactions: require("./handlers/transactions"),
  payroll: require("./handlers/payroll"),
  //   cleanup: require("./handlers/cleanup"),
};

const sqs = new SQSClient({ region: "eu-north-1" });
const QUEUE_URL = process.env.SQS_QUEUE_URL;
const WORKER_TYPE = process.env.WORKER_TYPE;

if (!handlers[WORKER_TYPE]) {
  console.error(`❌ Unknown worker type: ${WORKER_TYPE}`);
  process.exit(1);
}

async function dbPool() {
  return mysql.createPool({
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT || "3309"),
    user: process.env.DB_USER,
    password: process.env.DB_PASS ?? "",
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 5,
  });
}

async function run() {
  const pool = await dbPool();
  console.log(`🚀 Worker started for type: ${WORKER_TYPE}`);

  while (true) {
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
      const body = JSON.parse(m.Body);
      console.log(body);
      if (body.report_type !== WORKER_TYPE) continue; // filter by type

      try {
        await pool.query(
          "UPDATE report_jobs SET status='processing' WHERE id=?",
          [body.job_id]
        );
        await handlers[WORKER_TYPE](body.job_id, pool);
        await sqs.send(
          new DeleteMessageCommand({
            QueueUrl: QUEUE_URL,
            ReceiptHandle: m.ReceiptHandle,
          })
        );
        console.log(`✅ Job ${body.job_id} processed by ${WORKER_TYPE} worker`);
      } catch (err) {
        console.error(`❌ Job ${body.job_id} failed`, err);
        await pool.query(
          "UPDATE report_jobs SET status='failed', error=? WHERE id=?",
          [String(err), body.job_id]
        );
      }
    }
  }
}

run().catch((err) => console.error("Fatal worker error", err));
