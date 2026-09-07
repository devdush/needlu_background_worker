require("dotenv").config();

const {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
} = require("@aws-sdk/client-sqs");

const mysql = require("mysql2/promise");

// Job handlers
const handlers = {
  transactions: require("./handlers/transactions"),
  payroll: require("./handlers/payroll"),
  csv_upload: require("./handlers/csv_upload"),
  bulk_pay_slips_upload: require("./handlers/bulk_pay_slips_upload"),
  test_worker: require("./handlers/test_handler"),
};

const WORKER_TYPE = process.env.WORKER_TYPE;
const QUEUE_URL = process.env.SQS_QUEUE_URL;
const AWS_REGION = process.env.AWS_REGION || "eu-north-1";

if (!WORKER_TYPE) {
  console.error("❌ WORKER_TYPE is missing");
  process.exit(1);
}

if (!QUEUE_URL) {
  console.error("❌ SQS_QUEUE_URL is missing");
  process.exit(1);
}

if (!handlers[WORKER_TYPE]) {
  console.error(`❌ Unknown worker type: ${WORKER_TYPE}`);
  process.exit(1);
}

const sqs = new SQSClient({
  region: AWS_REGION,
});

async function processMessage(message) {
  let body;
  let pool;

  try {
    body = JSON.parse(message.Body);

    console.log(`📨 Received job ${body.job_id} for ${WORKER_TYPE}`);

    if (!body.job_id) {
      throw new Error("job_id is missing from message");
    }

    const dbInfo = body.db;

    if (!dbInfo) {
      throw new Error("Database information is missing");
    }

    // Optional safety check
    if (body.report_type && body.report_type !== WORKER_TYPE) {
      throw new Error(
        `Wrong job type. Expected ${WORKER_TYPE}, received ${body.report_type}`,
      );
    }
    // async function dbPool() {
    //   return mysql.createPool({
    //     host: process.env.DB_HOST,
    //     port: parseInt(process.env.DB_PORT || "3306"),
    //     user: process.env.DB_USER,
    //     password: process.env.DB_PASS ?? "",
    //     database: process.env.DB_NAME,
    //     waitForConnections: true,
    //     connectionLimit: 5,
    //   });
    // }
    // const pool = await dbPool();
    pool = mysql.createPool({
      host: dbInfo.host,
      user: dbInfo.username,
      password: dbInfo.password,
      database: dbInfo.database,
      waitForConnections: true,
      connectionLimit: 3,
    });

    console.log(`🔄 Starting job ${body.job_id}`);

    await pool.query(
      `
    UPDATE report_jobs
    SET status = 'processing'
    WHERE id = ?
  `,
      [body.job_id],
    );

    await handlers[WORKER_TYPE](body.job_id, pool);

    await sqs.send(
      new DeleteMessageCommand({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: message.ReceiptHandle,
      }),
    );

    console.log(`✅ Job ${body.job_id} completed by ${WORKER_TYPE}`);
  } catch (err) {
    console.error(`❌ Job ${body?.job_id || "unknown"} failed:`, err);

    // Try to update job status
    if (pool && body?.job_id) {
      try {
        await pool.query(
          `
        UPDATE report_jobs
        SET status = 'failed',
            error = ?
        WHERE id = ?
      `,
          [String(err.message || err), body.job_id],
        );
      } catch (dbErr) {
        console.error("❌ Failed to update job status:", dbErr);
      }
    }

    // Do NOT delete the SQS message here.
    // It will become visible again after VisibilityTimeout.
  } finally {
    if (pool) {
      await pool.end();
    }
  }
}

async function run() {
  console.log("=================================");
  console.log("🚀 SQS Worker Started");
  console.log(`👷 Worker Type: ${WORKER_TYPE}`);
  console.log(`🌍 AWS Region: ${AWS_REGION}`);
  console.log(`📦 Queue: ${QUEUE_URL}`);
  console.log(`🆔 PID: ${process.pid}`);
  console.log("=================================");

  while (true) {
    try {
      const response = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: QUEUE_URL,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 20,
          VisibilityTimeout: 300,
        }),
      );

      const messages = response.Messages || [];

      if (messages.length > 0) {
        console.log(`📥 Received ${messages.length} message(s)`);
      }

      // Process messages sequentially inside this worker instance
      for (const message of messages) {
        await processMessage(message);
      }
    } catch (err) {
      console.error("❌ SQS polling error:", err);

      // Avoid a tight error loop
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

run().catch((err) => {
  console.error("💀 Fatal worker error:", err);
  process.exit(1);
});
