require("dotenv").config();

module.exports = {
  apps: [

    {
      name: "worker-csv-upload",
      script: "./Main.js",
      cwd: "/home/ec2-user/background_worker",

      instances: 3,
      exec_mode: "fork",

      env: {
        WORKER_TYPE: "csv_upload",
        SQS_QUEUE_URL: process.env.CSV_UPLOAD_QUEUE_URL,
        AWS_REGION: process.env.AWS_REGION,
        S3_BUCKET: process.env.S3_BUCKET,
      },
    },

    {
      name: "worker-bulk-pay-slips",
      script: "./Main.js",
      cwd: "/home/ec2-user/background_worker",
      instances: 1,
      exec_mode: "fork",

      env: {
        WORKER_TYPE: "bulk_pay_slips_upload",
        SQS_QUEUE_URL: process.env.BULK_PAY_SLIPS_UPLOAD_QUEUE_URL,
        AWS_REGION: process.env.AWS_REGION,
        S3_BUCKET: process.env.S3_BUCKET,
      },
    },
  ],
};
