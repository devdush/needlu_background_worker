const fs = require("fs/promises");
const path = require("path");
const os = require("os");
const { generatePayslipPdf } = require("../services/generatePayslipPdf");

const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

const s3 = new S3Client({
  region: "ap-south-1",
});

const BUCKET = process.env.S3_BUCKET;

module.exports = async function handleBulkPaySlipsUpload(jobId, pool) {
  const taskStartTime = Date.now();

  let outputDir = null;
  let logFilePath = null;
  function getDateTime() {
    const now = new Date();

    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, "0");
    const day = String(now.getDate()).padStart(2, "0");

    const hours = String(now.getHours()).padStart(2, "0");
    const minutes = String(now.getMinutes()).padStart(2, "0");
    const seconds = String(now.getSeconds()).padStart(2, "0");

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }

  async function writeLog(message, level = "INFO") {
    const timestamp = getDateTime();

    const logMessage = `[${timestamp}] [${level}] ${message}\n`;

    console.log(logMessage.trim());

    if (logFilePath) {
      try {
        await fs.appendFile(logFilePath, logMessage, "utf8");
      } catch (logError) {
        console.error("Failed to write task log:", logError);
      }
    }
  }

  try {
    await writeLog(`==================================================`);

    await writeLog(`Starting bulk payslip upload job. Job ID: ${jobId}`);

    const [rows] = await pool.query(
      "SELECT params FROM report_jobs WHERE id = ?",
      [jobId],
    );

    let params = rows[0]?.params ?? {};

    if (typeof params === "string") {
      try {
        params = JSON.parse(params);
      } catch (err) {
        console.error("Failed to parse params JSON:", err);
        throw new Error(`Failed to parse report job params: ${err.message}`);
      }
    }

    console.log("params", params);

    outputDir = path.join(os.tmpdir(), `payslips-${jobId}`);

    await fs.mkdir(outputDir, {
      recursive: true,
    });
    const logFilename = `payslip-upload-${jobId}-${Date.now()}.log`;

    logFilePath = path.join(outputDir, logFilename);

    // Create the log file
    await fs.writeFile(logFilePath, "", "utf8");

    await writeLog(`Bulk payslip upload task started`);

    await writeLog(`Job ID: ${jobId}`);

    await writeLog(`Client ID: ${params.clientId}`);

    await writeLog(`Session User: ${params.sessionUser ?? "N/A"}`);

    await writeLog(`Entity: ${params.entity}`);

    await writeLog(`S3 Bucket: ${BUCKET}`);

    await writeLog(`Temporary output directory: ${outputDir}`);

    await writeLog(`Fetching payslip records from database...`);

    const query = `
      SELECT 
        h.ins AS instance,
        h.c997  AS slip_no,
        h.c1803 AS year,
        h.c1006 AS basic_salary,
        h.c998  AS month,
        h.c999  AS date,
        h.c5315 AS employee_name,
        h.c1001 AS employee_id,
        h.c1002 AS department,
        h.c1003 AS designation,
        h.c4220 AS currency,
        h.c1011 AS gross_salary,
        h.c1016 AS net_payable_salary,
        h.c4590 AS lump_sum_total,
        h.c4707 AS total_arrears,
        h.c4708 AS take_home_amount,
        h.c4090 AS applied_basic_salary_amount,
        h.c1012 AS no_pay_deduction,
        h.c1014 AS salary_advance,
        h.c4218 AS stamp_duty,
        h.c1015 AS epf_employee_contribution_8,

        h.c1793 AS bank_name,
        h.c1794 AS bank_account_no,
        h.c1802 AS branch_name,

        h.c1804 AS epf_no,
        h.c4198 AS pf_liable_total_earning,
        h.c1017 AS epf_employer_contribution_12,
        h.c1018 AS etf_employer_contribution_3,
        h.c4330 AS total_epf_contribution,

        h.c4186 AS basic_salary_arrears,
        h.c4179 AS arrears_ot,
        h.c4187 AS total_allowance_arrears,
        h.c4193 AS arrears_etf_employer,
        h.c4194 AS arrears_epf_employer,
        h.c4195 AS arrears_epf_employee,

        h.c4535 AS NCB1,
        h.c4657 AS tax_apit,

        h.c1010 AS bonus,
        h.c4390 AS bonus_adjustments,
        h.c4562 AS leave_encashment,
        h.c4563 AS medical_exp_reimbursements,
        h.c4564 AS shares_alloted,
        h.c4565 AS other_lump_sum_amount,
        h.c4566 AS description_of_other_lump_sum,

        h.c4587 AS overtime_amount,
        h.c5290 AS salary_adjustment_dates,
        h.c4388 AS basic_salary_adjustment,
        h.c1008 AS over_time_amount,

        h.c4284 AS currency_rate,
        (h.c5290 + h.c4388) AS salary_adjustment,
        h.c3179 AS total_deduction,
        h.c4293 AS cur_epf_employee_contribution_8,
        h.c4295 AS cur_stamp_duty,
        h.c4297 AS cur_total_deduction,
        h.c4298 AS cur_epf_employer_contribution_12,
        h.c4299 AS cur_etf_employer_contribution_3,
        ((h.c4707+h.c1011)*h.c4284)-h.c4297 AS CUR_NET_SALARRY,
        /* Allowance details */
        COALESCE(
          (
            SELECT JSON_ARRAYAGG(
              JSON_OBJECT(
                'description', a.c1919,
                'calculate_prorate_amount', a.c4414,
                'currency_amount', a.c5323  
              )
            )
            FROM portcitybpo.t347 a
            WHERE a.c1917 = h.c1001
              AND a.c1918 = h.c998
              AND a.entity = h.entity
          ),
          JSON_ARRAY()
        ) AS allowance_details,

        /* Deduction details */
        COALESCE(
          (
            SELECT JSON_ARRAYAGG(
              JSON_OBJECT(
                'description', d.c1966,
                'amount', d.c1967,
                'currency_amount', d.c5324
              )
            )
            FROM portcitybpo.t353 d
            WHERE d.c1964 = h.c1001
              AND d.c1965 = h.c998
              AND d.entity = h.entity
          ),
          JSON_ARRAY()
        ) AS deduction_details

      FROM portcitybpo.t180 h

      WHERE h.c998 = ?
        AND h.entity = ?
    `;

    const [data] = await pool.query(query, [params.month, params.entity]);

    await writeLog(`Database query completed. Records found: ${data.length}`);

    function sanitizeS3Part(value) {
      return String(value ?? "")
        .trim()
        .replace(/[\/\\]+/g, "_")
        .replace(/\s+/g, "_");
    }

    async function uploadPayslipToS3({
      filepath,
      clientId,
      entity,
      month,
      employeeId,
    }) {
      const safeEntity = sanitizeS3Part(entity);
      const safeEmployeeId = sanitizeS3Part(employeeId);

      const key =
        `customers/${clientId}/pay_slips/` +
        `${safeEntity}/${month}/${safeEmployeeId}.pdf`;

      const fileBuffer = await fs.readFile(filepath);

      await s3.send(
        new PutObjectCommand({
          Bucket: BUCKET,
          Key: key,
          Body: fileBuffer,
          ContentType: "application/pdf",
        }),
      );

      return key;
    }
    async function uploadLogToS3({ logFilePath, clientId, entity, month }) {
      const safeEntity = sanitizeS3Part(entity);

      const safeMonth = sanitizeS3Part(month);

      const logFilename = path.basename(logFilePath);

      const key =
        `customers/${clientId}/pay_slips_log/` +
        `${safeEntity}/${safeMonth}/${logFilename}`;

      const logBuffer = await fs.readFile(logFilePath);

      await s3.send(
        new PutObjectCommand({
          Bucket: BUCKET,
          Key: key,
          Body: logBuffer,
          ContentType: "text/plain",
        }),
      );

      return key;
    }

    const uploadedFiles = [];

    let successCount = 0;
    let failedCount = 0;

    for (const row of data) {
      const employeeId = row.employee_id;

      const employeeName = row.employee_name;

      const filename = `${employeeId}.pdf`;

      const filepath = path.join(outputDir, filename);

      try {
        await writeLog(`--------------------------------------------------`);

        await writeLog(`Processing employee: ${employeeId}`);

        await writeLog(`Employee name: ${employeeName}`);

        await writeLog(`Month: ${row.month}`);

        await writeLog(`Generating PDF: ${filename}`);

        await generatePayslipPdf(row, filepath, params.entity);

        await writeLog(`PDF generated successfully: ${filepath}`);

        await writeLog(`Uploading PDF to S3...`);

        const s3Key = await uploadPayslipToS3({
          filepath,
          clientId: params.clientId,
          entity: params.entity,
          month: row.month,
          employeeId,
        });
        if (s3Key) {
          try {
            const updateQuery = `UPDATE portcitybpo.t180 SET c5389 = ?, c5390 = ?, c5393 = ? WHERE ins = ? AND entity = ?`;
            let uploadedTime = getDateTime();
            const [result] = await pool.query(updateQuery, [
              s3Key,
              "uploaded",
              uploadedTime,
              row.instance,
              params.entity,
            ]);

            if (result.affectedRows === 0) {
              await writeLog(
                `Database update matched no rows for employee ${employeeId}, instance ${row.instance}`,
                "WARN",
              );
            } else {
              await writeLog(`Database updated for employee ${employeeId}`);
            }
          } catch (error) {
            await writeLog(
              `Failed to update database for employee ${employeeId}: ${error.message}`,
              "ERROR",
            );
            await writeLog(error.stack || String(error), "ERROR");
            console.error(
              `Failed to update database for employee ${employeeId}:`,
              error,
            );
          }
        }

        await writeLog(`PDF uploaded successfully`);

        await writeLog(`S3 key: ${s3Key}`);

        uploadedFiles.push({
          employeeId,
          employeeName,
          s3Key,
        });

        successCount++;

        await fs.unlink(filepath);

        await writeLog(`Temporary PDF deleted: ${filepath}`);

        await writeLog(`Employee ${employeeId} completed successfully`);
      } catch (employeeError) {
        failedCount++;

        await writeLog(
          `Employee ${employeeId} failed: ${employeeError.message}`,
          "ERROR",
        );

        await writeLog(employeeError.stack || String(employeeError), "ERROR");
        try {
          await fs.unlink(filepath);
        } catch (_) {
          // Ignore if file does not exist
        }

        continue;
      }
    }

    const taskDuration = Date.now() - taskStartTime;

    await writeLog(`==================================================`);

    await writeLog(`Bulk payslip upload task completed`);

    await writeLog(`Job ID: ${jobId}`);

    await writeLog(`Total records: ${data.length}`);

    await writeLog(`Successfully uploaded: ${successCount}`);

    await writeLog(`Failed: ${failedCount}`);

    await writeLog(`Execution time: ${taskDuration} ms`);

    await writeLog(`Uploading task log to S3...`);

    const logS3Key = await uploadLogToS3({
      logFilePath,
      clientId: params.clientId,
      entity: params.entity,
      month: data[0]?.month || "unknown",
    });

    console.log(`Task log uploaded successfully: ${logS3Key}`);

    console.log(`Successfully generated and uploaded ${successCount} payslips`);
    if (failedCount <= 0) {
      const updatedTime = getDateTime();
      console.log(`updated Time: ${updatedTime}`);
      const reportJobsUpdateQuery = `UPDATE report_jobs SET status='completed', updated_at = ?, s3_key = ? WHERE id = ?`;
      const updateResult = await pool.query(reportJobsUpdateQuery, [
        updatedTime,
        logS3Key,
        jobId,

      ]);
      if (updateResult[0].affectedRows === 0) {
        await writeLog(
          `Failed to mark job ${jobId} as completed in report_jobs table`,
          "ERROR",
        );
      } else {
        await writeLog(
          `Job ${jobId} marked as completed in report_jobs table`,
          "INFO",
        );
      }
    }
    if (failedCount > 0) {
      await writeLog(`${failedCount} payslip(s) failed`, "ERROR");
    }

    await fs.rm(outputDir, {
      recursive: true,
      force: true,
    });

    console.log(`Temporary directory deleted: ${outputDir}`);

    console.log(`Bulk payslip job ${jobId} completed successfully`);

    return true;
  } catch (err) {
    console.error(`❌ Job ${jobId} failed`, err);

    if (logFilePath) {
      try {
        await writeLog(
          `==================================================`,
          "ERROR",
        );

        await writeLog(`JOB FAILED`, "ERROR");

        await writeLog(`Job ID: ${jobId}`, "ERROR");

        await writeLog(err.stack || String(err), "ERROR");
        if (
          typeof params !== "undefined" &&
          params?.clientId &&
          params?.entity
        ) {
          const logS3Key =
            `customers/${params.clientId}/pay_slips_log/` +
            `${sanitizeS3Part(params.entity)}/` +
            `job-${jobId}/` +
            `${path.basename(logFilePath)}`;

          const logBuffer = await fs.readFile(logFilePath);

          await s3.send(
            new PutObjectCommand({
              Bucket: BUCKET,
              Key: logS3Key,
              Body: logBuffer,
              ContentType: "text/plain",
            }),
          );

          console.error(`Failure log uploaded to S3: ${logS3Key}`);
        }
      } catch (logUploadError) {
        console.error("Failed to upload failure log:", logUploadError);
      }
    }

    await pool.query(
      "UPDATE report_jobs SET status='failed', error = ? WHERE id = ?",
      [String(err), jobId],
    );
    if (outputDir) {
      try {
        await fs.rm(outputDir, {
          recursive: true,
          force: true,
        });
      } catch (cleanupError) {
        console.error("Failed to cleanup temporary directory:", cleanupError);
      }
    }

    return false;
  }
};
