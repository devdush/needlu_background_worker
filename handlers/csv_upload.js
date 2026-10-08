const csv = require("csv-parser");
const fs = require("fs/promises");
const path = require("path");
const os = require("os");

const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} = require("@aws-sdk/client-s3");

const s3 = new S3Client({ region: "ap-south-1" });
const BUCKET = process.env.S3_BUCKET;
function getDateTime() {
  const now = new Date();

  const sriLankaTime = new Date(
    now.toLocaleString("en-US", {
      timeZone: "Asia/Colombo",
    }),
  );

  const year = sriLankaTime.getFullYear();
  const month = String(sriLankaTime.getMonth() + 1).padStart(2, "0");
  const day = String(sriLankaTime.getDate()).padStart(2, "0");

  const hours = String(sriLankaTime.getHours()).padStart(2, "0");
  const minutes = String(sriLankaTime.getMinutes()).padStart(2, "0");
  const seconds = String(sriLankaTime.getSeconds()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}
// ---------------------------
// CHECKS THE STAGING TABLE
// ---------------------------
async function tableExists(tableName, pool) {
  const escapedTable = pool.escape(tableName);

  const query = `SHOW TABLES LIKE ${escapedTable}`;

  const [rows] = await pool.query(query);

  if (rows.length > 0) {
    return true;
  } else {
    return false;
  }
}
// ---------------------------
// CREATE THE STAGING TABLE
// ---------------------------

async function createStagingTable(mainTable, pool) {
  if (!pool) {
    throw new Error("Database connection failed.");
  }

  const stagingTable = `staging_${mainTable}`;

  try {
    // 1. Clean slate
    await pool.query(`DROP TABLE IF EXISTS \`${stagingTable}\``);

    // 2. Clone structure ONLY
    // No keys, no auto_increment, no constraints
    await pool.query(`
            CREATE TABLE \`${stagingTable}\`
            AS SELECT * FROM \`${mainTable}\`
            WHERE 1 = 0
        `);

    // 3. Remove unwanted columns if they exist
    const columnsToDrop = ["ins", "inspar"];

    for (const col of columnsToDrop) {
      const [checkCol] = await pool.query(
        `SHOW COLUMNS FROM \`${stagingTable}\` LIKE ?`,
        [col],
      );

      if (checkCol.length > 0) {
        await pool.query(
          `ALTER TABLE \`${stagingTable}\` DROP COLUMN \`${col}\``,
        );
      }
    }

    // 4. Add tracking columns and indexes
    const timestamp = Date.now();

    const sql = `
            ALTER TABLE \`${stagingTable}\`
            ADD COLUMN \`batch_id\` VARCHAR(50) NOT NULL,
            ADD COLUMN \`row_number\` INT,
            ADD COLUMN \`import_status\`
                ENUM('pending', 'completed', 'failed')
                DEFAULT 'pending',
            ADD COLUMN \`validation_error\` TEXT,
            ADD INDEX \`idx_batch_${timestamp}\` (\`batch_id\`),
            ADD INDEX \`idx_status_${timestamp}\` (\`import_status\`)
        `;

    await pool.query(sql);

    return stagingTable;
  } catch (err) {
    throw new Error(err.message);
  }
}
async function getFieldInfo(fieldId, pool) {
  const [rows] = await pool.query("SELECT * FROM data_item WHERE ID = ?", [
    fieldId,
  ]);
  return rows[0];
}
async function getFormName(formId, pool) {
  const [rows] = await pool.query(
    "SELECT form_name FROM data_form WHERE form_ID = ?",
    [formId],
  );
  return rows[0]?.form_name;
}
async function getEntity(formId, pool) {
  const [rows] = await pool.query(
    "SELECT data_entry_level FROM data_form WHERE form_ID = ?",
    [formId],
  );
  const [rows2] = await pool.query(
    "SELECT entity_name FROM entity WHERE entity_type = ?",
    [rows[0]?.data_entry_level],
  );
  return rows2[0]?.entity_name;
}

// helper: convert S3 stream → Node stream
const streamToNodeStream = (body) => {
  const { Readable } = require("stream");
  return Readable.from(body);
};

//work first
module.exports = async function handleCSVUpload(
  jobId,
  pool,
  token,
  databaseName,
  env111,
) {
  console.log("in progess");
  const errorDetails = [];
  const allErrors = [];
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
    }
  }

  const file_url = params.file_url;
  const entity = params.entity;
  const form = params.form;
  const fields = params.fields;
  const staging_table = params.staging_table;
  //const batch_id = params.batch_id;
  const sessionUser = params.user_id;
  const batchId = params.batch_id;
  const validation = params.validation;
  const frequency = params.frequency;

  const result = await tableExists(staging_table, pool);

  //If staging table doesn't exist, clone structure
  if (!result) {
    const mainTable = `t${form}`;

    // Check if 'user' column exists
    const [result] = await pool.query(
      `SHOW COLUMNS FROM \`${mainTable}\` LIKE 'user'`,
    );

    // If column does not exist, add it
    if (result.length === 0) {
      const alterQuery = `
                  ALTER TABLE \`${mainTable}\`
                  ADD COLUMN \`user\` VARCHAR(50) DEFAULT NULL
              `;

      await pool.query(alterQuery);
    }

    // Create staging table
    await createStagingTable(mainTable, pool);
  }

  try {
    const importResult = await importFileFromS3({
      file_url,
      fields,
      entity,
      batchId,
      sessionUser,
      staging_table,
      pool,
      errorDetails,
    });
    // ==========================================
    // NOW ALL DATA IS IN STAGING TABLE
    // START VALIDATION HERE
    // ==========================================
    //Date validation
    await validateDateFields({
      staging_table,
      batchId,
      fields,
      pool,
      errorDetails,
    });

    // Next:
    const validationRules = parseValidation(validation);
    await applyStagingRules({
      staging_table,
      batchId,
      pool,
      validationRules,
      errorDetails,
    });

    await validateOptionsFields({
      staging_table,
      fields,
      entity,
      batchId,
      pool,
      errorDetails,
    });
    console.log(errorDetails);
    if (errorDetails.length > 0) {
      const errorMessage = errorDetails.join("\n");

      await pool.query(
        `
              UPDATE report_jobs
              SET
                  status = 'failed',
                  error = ?,
                  notification_status = 'unread',
                  updated_at = ?
              WHERE id = ?
              `,
        [errorMessage, getDateTime(), jobId],
      );
      return {
        code: "failed",
        message: "File validation failed",
        errorDetails,
      };
    }
    let successCount = 0;

    // Get all rows from staging table for this batch
    const [stagingRows] = await pool.query(
      `SELECT * FROM \`${staging_table}\` WHERE batch_id = ?`,
      [batchId],
    );
    console.log("save staging_table!!!");
    // ---------------------------------------------------------
    // SAVE DATA
    // ---------------------------------------------------------

    for (const row of stagingRows) {
      try {
        const insertCols = ["entity", "user"];

        const insertVals = [row.entity, row.user];

        const fieldValues = {};

        fields.forEach((fld) => {
          const colName = `c${fld}`;
          fieldValues[fld] = row[colName] ?? "";
        });

        const payload = {
          client_id: "databaseName", //
          sessionUser: sessionUser || "",
          entity: entity || "",
          form: form,
          frequency: frequency,
          instance: "",
          parent_ins: "",
          select_ins: 0,
          ...fieldValues,
        };

        const apiUrl = `https://${databaseName}.needlu.${env111}/api/save`;

        const response = await fetch(
          `https://${databaseName}.needlu.${env111}/api/save`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${token}`,
            },
            body: JSON.stringify(payload),
          },
        );

        const contentType = response.headers.get("content-type") || "";

        let resJson;

        if (contentType.includes("application/json")) {
          resJson = await response.json();
        } else {
          const text = await response.text();
          throw new Error(
            `API returned non-JSON response. HTTP ${response.status}`,
          );
        }

        if (!response.ok) {
          throw new Error(
            resJson?.message ||
              `API request failed with HTTP ${response.status}`,
          );
        }

        if (resJson.code === "failed") {
          const errMessage = resJson.message || "Unknown error";
          errorDetails.push(`Line ${row.row_number}: ${errMessage}`);
          // errorCount++;
        } else if (resJson.code === "success") {
          await pool.query(
            `
                    UPDATE \`${staging_table}\`
                    SET
                        \`import_status\` = 'completed',
                        \`validation_error\` = NULL
                    WHERE \`batch_id\` = ?
                      AND \`row_number\` = ?
                    `,
            [batchId, row.row_number],
          );
          successCount++;
        }
      } catch (err) {
        console.log(err);
        let errMessage = err.message;

        if (err.code === "ER_DUP_ENTRY") {
          const match = err.sqlMessage?.match(
            /Duplicate entry '(.+?)' for key/,
          );

          const duplicateValue = match ? match[1] : "";

          errMessage = duplicateValue
            ? `Duplicate entry  '${duplicateValue}'`
            : "Duplicate record found";
        }

        errorDetails.push(`Line ${row.row_number}: ${errMessage}`);

        await pool.query(
          `
                    UPDATE \`${staging_table}\`
                    SET
                        \`import_status\` = 'failed',
                        \`validation_error\` = ?
                    WHERE \`batch_id\` = ?
                      AND \`row_number\` = ?
                    `,
          [errMessage, batchId, row.row_number],
        );
      }
    }
    if (errorDetails.length > 0 && successCount === 0) {
      const errorMessage = errorDetails.join("\n");
      console.log(errorDetails);

      await pool.query(
        `
              UPDATE report_jobs
              SET
                  status = 'failed',
                  error = ?,
                  updated_at = ?,
                  notification_status = 'unread'
              WHERE id = ?
              `,
        [errorMessage, getDateTime(), jobId],
      );

      return {
        code: "failed",
        message: "Validation failed. Please fix errors.",
        form_results: errorMessage,
      };
    }
    if (errorDetails.length > 0 && successCount > 0) {
      console.log(errorDetails);
      let message = `${successCount} records entered successfully.\n`;
      message += errorDetails.join("\n");

      await pool.query(
        `
              UPDATE report_jobs
              SET
                  status = 'failed',
                  error = ?,
                  updated_at = ?,
                  notification_status = 'unread'
              WHERE id = ?
              `,
        [message, getDateTime(), jobId],
      );

      return {
        code: "failed",
        message: "Validation failed. Please fix errors.",
        form_results: message,
      };
    }

    // Delete staging records if everything imported successfully
    if (errorDetails.length === 0 && allErrors.length === 0) {
      await pool.query(`DELETE FROM \`${staging_table}\` WHERE batch_id = ?`, [
        batchId,
      ]);
      await pool.query(
        "UPDATE report_jobs SET status='completed', notification_status='unread', updated_at = ? WHERE id=?",
        [getDateTime(), jobId],
      );
      console.log("rows imported successfully.");
      return {
        code: "success",
        message: `${successCount} rows imported successfully.`,
      };
    }
  } catch (err) {
    console.error("File import failed:", err);

    let errMessage = err.message || "File import failed";

    if (err.code === "ER_DUP_ENTRY") {
      const match = err.sqlMessage?.match(/Duplicate entry '(.+?)' for key/);

      const duplicateValue = match ? match[1] : "";

      errMessage = duplicateValue
        ? `Duplicate entry '${duplicateValue}'`
        : "Duplicate record found";
    }

    errorDetails.push(errMessage);

    // This is an import-level error, so don't use row_number
    await pool.query(
      `
    UPDATE \`${staging_table}\`
    SET
      import_status = 'failed',
      validation_error = ?
    WHERE batch_id = ?
    `,
      [errMessage, batchId],
    );

    return {
      code: "failed",
      message: errMessage,
      errorDetails,
    };
  }
};

async function importFileFromS3({
  file_url,
  fields,
  entity,
  batchId,
  sessionUser,
  staging_table,
  pool,
  errorDetails,
}) {
  const ExcelJS = require("exceljs");
  const csv = require("csv-parser");

  const url = new URL(file_url);

  const fileName = url.pathname.split("/").pop();
  const extension = fileName.split(".").pop().toLowerCase();

  const bucket = url.hostname.split(".")[0];
  const key = decodeURIComponent(url.pathname.slice(1));

  const response = await s3.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    }),
  );

  const rows = [];

  let rowNumber = 0;

  // -------------------------------------------------------
  // SAVE 500 ROWS AT ONCE INTO STAGING TABLE
  // -------------------------------------------------------
  async function saveBatch() {
    if (rows.length === 0) return;

    const columns = [
      "entity",
      "user",
      "batch_id",
      "row_number",
      ...fields.map((f) => {
        const fieldId = String(f).replace(/^c/, "");
        return `c${fieldId}`;
      }),
    ];

    const placeholders = rows
      .map(() => `(${columns.map(() => "?").join(",")})`)
      .join(",");
    const values = rows.flat();

    const sql = `
      INSERT INTO \`${staging_table}\`
      (
        ${columns.map((c) => `\`${c}\``).join(", ")}
      )
      VALUES ${placeholders}
    `;

    await pool.query(sql, values);

    // clear array
    rows.length = 0;
  }

  // -------------------------------------------------------
  // NORMALIZE CELL
  // -------------------------------------------------------
  function normalizeCell(cell) {
    if (cell === null || cell === undefined) {
      return "";
    }

    // Important: check Date BEFORE generic object processing
    if (cell instanceof Date && !isNaN(cell.getTime())) {
      return cell.toISOString();
    }

    if (typeof cell !== "object") {
      return String(cell).trim();
    }

    if (cell.text !== undefined) {
      return String(cell.text).trim();
    }

    if (cell.result !== undefined) {
      return String(cell.result).trim();
    }

    if (cell.hyperlink !== undefined) {
      return String(cell.hyperlink).trim();
    }

    if (cell.richText) {
      return cell.richText
        .map((x) => x.text)
        .join("")
        .trim();
    }

    return String(cell).trim();
  }

  // -------------------------------------------------------
  // PROCESS ONE FILE ROW
  // ONLY INSERT INTO STAGING
  // NO VALIDATION HERE
  // -------------------------------------------------------
  async function processRow(row) {
    rowNumber++;

    // Skip header
    if (rowNumber === 1) {
      return;
    }

    // Skip completely empty row
    const isEmpty = !row.some((value) => {
      const normalized = normalizeCell(value);
      return normalized !== "";
    });

    if (isEmpty) {
      return;
    }

    const values = [entity, sessionUser, batchId, rowNumber];

    // ---------------------------------------------------
    // ADD FIELD VALUES
    // ---------------------------------------------------
    for (let index = 0; index < fields.length; index++) {
      const value = normalizeCell(row[index]);

      values.push(value);
    }

    rows.push(values);

    // ---------------------------------------------------
    // INSERT EVERY 500 ROWS
    // ---------------------------------------------------
    if (rows.length >= 500) {
      await saveBatch();
    }
  }

  // -------------------------------------------------------
  // READ FILE
  // -------------------------------------------------------
  const stream = streamToNodeStream(response.Body);

  try {
    if (extension === "csv") {
      const parser = stream.pipe(
        csv({
          headers: false,
        }),
      );

      for await (const data of parser) {
        await processRow(Object.values(data));
      }
    } else if (extension === "xlsx") {
      const workbook = new ExcelJS.Workbook();

      await workbook.xlsx.read(stream);

      const sheet = workbook.worksheets[0];

      if (!sheet) {
        throw new Error("Excel file does not contain a worksheet.");
      }

      for (let rowIndex = 1; rowIndex <= sheet.rowCount; rowIndex++) {
        const row = sheet.getRow(rowIndex);

        await processRow(row.values.slice(1));
      }
    } else {
      throw new Error(`Unsupported file type: ${extension}`);
    }

    // Insert remaining rows
    await saveBatch();

    return {
      code: "success",
      message: "Imported into staging successfully",
    };
  } catch (err) {
    throw err;
  }
}

async function validateDateFields({
  staging_table,
  batchId,
  fields,
  pool,
  errorDetails,
}) {
  // 1. Get only date fields from metadata
  const fieldIds = fields.map((f) => String(f).replace(/^c/, ""));

  if (fieldIds.length === 0) {
    return;
  }

  const placeholders = fieldIds.map(() => "?").join(",");

  const [dateFields] = await pool.query(
    `
          SELECT
            ID,
            data_name,
            attributes
          FROM data_item
          WHERE ID IN (${placeholders})
            AND data_type = 'date'
        `,
    fieldIds,
  );

  if (dateFields.length === 0) {
    return;
  }

  // 2. Read staging rows once
  const dateColumns = dateFields.map((field) => `c${field.ID}`);

  const selectColumns = ["row_number", ...dateColumns]
    .map((col) => `\`${col}\``)
    .join(", ");

  const sql = `
  SELECT ${selectColumns}
  FROM \`${staging_table}\`
  WHERE \`batch_id\` = ?
`;

  const [rows] = await pool.query(sql, [batchId]);

  // 3. Validate every date value
  for (const row of rows) {
    for (const field of dateFields) {
      const column = `c${field.ID}`;
      const value = row[column];

      // Required check
      const isRequired = hasAttribute(field.attributes, "required");

      // Empty value
      if (
        value === null ||
        value === undefined ||
        String(value).trim() === ""
      ) {
        if (isRequired) {
          const message = `${field.data_name} is required`;

          errorDetails.push(`Line ${row.row_number}: ${message}`);

          await markValidationError({
            staging_table,
            batchId,
            rowNumber: row.row_number,
            message,
            pool,
          });
        }

        continue;
      }

      // Date validation
      const result = dateValidation(value);

      if (!result.status) {
        const message = `${field.data_name}: ${result.message}`;

        errorDetails.push(`Line ${row.row_number}: ${message}`);

        await markValidationError({
          staging_table,
          batchId,
          rowNumber: row.row_number,
          message,
          pool,
        });

        continue;
      }

      // 4. Update normalized date value
      if (result.value !== value) {
        await pool.query(
          `
            UPDATE \`${staging_table}\`
            SET \`${column}\` = ?
            WHERE \`batch_id\` = ?
              AND \`row_number\` = ?
            `,
          [result.value, batchId, row.row_number],
        );
      }
    }
  }
}

function hasAttribute(attributes, attributeName) {
  if (!attributes) return false;

  return String(attributes)
    .split(/[\s,]+/)
    .map((item) => item.trim().toLowerCase())
    .includes(attributeName.toLowerCase());
}

function dateValidation(value) {
  let date = null;

  if (value === null || value === undefined || String(value).trim() === "") {
    return {
      status: true,
      value: "",
    };
  }

  // ExcelJS Date object
  if (value instanceof Date && !isNaN(value.getTime())) {
    date = value;
  } else {
    let val = String(value).trim();

    // ISO datetime
    // 1999-07-27T00:00:00.000Z
    if (/^\d{4}-\d{2}-\d{2}T/.test(val)) {
      val = val.substring(0, 10);
    }

    // Excel serial
    if (/^\d+(\.\d+)?$/.test(val)) {
      const serial = Number(val);

      if (serial > 0 && serial <= 2958465) {
        const excelEpoch = Date.UTC(1899, 11, 30);

        const excelDate = new Date(excelEpoch + serial * 86400000);

        if (!isNaN(excelDate.getTime())) {
          date = new Date(
            excelDate.getUTCFullYear(),
            excelDate.getUTCMonth(),
            excelDate.getUTCDate(),
          );
        }
      }
    }

    // YYYY-MM-DD
    if (!date) {
      let match = val.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);

      if (match) {
        date = createValidDate(
          Number(match[1]),
          Number(match[2]),
          Number(match[3]),
        );
      }
    }

    // YYYY/M/D
    if (!date) {
      let match = val.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})$/);

      if (match) {
        date = createValidDate(
          Number(match[1]),
          Number(match[2]),
          Number(match[3]),
        );
      }
    }

    // M/D/YYYY or M-D-YYYY
    if (!date) {
      let match = val.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/);

      if (match) {
        date = createValidDate(
          Number(match[3]),
          Number(match[1]),
          Number(match[2]),
        );
      }
    }
  }

  if (!date || isNaN(date.getTime())) {
    return {
      status: false,
      message: `Invalid date '${value}'`,
    };
  }

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");

  return {
    status: true,
    value: `${year}-${month}-${day}`,
  };
}

function createValidDate(year, month, day) {
  const date = new Date(year, month - 1, day);

  if (
    date.getFullYear() !== year ||
    date.getMonth() !== month - 1 ||
    date.getDate() !== day
  ) {
    return null;
  }

  return date;
}

async function markValidationError({
  staging_table,
  batchId,
  rowNumber,
  message,
  pool,
}) {
  await pool.query(
    `
    UPDATE \`${staging_table}\`
    SET
      \`import_status\` = 'failed',
      \`validation_error\` =
        CASE
          WHEN \`validation_error\` IS NULL
            OR \`validation_error\` = ''
          THEN ?
          ELSE CONCAT(\`validation_error\`, '; ', ?)
        END
    WHERE \`batch_id\` = ?
      AND \`row_number\` = ?
    `,
    [message, message, batchId, rowNumber],
  );
}
// function parseValidation(validation) {
//   const rules = {};
//   if (!validation) return rules;
//   validation.split("^").forEach((item) => {
//     const parts = item.split("$", 3);

//     if (parts.length === 3) {
//       rules[`c${parts[0].trim()}`] = {
//         rule: parts[1],
//         message: parts[2],
//       };
//     }
//   });
//   return rules;
// }
function parseValidation(validation) {
  const rules = {};

  if (!validation) return rules;

  validation.split("^").forEach((item) => {
    const parts = item.split("$", 3);

    if (parts.length !== 3) return;

    const fieldIds = parts[0]
      .split(",")
      .map((field) => field.trim())
      .filter(Boolean);

    const rule = parts[1].trim();
    const message = parts[2].trim();

    fieldIds.forEach((fieldId) => {
      const column = `c${fieldId}`;

      if (!rules[column]) {
        rules[column] = [];
      }

      rules[column].push({
        rule,
        message,
      });
    });
  });

  return rules;
}
async function applyStagingRules({
  staging_table,
  batchId,
  pool,
  validationRules,
  errorDetails,
}) {
  for (const [column, configs] of Object.entries(validationRules)) {
    for (const config of configs) {
      const { rule, message } = config;
      switch (rule) {
        // ----------------------------------
        // UPPER CASE
        // ----------------------------------
        case "upperCase":
          await pool.query(
            `
            UPDATE \`${staging_table}\`
            SET \`${column}\` = UPPER(\`${column}\`)
            WHERE batch_id = ?
              AND \`${column}\` IS NOT NULL
              AND TRIM(\`${column}\`) != ''
          `,
            [batchId],
          );

          break;

        // ----------------------------------
        // LOWER CASE
        // ----------------------------------
        case "lowerCase":
          await pool.query(
            `
            UPDATE \`${staging_table}\`
            SET \`${column}\` = LOWER(\`${column}\`)
            WHERE batch_id = ?
              AND \`${column}\` IS NOT NULL
              AND TRIM(\`${column}\`) != ''
          `,
            [batchId],
          );

          break;
        // ----------------------------------
        // DATE VALIDATION
        // ----------------------------------
        case "dateValidation": {
          const [dateRows] = await pool.query(
            `
      SELECT
        \`row_number\`,
        \`${column}\` AS value
      FROM \`${staging_table}\`
      WHERE \`batch_id\` = ?
        AND \`${column}\` IS NOT NULL
        AND TRIM(\`${column}\`) != ''
    `,
            [batchId],
          );

          for (const row of dateRows) {
            const result = dateValidation(row.value);

            // Invalid date
            if (!result.status) {
              const errorMessage = `${message} (${row.value})`;

              errorDetails.push(`Line ${row.row_number}: ${errorMessage}`);

              await markValidationError({
                staging_table,
                batchId,
                rowNumber: row.row_number,
                message: errorMessage,
                pool,
              });

              continue;
            }

            // Convert valid date to YYYY-MM-DD
            if (result.value !== row.value) {
              await pool.query(
                `
          UPDATE \`${staging_table}\`
          SET \`${column}\` = ?
          WHERE \`batch_id\` = ?
            AND \`row_number\` = ?
        `,
                [result.value, batchId, row.row_number],
              );
            }
          }

          break;
        }

        // ----------------------------------
        // EMAIL VALIDATION
        // ----------------------------------
        case "emailValidation": {
          const [invalidRows] = await pool.query(
            `
            SELECT
              \`row_number\`,
              \`${column}\` AS value
            FROM \`${staging_table}\`
            WHERE \`batch_id\` = ?
              AND \`${column}\` IS NOT NULL
              AND TRIM(\`${column}\`) != ''
              AND TRIM(\`${column}\`)
                  NOT REGEXP '^[^[:space:]@]+@[^[:space:]@]+\\.[^[:space:]@]+$'
          `,
            [batchId],
          );

          for (const row of invalidRows) {
            const errorMessage = `${message} (${row.value})`;

            errorDetails.push(`Line ${row.row_number}: ${errorMessage}`);

            await markValidationError({
              staging_table,
              batchId,
              rowNumber: row.row_number,
              message: errorMessage,
              pool,
            });
          }

          break;
        }
        case "required": {
          const [invalidRows] = await pool.query(
            `
            SELECT
              \`row_number\`,
              \`${column}\` AS value
            FROM \`${staging_table}\`
            WHERE \`batch_id\` = ?
              AND (
                \`${column}\` IS NULL
                OR TRIM(\`${column}\`) = ''
              )
          `,
            [batchId],
          );

          for (const row of invalidRows) {
            const errorMessage = `${message}`;

            errorDetails.push(
              `Line ${row.row_number}:${column} is ${errorMessage}`,
            );

            await markValidationError({
              staging_table,
              batchId,
              rowNumber: row.row_number,
              message: errorMessage,
              pool,
            });
          }

          break;
        }

        // ----------------------------------
        // PHONE VALIDATION
        // ----------------------------------
        case "phoneValidation": {
          // Normalize phone number
          await pool.query(
            `
            UPDATE \`${staging_table}\`
            SET \`${column}\` =
              REPLACE(
                REPLACE(
                  REPLACE(
                    REPLACE(\`${column}\`, ' ', ''),
                    '-', ''
                  ),
                  '(', ''
                ),
                ')', ''
              )
            WHERE \`batch_id\` = ?
              AND \`${column}\` IS NOT NULL
          `,
            [batchId],
          );

          // Find invalid phone numbers
          const [invalidRows] = await pool.query(
            `
            SELECT
              \`row_number\`,
              \`${column}\` AS value
            FROM \`${staging_table}\`
            WHERE \`batch_id\` = ?
              AND \`${column}\` IS NOT NULL
              AND TRIM(\`${column}\`) != ''
              AND TRIM(\`${column}\`)
                  NOT REGEXP '^0[0-9]{9}$'
          `,
            [batchId],
          );

          for (const row of invalidRows) {
            const errorMessage = `${message} (${row.value})`;

            errorDetails.push(`Line ${row.row_number}: ${errorMessage}`);

            await markValidationError({
              staging_table,
              batchId,
              rowNumber: row.row_number,
              message: errorMessage,
              pool,
            });
          }

          break;
        }

        // ----------------------------------
        // OTHER RULES
        // exact / max / min / replace
        // ----------------------------------
        default: {
          // ==================================
          // EXACT / MAX / MIN
          //
          // exact(int)(15)
          // max(int)(15)
          // min(int)(5)
          //
          // exact(char)(10)
          // max(char)(20)
          // min(char)(3)
          //
          // exact(both)(15)
          // max(both)(15)
          // min(both)(5)
          // ==================================

          const lengthRegex = /^(exact|max|min)\((int|char|both)\)\((\d+)\)$/;

          const lengthMatch = rule.match(lengthRegex);

          if (lengthMatch) {
            const checkType = lengthMatch[1];
            const valueType = lengthMatch[2];
            const limit = Number(lengthMatch[3]);

            let condition = "";

            // -------------------------------
            // INT
            // Digits only
            // -------------------------------
            if (valueType === "int") {
              if (checkType === "exact") {
                condition = `
                (
                  TRIM(\`${column}\`) NOT REGEXP '^[0-9]+$'
                  OR CHAR_LENGTH(TRIM(\`${column}\`)) != ?
                )
              `;
              }

              if (checkType === "max") {
                condition = `
                (
                  TRIM(\`${column}\`) NOT REGEXP '^[0-9]+$'
                  OR CHAR_LENGTH(TRIM(\`${column}\`)) > ?
                )
              `;
              }

              if (checkType === "min") {
                condition = `
                (
                  TRIM(\`${column}\`) NOT REGEXP '^[0-9]+$'
                  OR CHAR_LENGTH(TRIM(\`${column}\`)) < ?
                )
              `;
              }
            }

            // -------------------------------
            // CHAR
            // Character length
            // -------------------------------
            if (valueType === "char") {
              if (checkType === "exact") {
                condition = `
                CHAR_LENGTH(TRIM(\`${column}\`)) != ?
              `;
              }

              if (checkType === "max") {
                condition = `
                CHAR_LENGTH(TRIM(\`${column}\`)) > ?
              `;
              }

              if (checkType === "min") {
                condition = `
                CHAR_LENGTH(TRIM(\`${column}\`)) < ?
              `;
              }
            }

            // -------------------------------
            // BOTH
            // Letters + numbers / any value
            // -------------------------------
            if (valueType === "both") {
              if (checkType === "exact") {
                condition = `
                CHAR_LENGTH(TRIM(\`${column}\`)) != ?
              `;
              }

              if (checkType === "max") {
                condition = `
                CHAR_LENGTH(TRIM(\`${column}\`)) > ?
              `;
              }

              if (checkType === "min") {
                condition = `
                CHAR_LENGTH(TRIM(\`${column}\`)) < ?
              `;
              }
            }

            const [invalidRows] = await pool.query(
              `
              SELECT
                \`row_number\`,
                \`${column}\` AS value
              FROM \`${staging_table}\`
              WHERE \`batch_id\` = ?
                AND \`${column}\` IS NOT NULL
                AND TRIM(\`${column}\`) != ''
                AND ${condition}
            `,
              [batchId, limit],
            );

            for (const row of invalidRows) {
              const errorMessage = `${message} (${row.value})`;

              errorDetails.push(`Line ${row.row_number}: ${errorMessage}`);

              await markValidationError({
                staging_table,
                batchId,
                rowNumber: row.row_number,
                message: errorMessage,
                pool,
              });
            }

            break;
          }

          // ==================================
          // REPLACE
          //
          // replace(,)('')
          // replace(LKR)(USD)
          // replace($)('')
          // ==================================
          if (rule.startsWith("replace(")) {
            const replaceRegex = /^replace\((.*?)\)\((.*?)\)$/;

            const match = rule.match(replaceRegex);

            if (!match) {
              throw new Error(`Invalid replace rule '${rule}' for ${column}`);
            }

            const find = match[1];
            let replace = match[2];

            // Remove quote wrappers
            // '' -> empty string
            // "" -> empty string
            // 'USD' -> USD
            if (
              (replace.startsWith("'") && replace.endsWith("'")) ||
              (replace.startsWith('"') && replace.endsWith('"'))
            ) {
              replace = replace.substring(1, replace.length - 1);
            }

            await pool.query(
              `
              UPDATE \`${staging_table}\`
              SET \`${column}\` =
                REPLACE(\`${column}\`, ?, ?)
              WHERE batch_id = ?
                AND \`${column}\` IS NOT NULL
            `,
              [find, replace, batchId],
            );

            break;
          }

          // Unknown validation rule
          console.warn(
            `Unknown validation rule '${rule}' for column '${column}'`,
          );

          break;
        }
      }
    }
  }
}
async function validateOptionsFields({
  staging_table,
  fields,
  entity,
  batchId,
  pool,
  errorDetails,
}) {
  const optionsInfo = await getOptionsInfo(fields, entity, pool);

  for (const fld in optionsInfo) {
    const info = optionsInfo[fld];
    const parentTable = `t${info.form}`;
    const parentCol = `c${info.field}`;
    const childCol = `c${fld}`;

    const joinConditions = [];

    // child value = parent value
    joinConditions.push(`s.\`${childCol}\` = p.\`${parentCol}\``);

    // parent entity
    // joinConditions.push(`p.entity = ?`);

    // const params = [info.entity];
    const params = [];

    // Additional conditions
    for (const cond of info.conditions || []) {
      const parentConditionCol = `c${cond.parent_col}`;
      if (cond.type === "constant") {
        joinConditions.push(`p.\`${parentConditionCol}\` = ?`);
        params.push(cond.value);
      }

      if (cond.type === "child_field") {
        const childConditionCol = `c${cond.value}`;

        joinConditions.push(
          `p.\`${parentConditionCol}\` = s.\`${childConditionCol}\``,
        );
      }
    }

    // Select ONLY invalid staging rows
    const sql = `
      SELECT
        s.\`row_number\`,
        s.\`${childCol}\` AS value
      FROM \`${staging_table}\` s

      LEFT JOIN \`${parentTable}\` p
        ON ${joinConditions.join(" AND ")}

      WHERE s.\`batch_id\` = ?
        AND s.\`${childCol}\` IS NOT NULL
        AND TRIM(s.\`${childCol}\`) != ''
        AND p.\`${parentCol}\` IS NULL

      ORDER BY s.\`row_number\`
    `;
    const [invalidRows] = await pool.query(sql, [...params, batchId]);

    for (const row of invalidRows) {
      const message = `Invalid value '${row.value}' for ${info.toField}`;

      errorDetails.push(`Line ${row.row_number}: ${message}`);
    }
  }
}

async function getOptionsInfo(fieldsArr, entity, pool) {
  const optionsInfo = {};

  for (const fld of fieldsArr) {
    const fieldInfo = await getFieldInfo(fld, pool);

    let optionFrom = {};

    if (fieldInfo?.data_type === "options") {
      const formInfo = await getFieldInfo(fieldInfo.options_from, pool);

      optionFrom.form = formInfo?.form_ID;
      optionFrom.fromFormName = await getFormName(optionFrom.form, pool);
      optionFrom.field = fieldInfo.options_from;
      optionFrom.entity = await getEntity(optionFrom.form, pool);
      optionFrom.conditions = [];
      optionFrom.toField = fieldInfo.data_name;

      optionsInfo[fld] = optionFrom;
    } else if (
      fieldInfo?.data_type === "options_search" ||
      fieldInfo?.data_type === "large_search"
    ) {
      const cal = fieldInfo.calculation || "";
      const parts = cal.split("$");

      const formId = parseInt(parts[0], 10);

      optionFrom.form = formId;
      optionFrom.fromFormName = await getFormName(formId, pool);

      const firstField = (parts[1] || "").split(",")[0];
      optionFrom.field = firstField;

      optionFrom.entity = await getEntity(formId, pool);

      optionFrom.conditions = parseOptionConditions(parts[4] || "");
      optionFrom.toField = fieldInfo.data_name;

      optionsInfo[fld] = optionFrom;
    }
  }

  return optionsInfo;
}
function parseOptionConditions(condStr) {
  if (!condStr) return [];
  const conditions = [];

  const pairs = condStr.split(",");

  for (const p of pairs) {
    if (!p.includes("=")) continue;

    const [parentColRaw, rightRaw] = p.split("=");

    const parentCol = parseInt(parentColRaw, 10);

    if (/\{(\d+)\}/.test(rightRaw)) {
      // Right side is a CHILD FIELD like {12}
      const match = rightRaw.match(/\{(\d+)\}/);

      conditions.push({
        parent_col: parentCol,
        type: "child_field",
        value: parseInt(match[1], 10),
      });
    } else {
      // Right side is a CONSTANT
      conditions.push({
        parent_col: parentCol,
        type: "constant",
        value: rightRaw,
      });
    }
  }

  return conditions;
}
