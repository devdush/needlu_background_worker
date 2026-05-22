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
async function getFieldInfo(fieldId, pool) {
  const [rows] = await pool.query("SELECT * FROM data_item WHERE ID = ?", [
    fieldId,
  ]);
  return rows[0];
}
// helper: convert S3 stream → Node stream
const streamToNodeStream = (body) => {
  const { Readable } = require("stream");
  return Readable.from(body);
};

async function importCSVFromS3({
  file_url,
  fields,
  entity,
  batchId,
  sessionUser,
  staging_table,
  pool,
}) {
  const { Readable } = require("stream");
  // parse bucket + key from S3 URL
  const url = new URL(file_url);

  const bucket = url.hostname.split(".")[0]; // simple case
  const key = decodeURIComponent(url.pathname.slice(1));

  const command = new GetObjectCommand({
    Bucket: bucket,
    Key: key,
  });

  const response = await s3.send(command);

  const stream = streamToNodeStream(response.Body);

  let line = 0;

  return new Promise((resolve, reject) => {
    stream
      .pipe(csv({ headers: false }))
      .on("data", async (data) => {
        stream.pause();

        try {
          line++;

          if (line === 1) {
            stream.resume();
            return;
          }

          const row = Object.values(data);

          const hasData = row.some(
            (v) => v !== null && v !== undefined && String(v).trim() !== "",
          );

          if (!hasData) {
            stream.resume();
            return;
          }

          const colNames = ["entity", "user", "batch_id", "row_number"];
          const colValues = [entity, sessionUser, batchId, line];

          fields.forEach((fld, index) => {
            const columnName = fld.startsWith("c") ? fld : `c${fld}`;
            colNames.push(columnName);
            colValues.push((row[index] || "").trim());
          });

          const sql = `
                        INSERT INTO \`${staging_table}\`
                        (${colNames.map((c) => `\`${c}\``).join(", ")})
                        VALUES (${colValues.map(() => "?").join(", ")})
                    `;

          const [result] = await pool.query(sql, colValues);

          stream.resume();
        } catch (err) {
          stream.destroy();
          reject({
            code: "failed",
            message: err.message,
          });
        }
      })
      .on("end", () =>
        resolve({
          code: "success",
          message: "Imported successfully",
        }),
      )
      .on("error", reject);
  });
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
async function formSubmit() {}
module.exports = async function handleCSVUpload(jobId, pool) {
  //console.log(jobId);
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
  const batch_id = params.batch_id;
  const sessionUser = params.user_id;
  const batchId = params.batch_id;
  const result = await tableExists(staging_table, pool);
  //console.log(`Table ${staging_table} exists:`, result);
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

  await importCSVFromS3({
    file_url,
    fields,
    entity,
    batchId,
    sessionUser,
    staging_table,
    pool,
  });
  const optionsInfo = await getOptionsInfo(fields, entity, pool);
  const allErrors = [];
  for (const fld in optionsInfo) {
    const info = optionsInfo[fld];

    const parentTable = `t${info.form}`;
    const parentCol = `c${info.field}`;
    const childCol = `c${fld}`;

    const joinConditions = [];

    joinConditions.push(`s.\`${childCol}\` = p.\`${parentCol}\``);

    joinConditions.push(`p.entity = ?`);
    const params = [info.entity];

    for (const cond of info.conditions || []) {
      const pColName = `c${cond.parent_col}`;

      if (cond.type === "constant") {
        joinConditions.push(`p.\`${pColName}\` = ?`);
        params.push(cond.value);
      }

      if (cond.type === "child_field") {
        const otherCol = `c${cond.value}`;
        joinConditions.push(`p.\`${pColName}\` = s.\`${otherCol}\``);
      }
    }

    const sql = `
    SELECT s.row_number, s.\`${childCol}\`
            FROM \`${staging_table}\` s
            LEFT JOIN \`${parentTable}\` p
            ON ${joinConditions.join(" AND ")}
            WHERE s.batch_id = ?
            AND s.\`${childCol}\` IS NOT NULL
            AND s.\`${childCol}\` != ''
            AND p.\`${parentCol}\` IS NULL
            `;
    const [rows] = await pool.query(sql, [...params, batchId]);

    for (const r of rows) {
      allErrors.push(
        `Line ${r.row_number}: Invalid value '${r[childCol]}' for ${info.toField}`,
      );
    }
  }

  // ---------------------------
  // 6. STOP IF ERRORS
  // ---------------------------
  if (allErrors.length > 0) {
    await pool.query(`DELETE FROM \`${staging_table}\` WHERE batch_id = ?`, [
      batchId,
    ]);

    await pool.query(
      "UPDATE report_jobs SET status='failed',error=?, notification_status='unread' WHERE id=?",
      [allErrors, jobId],
    );
    return {
      code: "failed",
      message: "Validation failed. Please fix errors.",
      form_results: allErrors.join("\n"),
    };
  }
  // ---------------------------
  // 7. TRANSFER DATA TO MAIN TABLE
  // ---------------------------

  const mainTable = `t${form}`;

  let successCount = 0;
  const errorDetails = [];

  // Get all rows from staging table for this batch
  const [stagingRows] = await pool.query(
    `SELECT * FROM \`${staging_table}\` WHERE batch_id = ?`,
    [batchId],
  );

  for (const row of stagingRows) {
    try {
      const insertCols = ["entity", "user"];
      const insertVals = [row.entity, row.user];

      // Dynamic columns from CSV fields
      fields.forEach((fld) => {
        const colName = `c${fld}`;

        insertCols.push(colName);
        insertVals.push(row[colName]);
      });

      // Build dynamic insert query
      const sql = `
      INSERT INTO \`${mainTable}\`
      (${insertCols.map((c) => `\`${c}\``).join(", ")})
      VALUES (${insertVals.map(() => "?").join(", ")})
    `;

      await pool.query(sql, insertVals);

      // Mark staging row as completed
      await pool.query(
        `
      UPDATE \`${staging_table}\`
      SET import_status = 'completed'
      WHERE batch_id = ? AND row_number = ?
      `,
        [batchId, row.row_number],
      );

      successCount++;
    } catch (err) {
      console.error(err);

      let errMessage = err.message;

      // Duplicate entry error
      if (err.code === "ER_DUP_ENTRY") {
        errMessage = "Duplicate record found";
      }

      errorDetails.push(`Line ${row.row_number}: Failed. (${errMessage})`);

      // Mark staging row as failed
      await pool.query(
        `
      UPDATE \`${staging_table}\`
      SET
        import_status = 'failed',
        validation_error = ?
      WHERE batch_id = ? AND row_number = ?
      `,
        [errMessage, batchId, row.row_number],
      );
    }
  }

  // ---------------------------
  // 8. FINAL RESPONSE
  // ---------------------------

  if (errorDetails.length > 0) {
    await pool.query(
      "UPDATE report_jobs SET status='failed',error=?, notification_status='unread' WHERE id=?",
      [errorDetails, jobId],
    );
    return {
      code: "partial_success",
      message: `${successCount} rows imported, ${errorDetails.length} failed.`,
      errors: errorDetails,
    };
  }

  // Delete staging records if everything imported successfully
  if (errorDetails.length === 0 && allErrors.length === 0) {
    await pool.query(`DELETE FROM \`${staging_table}\` WHERE batch_id = ?`, [
      batchId,
    ]);
    await pool.query("UPDATE report_jobs SET status='completed', notification_status='unread' WHERE id=?", [
      jobId,
    ]);

    return {
      code: "success",
      message: `${successCount} rows imported successfully.`,
    };
  }
};
