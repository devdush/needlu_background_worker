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

const errorDetails = [];
async function importCSVFromS3({
    file_url,
    fields,
    entity,
    batchId,
    sessionUser,
    staging_table,
    pool,
    validation
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
            Key: key
        })
    );


    const validationRules =
        parseValidation(validation);



    const rows = [];

    let rowNumber = 0;



    async function saveBatch() {

        if (rows.length === 0)
            return;


        const columns = [
            "entity",
            "user",
            "batch_id",
            "row_number",
            ...fields.map(f =>
                f.startsWith("c") ? f : `c${f}`
            )
        ];


        const placeholders =
            rows.map(() =>
                `(${columns.map(() => "?").join(",")})`
            ).join(",");


        const values =
            rows.flat();


        const sql = `
      INSERT INTO \`${staging_table}\`
      (${columns.map(c => ` \`${c}\``).join(",")})
      VALUES ${placeholders}
    `;


        await pool.query(sql, values);


        rows.length = 0;

    }

    function parseValidation(validation) {

        const rules = {};


        if (!validation)
            return rules;


        validation.split("^")
            .forEach(item => {

                const parts =
                    item.split("$", 3);


                if (parts.length === 3) {

                    rules[`c${parts[0].trim()}`] = {
                        rule: parts[1],
                        message: parts[2]
                    };

                }

            });


        return rules;

    }

    function normalizeCell(cell) {

        if (cell === null || cell === undefined)
            return "";
        if (typeof cell !== "object")
            return String(cell).trim();
        if (cell.text)
            return String(cell.text).trim();
        if (cell.result)
            return String(cell.result).trim();
        if (cell.hyperlink)
            return String(cell.hyperlink).trim();
        if (cell.richText)
            return cell.richText
                .map(x => x.text)
                .join("")
                .trim();
        if (cell instanceof Date)
            return cell.toISOString();
        return String(cell).trim();

    }

    async function processRow(row) {
        rowNumber++;
        // skip header
        if (rowNumber === 1)
            return;
        if (!row.some(v =>
            String(v ?? "").trim()
        ))
            return;
        const values = [
            entity,
            sessionUser,
            batchId,
            rowNumber
        ];

        let rowHasError = false;
        fields.forEach((field, index) => {
            const column =
                field.startsWith("c")
                    ? field
                    : `c${field}`;

            let value =
                normalizeCell(row[index]);

            if (validationRules[column]) {
                const result =
                    applyRule(
                        validationRules[column].rule,
                        value,
                        validationRules[column].message
                    );
                if (!result.status) {
                    errorDetails.push(
                        `Line ${rowNumber}: ${column} - ${result.message}`
                    );
                    rowHasError = true;
                    return;
                }
                value = result.value;

            }
            values.push(value);

        });
        if (rowHasError) {
            return;
        }
        rows.push(values);
        if (rows.length >= 500) {
            await saveBatch();
        }
    }



    const stream =
        streamToNodeStream(response.Body);
    try {
        if (extension === "csv") {
            const parser =
                stream.pipe(
                    csv({ headers: false })
                );
            for await (const data of parser) {

                await processRow(
                    Object.values(data)
                );
            }
        }
        else if (
            extension === "xlsx" ||
            extension === "xls"
        ) {
            const workbook =
                new ExcelJS.Workbook();
            await workbook.xlsx.read(stream);
            const sheet =
                workbook.worksheets[0];
            sheet.eachRow(row => {

                processRow(
                    row.values.slice(1)
                );
            });
        }
        else {
            throw new Error(
                "Unsupported file type"
            );
        }

        await saveBatch();
        return {
            code: "success",
            message: "Imported successfully"
        };
    }
    catch (err) {
        return {
            code: "failed",
            message: err.message
        };
    }
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
async function formSubmit() { }
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
    const validation = params.validation;

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

    const allErrors = [];

    try {
        await importCSVFromS3({
            file_url,
            fields,
            entity,
            batchId,
            sessionUser,
            staging_table,
            pool,
            validation
        });
    } catch (err) {
        const errorMessage = err.message;

        // Delete staging data for this batch
        await pool.query(
            `DELETE FROM \`${staging_table}\` WHERE batch_id = ?`,
            [batchId]
        );


        // Update job error
        await pool.query(
            `
          UPDATE report_jobs 
          SET 
            status='failed',
            error=?,
            notification_status='unread'
          WHERE id=?
          `,
            [
                errorMessage,
                jobId
            ]
        );


        return {
            code: "failed",
            message: "Validation failed. Please fix errors.",
            form_results: errorMessage
        };
    }
    const optionsInfo = await getOptionsInfo(fields, entity, pool);

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
            [
                allErrors.join("\n"),
                jobId
            ],
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
            [
                errorDetails.join("\n"),
                jobId
            ],
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

function applyRule(rule, value, message) {
    switch (rule) {
        case "upperCase":
            return {
                status: true,
                value: String(value).toUpperCase()
            };

        case "lowerCase":
            return {
                status: true,
                value: String(value).toLowerCase()
            };

        case "emailValidation":
            const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

            if (!emailRegex.test(String(value))) {
                return {
                    status: false,
                    message
                };
            }

            return {
                status: true,
                value
            };

        case "phoneValidation":
            const phone = String(value).replace(/[\s\-()]/g, "");

            if (!/^0\d{9}$/.test(phone)) {
                return {
                    status: false,
                    message
                };
            }

            return {
                status: true,
                value: phone
            };
        case "dateValidation": {
            let val = String(value).trim();

            // Convert ISO datetime to YYYY-MM-DD
            if (/^\d{4}-\d{2}-\d{2}T/.test(val)) {
                val = val.split("T")[0];
            }

            let date = null;

            // YYYY-MM-DD
            if (/^\d{4}-\d{2}-\d{2}$/.test(val)) {
                date = new Date(val);
            }
            // YYYY/MM/DD
            else if (/^\d{4}\/\d{2}\/\d{2}$/.test(val)) {
                const [y, m, d] = val.split("/");
                date = new Date(`${y}-${m}-${d}`);
            }
            // MM/DD/YYYY or DD/MM/YYYY
            else if (/^\d{2}\/\d{2}\/\d{4}$/.test(val)) {
                const [a, b, c] = val.split("/").map(Number);

                if (a > 12) {
                    date = new Date(c, b - 1, a);
                } else {
                    date = new Date(c, a - 1, b);
                }
            }

            if (date && !isNaN(date.getTime())) {
                return {
                    status: true,
                    value: val
                };
            }

            return {
                status: false,
                message
            };
        }

        default:
            return {
                status: true,
                value
            };
    }
}