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
    validation,
    errorDetails
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
                if (validationRules[column] && 
                    value !== null && 
                    value !== undefined && 
                    value !== '') {
                    const result = applyRule(
                        validationRules[column].rule,
                        value,
                        validationRules[column].message
                    );
                
                    if (!result.status) {
                        errorDetails.push(
                            `Line ${rowNumber}: ${column} - ${result.message}`
                        );
                    
                        rowHasError = true;
                    
                        console.log("---------------In error validation------------------");
                        console.log(errorDetails, column, value);
                    
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
    const errorDetails = [];
    const allErrors = [];
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

    

    try {
        await importCSVFromS3({
            file_url,
            fields,
            entity,
            batchId,
            sessionUser,
            staging_table,
            pool,
            validation,
            errorDetails
        });
    } catch (err) {

    console.error(err);

    let errMessage = err.message;

    // Duplicate entry error
    if (err.code === "ER_DUP_ENTRY") {

        const match = err.sqlMessage?.match(/Duplicate entry '(.+?)' for key/);

        const duplicateValue = match ? match[1] : "";

        errMessage = duplicateValue
            ? `Duplicate entry '${duplicateValue}'`
            : "Duplicate record found";
    }

    errorDetails.push(
        `Line ${row.row_number}: ${errMessage}`
    );

    // Mark staging row as failed
    await pool.query(
        `
        UPDATE \`${staging_table}\`
        SET
            \`import_status\` = 'failed',
            \`validation_error\` = ?
        WHERE \`batch_id\` = ?
          AND \`row_number\` = ?
        `,
        [errMessage, batchId, row.row_number]
    );
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
    // ---------------------------------------------------------
    // 1. Get key_member fields
    // ---------------------------------------------------------
    const [keyFieldRows] = await pool.query(
        `
        SELECT ID AS field_id
        FROM erp.data_item
        WHERE form_ID = ?
          AND key_member = 1
        ORDER BY sorting_value
        `,
        [form]
    );

    const keyColumns = keyFieldRows.map(
        item => `c${item.field_id}`
    );



    // ---------------------------------------------------------
    // 2. FIRST LOOP - CHECK ALL DUPLICATES
    // ---------------------------------------------------------

    let hasDuplicate = false;

    for (const row of stagingRows) {

        try {

            if (keyColumns.length === 0) {
                continue;
            }

            const duplicateConditions = [];
            const duplicateValues = [];


            keyColumns.forEach((keyCol) => {

                const keyValue = row[keyCol];

                if (
                    keyValue !== null &&
                    keyValue !== undefined &&
                    String(keyValue).trim() !== ""
                ) {
                    duplicateConditions.push(
                        `\`${keyCol}\` = ?`
                    );

                    duplicateValues.push(keyValue);
                }
            });


            if (duplicateConditions.length === 0) {
                continue;
            }


            const duplicateSql = `
                SELECT *
                FROM \`${mainTable}\`
                WHERE ${duplicateConditions.join(" OR ")}
                LIMIT 1
            `;


            const [duplicateRows] = await pool.query(
                duplicateSql,
                duplicateValues
            );


            // Duplicate found
            if (duplicateRows.length > 0) {

                hasDuplicate = true;

                const existingRow = duplicateRows[0];

                const duplicateFields = [];

                for (const keyCol of keyColumns) {

                    if (
                        row[keyCol] !== null &&
                        row[keyCol] !== undefined &&
                        existingRow[keyCol] == row[keyCol]
                    ) {
                        duplicateFields.push(
                            `${keyCol}: ${row[keyCol]}`
                        );
                    }
                }


                const errMessage =
                    `Duplicate entry (${duplicateFields.join(", ")})`;


                errorDetails.push(
                    `Line ${row.row_number}: ${errMessage}`
                );


                // Mark duplicate row as failed
                await pool.query(
                    `
                    UPDATE \`${staging_table}\`
                    SET
                        \`import_status\` = 'failed',
                        \`validation_error\` = ?
                    WHERE \`batch_id\` = ?
                      AND \`row_number\` = ?
                    `,
                    [
                        errMessage,
                        batchId,
                        row.row_number
                    ]
                );
            }

        } catch (err) {

            console.error(err);

            hasDuplicate = true;

            const errMessage = err.message;

            errorDetails.push(
                `Line ${row.row_number}: ${errMessage}`
            );


            await pool.query(
                `
                UPDATE \`${staging_table}\`
                SET
                    \`import_status\` = 'failed',
                    \`validation_error\` = ?
                WHERE \`batch_id\` = ?
                  AND \`row_number\` = ?
                `,
                [
                    errMessage,
                    batchId,
                    row.row_number
                ]
            );
        }
    }


    // ---------------------------------------------------------
    // 3. IF ANY DUPLICATE EXISTS -> DO NOT INSERT ANYTHING
    // ---------------------------------------------------------

    if (hasDuplicate) {

        console.log("Duplicate found. Import cancelled.");

        // Optional:
        // mark remaining pending rows as not imported

        await pool.query(
            `
            UPDATE \`${staging_table}\`
            SET
                \`import_status\` = 'failed',
                \`validation_error\` = 
                    COALESCE(
                        \`validation_error\`,
                        'Import cancelled because duplicate data was found'
                    )
            WHERE \`batch_id\` = ?
              AND (
                  \`import_status\` IS NULL
                  OR \`import_status\` = 'pending'
              )
            `,
            [batchId]
        );

    } 
    else {

        // ---------------------------------------------------------
        // 4. SECOND LOOP - INSERT DATA
        // ---------------------------------------------------------

        for (const row of stagingRows) {

            try {

                const insertCols = [
                    "entity",
                    "user"
                ];

                const insertVals = [
                    row.entity,
                    row.user
                ];


                fields.forEach((fld) => {

                    const colName = `c${fld}`;

                    insertCols.push(colName);
                    insertVals.push(row[colName]);
                });


                const insertSql = `
                    INSERT INTO \`${mainTable}\`
                    (
                        ${insertCols
                            .map(col => `\`${col}\``)
                            .join(", ")}
                    )
                    VALUES (
                        ${insertVals
                            .map(() => "?")
                            .join(", ")}
                    )
                `;


                await pool.query(
                    insertSql,
                    insertVals
                );


                // Mark completed
                await pool.query(
                    `
                    UPDATE \`${staging_table}\`
                    SET
                        \`import_status\` = 'completed',
                        \`validation_error\` = NULL
                    WHERE \`batch_id\` = ?
                      AND \`row_number\` = ?
                    `,
                    [
                        batchId,
                        row.row_number
                    ]
                );


                successCount++;

            } catch (err) {

                console.error(err);

                let errMessage = err.message;


                if (err.code === "ER_DUP_ENTRY") {

                    const match = err.sqlMessage?.match(
                        /Duplicate entry '(.+?)' for key/
                    );

                    const duplicateValue =
                        match ? match[1] : "";

                    errMessage = duplicateValue
                        ? `Duplicate entry '${duplicateValue}'`
                        : "Duplicate record found";
                }


                errorDetails.push(
                    `Line ${row.row_number}: ${errMessage}`
                );


                await pool.query(
                    `
                    UPDATE \`${staging_table}\`
                    SET
                        \`import_status\` = 'failed',
                        \`validation_error\` = ?
                    WHERE \`batch_id\` = ?
                      AND \`row_number\` = ?
                    `,
                    [
                        errMessage,
                        batchId,
                        row.row_number
                    ]
                );
            }
        }
    }
    if (errorDetails.length > 0) {

        const errorMessage = errorDetails.join("\n");
        
        await pool.query(
            `
            UPDATE report_jobs
            SET
                status = 'failed',
                error = ?,
                notification_status = 'unread'
            WHERE id = ?
            `,
            [errorMessage, jobId]
        );
    
        return {
            code: "failed",
            message: "Validation failed. Please fix errors.",
            form_results: errorMessage
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


        case "emailValidation": {
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
        }


        case "phoneValidation": {
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
        }

        case "dateValidation": {
                
            let val = String(value).trim();
            let date = null;
                
            // Empty date handling
            if (val === '') {
                return {
                    status: true,
                    value: ''
                };
            }
        
            // Convert ISO datetime
            // Example: 2026-08-13T10:30:00 -> 2026-08-13
            if (/^\d{4}-\d{2}-\d{2}T/.test(val)) {
                val = val.split("T")[0];
            }
        
            /*
                Supported formats:
        
                Y-m-d
                Y/n/j
                Y/m/d
                m-d-Y
                n-j-Y
                m/d/Y
                n/j/Y
            */
        
            // YYYY-MM-DD or YYYY/MM/DD
            let match = val.match(
                /^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/
            );
        
            if (match) {
            
                const year = Number(match[1]);
                const month = Number(match[2]);
                const day = Number(match[3]);
            
                const tempDate = new Date(year, month - 1, day);
            
                // Same idea as DateTime::createFromFormat validation
                if (
                    tempDate.getFullYear() === year &&
                    tempDate.getMonth() === month - 1 &&
                    tempDate.getDate() === day
                ) {
                    date = tempDate;
                }
            }
        
            // MM-DD-YYYY or MM/DD/YYYY
            if (!date) {
            
                match = val.match(
                    /^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})$/
                );
            
                if (match) {
                
                    const month = Number(match[1]);
                    const day = Number(match[2]);
                    const year = Number(match[3]);
                
                    // Same extra PHP validation:
                    // month cannot be greater than 12
                    if (month <= 12) {
                    
                        const tempDate = new Date(year, month - 1, day);
                    
                        if (
                            tempDate.getFullYear() === year &&
                            tempDate.getMonth() === month - 1 &&
                            tempDate.getDate() === day
                        ) {
                            date = tempDate;
                        }
                    }
                }
            }
        
            // Invalid date
            if (!date || isNaN(date.getTime())) {
                return {
                    status: false,
                    message:
                        message ||
                        `Invalid date '${val}'. Please use YYYY-MM-DD, YYYY/MM/DD, MM-DD-YYYY or MM/DD/YYYY.`
                };
            }
        
            // Convert to YYYY-MM-DD
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
        
            return {
                status: true,
                value: `${year}-${month}-${day}`
            };
        }


        case "replace": {

            /*
                Rule format:
                replace(find)(replace)

                Examples:
                replace(,)('')
                replace(,)( )
                replace(-)(/)
            */

            const replaceRegex = /^replace\((.*?)\)\((.*?)\)$/;
            const match = String(rule).match(replaceRegex);


            if (!match) {

                return {
                    status: false,
                    message: "Invalid replace rule format"
                };

            }


            let find = match[1];
            let replace = match[2];


            // Remove quote wrappers
            if (
                (replace.startsWith("'") && replace.endsWith("'")) ||
                (replace.startsWith('"') && replace.endsWith('"'))
            ) {

                replace = replace.substring(1, replace.length - 1);

            }


            const newValue = String(value).replaceAll(find, replace);


            return {
                status: true,
                value: newValue
            };
        }


        default:
            return {
                status: true,
                value
            };
    }
}