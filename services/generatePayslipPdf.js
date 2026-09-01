const puppeteer = require("puppeteer");

function formatAmount(value) {
  const num = Number(value ?? 0);
  if (num < 0) {
    return `(${Math.abs(num).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })})`;
  }
  return num.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function numberValue(value) {
  return Number(value ?? 0);
}

function parseJsonArray(value) {
  if (!value) return [];

  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  return [];
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function getMonthName(month) {
  if (!month) return "";

  const date = new Date(`${month}-01T00:00:00`);

  return date
    .toLocaleDateString("en-US", {
      month: "long",
      year: "numeric",
    })
    .toUpperCase();
}

function createPayslipHtml(row, entityDisplay = row.entity) {
  const monthName = getMonthName(row.month);

  const currencyRate = numberValue(row.currency_rate);

  const basicSalaryRaw = numberValue(row.basic_salary);
  const basicSalary = formatAmount(basicSalaryRaw);
  const basicSalarySLR = formatAmount(basicSalaryRaw * currencyRate);

  const salaryAdjustmentRaw = numberValue(row.salary_adjustment);
  const salaryAdjustment = formatAmount(salaryAdjustmentRaw);
  const salaryAdjustmentSLR = formatAmount(salaryAdjustmentRaw * currencyRate);

  const noPayRaw = numberValue(row.no_pay_deduction);
  const noPayDeduction = formatAmount(noPayRaw);
  const noPayDeductionSLR = formatAmount(noPayRaw * currencyRate);

  const totalArrearsRaw = numberValue(row.total_arrears);
  const totalArrears = formatAmount(totalArrearsRaw);
  const totalArrearsSLR = formatAmount(totalArrearsRaw * currencyRate);

  const overtimeRaw = numberValue(row.overtime_amount);
  const overtimeAmount = formatAmount(overtimeRaw);
  const overtimeAmountSLR = formatAmount(overtimeRaw * currencyRate);

  const grossRaw = numberValue(row.gross_salary);
  const grossEarning = formatAmount(grossRaw);
  const grossEarningSLR = formatAmount(grossRaw * currencyRate);

  const epf8 = formatAmount(row.epf_employee_contribution_8);

  /*
   * If your query returns c5323 / c5324, use those values instead
   * of calculating the currency amount here.
   */
  const epf8Currency = formatAmount(
    numberValue(row.epf_employee_contribution_8) * currencyRate,
  );

  const stampDutyRaw = numberValue(row.stamp_duty);
  const stampDuty = formatAmount(stampDutyRaw);
  const stampDutyCurrency = formatAmount(row.cur_stamp_duty);

  const salaryAdvanceRaw = numberValue(row.salary_advance);
  const salaryAdvance = formatAmount(salaryAdvanceRaw);
  const salaryAdvanceCurrency = formatAmount(salaryAdvanceRaw * currencyRate);

  const totalDeduction = formatAmount(row.total_deduction);

  const totalDeductionCurrency = formatAmount(
    numberValue(row.total_deduction) * currencyRate,
  );
  const netSalary = formatAmount(row.take_home_amount);

  const netSalaryCurrency = formatAmount(numberValue(row.CUR_NET_SALARRY));

  const totalEPF = formatAmount(row.pf_liable_total_earning);

  const totalEPFCurrency = formatAmount(
    numberValue(row.pf_liable_total_earning) * currencyRate,
  );

  const epfEmployer12 = formatAmount(row.epf_employer_contribution_12);

  const epfEmployer12Currency = formatAmount(
    numberValue(row.epf_employer_contribution_12) * currencyRate,
  );

  const etfEmployer3 = formatAmount(row.etf_employer_contribution_3);

  const etfEmployer3Currency = formatAmount(
    numberValue(row.etf_employer_contribution_3) * currencyRate,
  );

  const allowances = parseJsonArray(row.allowance_details);
  const deductions = parseJsonArray(row.deduction_details);

  const allowanceRows = allowances
    .map((allowance) => {
      return `
        <div class="salary-row">
          <span>${escapeHtml(allowance.description)}</span>
          <span>${formatAmount(allowance.calculate_prorate_amount)}</span>
          <span>${formatAmount(
            numberValue(allowance.calculate_prorate_amount) * currencyRate,
          )}</span>
        </div>
      `;
    })
    .join("");

  const deductionRows = deductions
    .map((deduction) => {
      return `
        <div class="salary-row">
          <span>${escapeHtml(deduction.description)}</span>
          <span>${formatAmount(deduction.amount)}</span>
          <span>${formatAmount(numberValue(deduction.currency_amount))}</span>
        </div>
      `;
    })
    .join("");

  return `
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">

<style>
@page {
  size: A4;
  margin: 12mm;
}

* {
  box-sizing: border-box;
}

body {
    font-size: 12px;
    font-family: Arial, sans-serif;
    background: #f5f5f5;
    margin: 0;
    padding: 20px;
}
.payslip-container {
    width: 850px;
    margin: auto;
    background: #fff;
    padding: 30px;
}

.company-header {
    text-align: center;
}

.company-name {
    font-size: 24px;
    font-weight: bold;
}

// .company-address {
//     font-size: 14px;
//     margin: 3px 0;
// }

.pay-title {
    text-align: center;
    margin: 30px 0;
    font-size: 22px;
    font-weight:300;
}

.employee-section {
    display: flex;
    justify-content: space-between;
    margin-bottom: 30px;
}

.employee-left,
.employee-right {
    width: 45%;
}
.rowlol {
    display: flex;
    margin-bottom: 10px;
}
.label {
    font-weight: bold;
    display: inline-block;
    width: 140px;
    color: #000;
    flex-shrink: 0;
}

.value {
    flex: 1;
    word-break: break-word;
    margin-left: 10px;
}

.section-title {
    font-size: 16px;
    font-weight: bold;
    text-decoration: underline;
    letter-spacing: 3px;
    margin-bottom: 10px;
}

.salary-row {
    display: grid;
    grid-template-columns: 2fr 1fr 1fr;
    padding: 8px 0;
}

.salary-row span:nth-child(2),
.salary-row span:nth-child(3) {
    text-align: right;
    justify-self: end;
}

.header-row {
    font-weight: bold;
}

.total {
    font-weight: bold;
    /* border-top: 1px solid #000;
    border-bottom: 4px double  #000; */
    padding: 10px 0;
}
.linesty {
    display: inline-block;
    min-width: 120px;   /* adjust as needed */
    text-align: right;
    
    border-top: 1px solid #000;
    border-bottom: 4px double #000;
}

.net-salary {
    font-size: 18px;
}

/* .bank-section {
    margin-top: 40px;
    border-top: 2px dashed #999;
    padding-top: 20px;
} */

.bank-section {
    margin-top: 40px;
    border-top: 2px dashed #999;
    padding-top: 20px;
    display: flex;
    flex-wrap: wrap; /* important */
    margin-bottom: 30px;
}

.bank-full {
    width: 100%;
}

.bank-left,
.bank-right {
    width: 50%;
}

</style>
</head>

<body>

<div class="company-header">
  <h2 class="company-name">
    PORT CITY BPO (PVT) LTD
  </h2>

  <p class="company-address">
    2nd Floor, Aitken Spence Tower
  </p>

  <p class="company-address">
    #315, Vauxhall Street, Colombo 02.
  </p>
</div>

<h2 class="pay-title">
  PAY ADVICE FOR THE MONTH OF ${monthName}
</h2>

<div class="employee-section">

  <div class="employee-left">

    <p>
      <span class="label">EMPLOYEE NO</span>
      <span class="value">${escapeHtml(row.employee_id)}</span>
    </p>

    <p>
      <span class="label">NAME</span>
      <span class="value">${escapeHtml(row.employee_name)}</span>
    </p>

  </div>

  <div class="employee-right">

    <div class="rowlol">
      <div class="label">DIVISION</div>
      <div class="value">${escapeHtml(entityDisplay)}</div>
    </div>

    <div class="rowlol">
      <div class="label">DESIGNATION</div>
      <div class="value">${escapeHtml(row.designation)}</div>
    </div>

  </div>

</div>


<!-- EARNINGS -->

<div class="salary-section">

  <h3 class="section-title">EARNINGS</h3>

  <div class="salary-row header-row">
    <span></span>
    <span>USD</span>
    <span>LKR</span>
  </div>

  <div class="salary-row">
    <span>Basic Salary</span>
    <span>${basicSalary}</span>
    <span>${basicSalarySLR}</span>
  </div>

  ${
    salaryAdjustmentRaw !== 0
      ? `
  <div class="salary-row">
    <span>Salary Adjustment</span>
    <span>${salaryAdjustment}</span>
    <span>${salaryAdjustmentSLR}</span>
  </div>
  `
      : ""
  }

  ${
    noPayRaw !== 0
      ? `
  <div class="salary-row">
    <span>No Pay Deductions</span>
    <span>(${noPayDeduction})</span>
    <span>(${noPayDeductionSLR})</span>
  </div>
  `
      : ""
  }

  ${
    totalArrearsRaw !== 0
      ? `
  <div class="salary-row">
    <span>Arrears</span>
    <span>${totalArrears}</span>
    <span>${totalArrearsSLR}</span>
  </div>
  `
      : ""
  }

  ${allowanceRows}

  ${
    overtimeRaw !== 0
      ? `
  <div class="salary-row">
    <span>Over Time Earning</span>
    <span>${overtimeAmount}</span>
    <span>${overtimeAmountSLR}</span>
  </div>
  `
      : ""
  }

  <div class="salary-row total">
    <span>Gross Earning</span>
    <span class="linesty">${grossEarning}</span>
    <span class="linesty">${grossEarningSLR}</span>
  </div>

</div>


<!-- DEDUCTIONS -->

<div class="salary-section">

  <h3 class="section-title">DEDUCTIONS</h3>

  ${deductionRows}

  <div class="salary-row">
    <span>E.P.F Employee Contribution 8%</span>
    <span>${epf8}</span>
    <span>${epf8Currency}</span>
  </div>

  ${
    stampDutyRaw !== 0
      ? `
  <div class="salary-row">
    <span>Stamp Duty</span>
    <span>${stampDuty}</span>
    <span>${stampDutyCurrency}</span>
  </div>
  `
      : ""
  }

  ${
    salaryAdvanceRaw !== 0
      ? `
  <div class="salary-row">
    <span>Salary Advance</span>
    <span>${salaryAdvance}</span>
    <span>${salaryAdvanceCurrency}</span>
  </div>
  `
      : ""
  }

  <div class="salary-row total">
    <span>Total Deductions</span>
    <span class="linesty">${totalDeduction}</span>
    <span class="linesty">${totalDeductionCurrency}</span>
  </div>

</div>


<!-- SUMMARY -->

<div class="salary-section">

  <h3 class="section-title">SUMMARY</h3>

  <div class="salary-row">
    <span>Gross Earning</span>
    <span>${grossEarning}</span>
    <span>${grossEarningSLR}</span>
  </div>

  <div class="salary-row">
    <span>Total Deductions</span>
    <span>${totalDeduction}</span>
    <span>${totalDeductionCurrency}</span>
  </div>

  <div class="salary-row total">
    <span>Net Salary</span>
    <span class="linesty">${netSalary}</span>
    <span class="linesty">${netSalaryCurrency}</span>
  </div>

</div>


<!-- EPF -->

<div class="salary-section">

  <h3 class="section-title">EPF/FUND SUMMARY</h3>

  <div class="salary-row">
    <span>Total for E.P.F</span>
    <span>${totalEPF}</span>
    <span>${totalEPFCurrency}</span>
  </div>

  <div class="salary-row">
    <span>E.P.F Employee Contribution 8%</span>
    <span>${epf8}</span>
    <span>${epf8Currency}</span>
  </div>

  <div class="salary-row">
    <span>E.P.F Employer Contribution 12%</span>
    <span>${epfEmployer12}</span>
    <span>${epfEmployer12Currency}</span>
  </div>

  <div class="salary-row">
    <span>E.T.F Employer Contribution 3%</span>
    <span>${etfEmployer3}</span>
    <span>${etfEmployer3Currency}</span>
  </div>

</div>


<!-- BANK -->

<div class="bank-section">

  <div class="bank-full">
    <p>
      FOREIGN CURRENCY RATE (USD) :
      ${formatAmount(currencyRate)}
    </p>
  </div>

  <div class="bank-full">
    <p>
      YOUR NET SALARY HAS BEEN CREDITED TO THE FOLLOWING BANK A/C.
    </p>
  </div>

  <div class="bank-left">

    <p>
      <span class="label">BANK NAME</span>
      <span class="value">: ${escapeHtml(row.bank_name)}</span>
    </p>

    <p>
      <span class="label">A/C No</span>
      <span class="value">: ${escapeHtml(row.bank_account_no)}</span>
    </p>

  </div>

  <div class="bank-right">

    <p>
      <span class="label">BRANCH</span>
      <span class="value">: ${escapeHtml(row.branch_name)}</span>
    </p>

    <p>
      <span class="label">Amount</span>
      <span class="value">
        : ${netSalaryCurrency}
        &nbsp;&nbsp;&nbsp;&nbsp;
        (USD): ${netSalary}
      </span>
    </p>

  </div>

</div>

</body>
</html>
`;
}

async function generatePayslipPdf(row, outputPath, entityDisplay) {
  const browser = await puppeteer.launch({
    executablePath:
      "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",

    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();

    const html = createPayslipHtml(row, entityDisplay);

    await page.setContent(html, {
      waitUntil: "networkidle0",
    });

    await page.pdf({
      path: outputPath,
      format: "A4",
      printBackground: true,
      margin: {
        top: "10mm",
        right: "10mm",
        bottom: "10mm",
        left: "10mm",
      },
    });
  } finally {
    await browser.close();
  }
}

module.exports = {
  createPayslipHtml,
  generatePayslipPdf,
};
