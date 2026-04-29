// ============ Format helpers ============
function fmt(n) {
  if (n == null || isNaN(n)) return '—';
  const abs = Math.abs(n);
  if (abs >= 10000000) return '₹' + (n/10000000).toFixed(2) + ' Cr';
  if (abs >= 100000) return '₹' + (n/100000).toFixed(2) + ' L';
  if (abs >= 1000) return '₹' + (n/1000).toFixed(1) + 'K';
  return '₹' + Math.round(n);
}
function fmtFull(n) {
  if (n == null || isNaN(n)) return '—';
  return '₹' + Number(n).toLocaleString('en-IN', {maximumFractionDigits: 2});
}
function fmtPct(n) {
  if (n == null || isNaN(n)) return '—';
  return (n > 0 ? '+' : '') + n.toFixed(1) + '%';
}
function fmtDate(s) {
  if (!s) return '—';
  try { return new Date(s).toLocaleDateString('en-IN', {day:'2-digit', month:'short', year:'numeric'}); }
  catch(e) { return s; }
}

// ============ State ============
let currentPreset = 'this_month';
let currentCompare = 'previous_period';
let customFrom = null;
let customTo = null;
let drillStack = [];

const qs = () => {
  if (currentPreset === 'custom' && customFrom && customTo) {
    return `?from=${customFrom}&to=${customTo}&compare=${currentCompare}`;
  }
  return `?preset=${currentPreset}&compare=${currentCompare}`;
};
const qsNoCompare = () => {
  if (currentPreset === 'custom' && customFrom && customTo) {
    return `?from=${customFrom}&to=${customTo}`;
  }
  return `?preset=${currentPreset}`;
};
const api = (path) => fetch(path).then(r => r.json());

const COLORS = {
  ink: '#161513', accent: '#c9551f', accent2: '#e06a2a',
  good: '#2d6a4f', warn: '#b5880a', bad: '#9b2226',
  muted: '#6e6a60', line: '#d9d2c0'
};
const baseChart = {
  chart: { fontFamily: "'Geist', sans-serif", toolbar: {show: false}, background: 'transparent',
           animations: {enabled: true, easing: 'easeout', speed: 400} },
  grid: { borderColor: COLORS.line, strokeDashArray: 3 },
  dataLabels: { enabled: false },
  tooltip: { theme: 'light', style: {fontFamily: "'JetBrains Mono', monospace", fontSize: '12px'} }
};

// ============ KPIs ============
async function loadKPIs() {
  const data = await api('/api/kpi' + qs());
  const metrics = [
    {k: 'cash', label: 'Cash Position', drill: 'cash', accent: true},
    {k: 'revenue', label: 'Revenue', drill: 'revenue'},
    {k: 'expenses', label: 'Expenses', drill: 'expenses'},
    {k: 'net_income', label: 'Net Income', drill: 'pnl', highlight: true},
    {k: 'ar', label: 'Receivable (AR)', drill: 'ar'},
    {k: 'ap', label: 'Payable (AP)', drill: 'ap'},
  ];
  const grid = document.getElementById('kpi-grid');
  grid.innerHTML = '';
  metrics.forEach(m => {
    const v = data.primary[m.k];
    const d = data.deltas[m.k] || {pct:0, abs:0};
    const arrow = d.pct > 0 ? '↑' : (d.pct < 0 ? '↓' : '·');
    const color = d.pct > 0 ? 'var(--good)' : (d.pct < 0 ? 'var(--bad)' : 'var(--muted)');
    const cls = ['kpi-card'];
    if (m.accent) cls.push('kpi-accent');
    if (m.highlight) cls.push('kpi-highlight');
    const btn = document.createElement('button');
    btn.className = cls.join(' ');
    btn.innerHTML = `
      <div class="kpi-label">${m.label}</div>
      <div class="kpi-value">${fmt(v)}</div>
      <div class="kpi-foot" style="color:${color}; font-weight:500;">
        ${arrow} ${fmtPct(d.pct)} <span style="opacity:0.6; margin-left:4px;">${fmt(d.abs)}</span>
      </div>`;
    btn.onclick = () => drillTo({level: m.drill, label: m.label});
    grid.appendChild(btn);
  });
}

// ============ Charts ============
async function loadTrendChart() {
  const data = await api('/api/monthly-trend' + qsNoCompare());
  const el = document.querySelector('#chart-trend');
  el.innerHTML = '';
  if (data.length === 0) { el.innerHTML = '<div class="empty-state">No data in this period</div>'; return; }
  new ApexCharts(el, {
    ...baseChart,
    chart: {...baseChart.chart, type: 'bar', height: 320,
      events: { dataPointSelection: (_, __, cfg) => {
        const m = data[cfg.dataPointIndex];
        drillTo({level: 'month', label: m.month, payload: m.month_key});
      }}},
    series: [
      {name: 'Revenue', data: data.map(d => d.revenue)},
      {name: 'Expenses', data: data.map(d => d.expenses)}
    ],
    xaxis: {categories: data.map(d => d.month),
            labels: {style: {fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", colors: COLORS.muted}}},
    yaxis: {labels: {style: {fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", colors: COLORS.muted}, formatter: fmt}},
    colors: [COLORS.ink, COLORS.accent],
    plotOptions: {bar: {borderRadius: 2, columnWidth: '55%'}},
    legend: {position: 'top', horizontalAlign: 'right', fontFamily: "'JetBrains Mono', monospace", fontSize: '11px',
             labels: {colors: COLORS.muted}, markers: {width: 10, height: 10, radius: 2}},
    tooltip: {...baseChart.tooltip, y: {formatter: fmtFull}}
  }).render();
}

async function loadAgingChart() {
  const data = await api('/api/ar-aging');
  const el = document.querySelector('#chart-aging');
  el.innerHTML = '';
  new ApexCharts(el, {
    ...baseChart,
    chart: {...baseChart.chart, type: 'bar', height: 320,
      events: { dataPointSelection: (_, __, cfg) => {
        drillTo({level: 'aging_bucket', label: data[cfg.dataPointIndex].bucket, payload: data[cfg.dataPointIndex].bucket});
      }}},
    series: [{name: 'Amount', data: data.map(d => d.amount)}],
    xaxis: {categories: data.map(d => d.bucket)},
    yaxis: {labels: {style: {fontSize: '11px', fontFamily: "'JetBrains Mono', monospace", colors: COLORS.muted}, formatter: fmt}},
    plotOptions: {bar: {borderRadius: 2, columnWidth: '52%', distributed: true}},
    colors: [COLORS.good, COLORS.warn, COLORS.accent2, COLORS.bad],
    legend: {show: false},
    tooltip: {...baseChart.tooltip, y: {formatter: fmtFull}}
  }).render();
}

async function loadCustomersChart() {
  const data = await api('/api/top-customers' + qsNoCompare());
  const el = document.querySelector('#chart-customers');
  el.innerHTML = '';
  if (data.length === 0) { el.innerHTML = '<div class="empty-state">No customer revenue in this period</div>'; return; }
  new ApexCharts(el, {
    ...baseChart,
    chart: {...baseChart.chart, type: 'bar', height: 320,
      events: { dataPointSelection: (_, __, cfg) => {
        const c = data[cfg.dataPointIndex];
        drillTo({level: 'customer', label: c.customer, payload: c.customer});
      }}},
    series: [{name: 'Revenue', data: data.map(d => d.revenue)}],
    xaxis: {categories: data.map(d => d.customer),
            labels: {style: {fontSize: '10px', fontFamily: "'JetBrains Mono', monospace", colors: COLORS.muted}, formatter: fmt}},
    yaxis: {labels: {style: {fontSize: '11px', colors: COLORS.muted}}},
    plotOptions: {bar: {horizontal: true, borderRadius: 2, barHeight: '70%'}},
    colors: [COLORS.accent],
    legend: {show: false},
    tooltip: {...baseChart.tooltip, y: {formatter: fmtFull}},
    dataLabels: {enabled: true, textAnchor: 'start', offsetX: 6,
                 style: {fontSize: '10px', fontFamily: "'JetBrains Mono', monospace", colors: [COLORS.ink]},
                 formatter: fmt}
  }).render();
}

async function loadExpensesChart() {
  const data = await api('/api/expense-categories' + qsNoCompare());
  const el = document.querySelector('#chart-expenses');
  el.innerHTML = '';
  if (data.length === 0) { el.innerHTML = '<div class="empty-state">No expenses in this period</div>'; return; }
  new ApexCharts(el, {
    ...baseChart,
    chart: {...baseChart.chart, type: 'donut', height: 320,
      events: { dataPointSelection: (_, __, cfg) => {
        const c = data[cfg.dataPointIndex];
        drillTo({level: 'expense_category', label: c.category, payload: c.category});
      }}},
    series: data.map(d => d.amount),
    labels: data.map(d => d.category),
    colors: ['#c9551f','#2a2822','#2d6a4f','#b5880a','#9b2226','#6e6a60','#e06a2a','#4a7c59','#8b6914','#bf9b30','#553c25','#a0574a'],
    legend: {position: 'right', fontSize: '12px', fontFamily: "'Geist', sans-serif",
             labels: {colors: COLORS.muted}, markers: {width: 10, height: 10, radius: 2}},
    plotOptions: {pie: {donut: {size: '62%', labels: {show: true,
      value: {fontFamily: "'Fraunces', serif", fontSize: '22px', color: COLORS.ink, formatter: fmt},
      total: {show: true, label: 'Total', fontFamily: "'Geist', sans-serif", fontSize: '11px', color: COLORS.muted,
              formatter: w => fmt(w.globals.seriesTotals.reduce((a,b)=>a+b, 0))}}}}},
    tooltip: {...baseChart.tooltip, y: {formatter: fmtFull}},
    stroke: {width: 2, colors: ['#ffffff']}
  }).render();
}

// ============ Summary cards ============
async function loadSummaries() {
  const [pnl, bs, cf] = await Promise.all([
    api('/api/pnl' + qs()),
    api('/api/balance-sheet'),
    api('/api/cash-flow' + qs())
  ]);
  const del = (a, b) => b ? ((a-b)/Math.abs(b)*100) : 0;
  const colorOf = d => d > 0 ? 'var(--good)' : (d < 0 ? 'var(--bad)' : 'var(--muted)');

  const p = pnl.primary, pc = pnl.comparison;
  document.getElementById('summary-pnl').innerHTML = `
    <div class="sum-row"><span>Revenue</span><span>${fmt(p.total_revenue)}</span></div>
    <div class="sum-row"><span>Expenses</span><span>${fmt(p.total_expense)}</span></div>
    <div class="sum-row sum-total"><span>Net Income</span>
      <span>${fmt(p.net_income)} <span style="color:${colorOf(del(p.net_income, pc.net_income))}; font-size:11px; margin-left:4px;">${fmtPct(del(p.net_income, pc.net_income))}</span></span>
    </div>
    <div style="color:var(--muted); font-size:11px; margin-top:8px; text-align:right;">Margin: ${p.net_margin_pct.toFixed(1)}%</div>`;

  document.getElementById('summary-bs').innerHTML = `
    <div class="sum-row"><span>Total Assets</span><span>${fmt(bs.total_assets)}</span></div>
    <div class="sum-row"><span>Total Liabilities</span><span>${fmt(bs.total_liabilities)}</span></div>
    <div class="sum-row sum-total"><span>Total Equity</span><span>${fmt(bs.total_equity)}</span></div>`;

  const cfp = cf.primary, cfc = cf.comparison;
  document.getElementById('summary-cf').innerHTML = `
    <div class="sum-row"><span>Cash In</span><span>${fmt(cfp.inflow)}</span></div>
    <div class="sum-row"><span>Cash Out</span><span>${fmt(cfp.outflow)}</span></div>
    <div class="sum-row sum-total"><span>Net Flow</span>
      <span>${fmt(cfp.net_cash_flow)} <span style="color:${colorOf(del(cfp.net_cash_flow, cfc.net_cash_flow))}; font-size:11px; margin-left:4px;">${fmtPct(del(cfp.net_cash_flow, cfc.net_cash_flow))}</span></span>
    </div>`;
}

// ============ Tables ============
async function loadItems() {
  const data = await api('/api/top-items' + qsNoCompare());
  const tb = document.querySelector('#tbl-items tbody');
  tb.innerHTML = data.length === 0
    ? `<tr><td colspan="3" class="empty-td">No items sold in this period</td></tr>`
    : data.map(r => `<tr class="clickable" data-drill='${JSON.stringify({level:"item", label: r.item, payload: r.item})}'>
        <td>${r.item}</td><td class="num">${r.quantity}</td><td class="num">${fmtFull(r.revenue)}</td>
      </tr>`).join('');
  tb.querySelectorAll('tr.clickable').forEach(tr => {
    tr.addEventListener('click', () => drillTo(JSON.parse(tr.dataset.drill)));
  });
}
async function loadVendors() {
  const data = await api('/api/top-vendors' + qsNoCompare());
  const tb = document.querySelector('#tbl-vendors tbody');
  tb.innerHTML = data.length === 0
    ? `<tr><td colspan="3" class="empty-td">No vendor transactions in this period</td></tr>`
    : data.map(r => `<tr class="clickable" data-drill='${JSON.stringify({level:"vendor", label: r.vendor, payload: r.vendor})}'>
        <td>${r.vendor}</td><td class="num">${r.transactions}</td><td class="num">${fmtFull(r.amount)}</td><td class="num">${fmtFull(r.balance || 0)}</td>
      </tr>`).join('');
  tb.querySelectorAll('tr.clickable').forEach(tr => {
    tr.addEventListener('click', () => drillTo(JSON.parse(tr.dataset.drill)));
  });
}
async function loadRecent() {
  const data = await api('/api/recent-transactions');
  const tb = document.querySelector('#tbl-recent tbody');
  tb.innerHTML = data.map(t => `<tr class="clickable" data-drill='${JSON.stringify({level: t.type==="Invoice" ? "invoice" : "bill", label: t.type + " " + t.number, payload: t.id})}'>
    <td><span class="tag tag-${t.type.toLowerCase()}">${t.type}</span></td>
    <td>${t.number || '—'}</td>
    <td>${t.party || '—'}</td>
    <td>${fmtDate(t.txn_date)}</td>
    <td class="num">${fmtFull(t.amount)}</td>
  </tr>`).join('');
  tb.querySelectorAll('tr.clickable').forEach(tr => {
    tr.addEventListener('click', () => drillTo(JSON.parse(tr.dataset.drill)));
  });
}

// ============ Drill-down ============
function drillTo(state) {
  drillStack.push(state);
  syncHash();
  renderDrill();
  window.scrollTo({top: 0, behavior: 'smooth'});
}
function drillBackTo(i) { drillStack = drillStack.slice(0, i + 1); syncHash(); renderDrill(); }
function drillClear() { drillStack = []; syncHash(); renderDrill(); refreshOverview(); }
window.drillTo = drillTo;
window.drillBackTo = drillBackTo;
window.drillClear = drillClear;

function syncHash() {
  if (drillStack.length === 0) history.replaceState(null, '', '#');
  else history.replaceState(null, '', '#' + drillStack.map(s => `${s.level}:${encodeURIComponent(s.payload || '')}`).join('/'));
}
function restoreFromHash() {
  const h = location.hash.slice(1);
  if (!h) return;
  drillStack = h.split('/').filter(Boolean).map(p => {
    const [level, payload] = p.split(':');
    return { level, label: decodeURIComponent(payload || ''), payload: decodeURIComponent(payload || '') };
  });
}

async function renderDrill() {
  const overview = document.getElementById('view-overview');
  const drill = document.getElementById('view-drill');
  const crumbs = document.getElementById('breadcrumbs');

  if (drillStack.length === 0) {
    overview.style.display = 'block';
    drill.style.display = 'none';
    crumbs.style.display = 'none';
    return;
  }
  overview.style.display = 'none';
  drill.style.display = 'block';
  crumbs.style.display = 'flex';

  let h = `<span class="crumb" onclick="drillClear()">Overview</span>`;
  drillStack.forEach((s, i) => {
    const last = i === drillStack.length - 1;
    h += `<span class="crumb-sep">›</span>`;
    h += `<span class="crumb ${last ? 'current' : ''}" ${last ? '' : `onclick="drillBackTo(${i})"`}>${s.label}</span>`;
  });
  crumbs.innerHTML = h;

  const top = drillStack[drillStack.length - 1];
  const titleEl = document.getElementById('drill-title');
  const subEl = document.getElementById('drill-sub');
  const content = document.getElementById('drill-content');
  content.innerHTML = '<div class="loading-state">Loading…</div>';

  try {
    if (top.level === 'cash') await drillCash(titleEl, subEl, content);
    else if (top.level === 'ar') await drillAR(titleEl, subEl, content);
    else if (top.level === 'ap') await drillAP(titleEl, subEl, content);
    else if (top.level === 'revenue') await drillRevenue(titleEl, subEl, content);
    else if (top.level === 'expenses') await drillExpenses(titleEl, subEl, content);
    else if (top.level === 'customer') await drillCustomer(top, titleEl, subEl, content);
    else if (top.level === 'invoice') await drillInvoice(top, titleEl, subEl, content);
    else if (top.level === 'aging_bucket') await drillAgingBucket(top, titleEl, subEl, content);
    else if (top.level === 'expense_category') await drillExpenseCategory(top, titleEl, subEl, content);
    else if (top.level === 'pnl') await drillPNL(titleEl, subEl, content);
    else if (top.level === 'bs') await drillBS(titleEl, subEl, content);
    else if (top.level === 'cf') await drillCF(titleEl, subEl, content);
    else if (top.level === 'month') await drillMonth(top, titleEl, subEl, content);
    else content.innerHTML = `<div class="loading-state">Detail for '${top.level}' available in Phase 2.</div>`;
  } catch (e) {
    content.innerHTML = `<div class="loading-state" style="color:var(--bad)">Error: ${e.message}</div>`;
  }
}

// Drill handlers
async function drillCash(titleEl, subEl, content) {
  const rows = await api('/api/cash-accounts');
  titleEl.textContent = 'Cash Position'; subEl.textContent = 'Bank & cash accounts';
  const total = rows.reduce((a,b) => a+b.balance, 0);
  content.innerHTML = `
    <div class="drill-summary">
      <div class="drill-summary-item"><div class="meta-label">Total Cash</div><div class="meta-value">${fmtFull(total)}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Accounts</div><div class="meta-value">${rows.length}</div></div>
    </div>
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Account</th><th>Type</th><th class="num">Balance</th></tr></thead>
      <tbody>${rows.map(r => `<tr><td>${r.full_name}</td><td><span class="tag tag-bill">${r.account_type}</span></td><td class="num">${fmtFull(r.balance)}</td></tr>`).join('')}</tbody>
    </table></div>`;
}

async function drillAR(titleEl, subEl, content) {
  const rows = await api('/api/ar-aging');
  titleEl.textContent = 'Accounts Receivable'; subEl.textContent = 'Click a bucket to see invoices';
  const total = rows.reduce((a,b) => a+b.amount, 0);
  content.innerHTML = `
    <div class="drill-summary"><div class="drill-summary-item">
      <div class="meta-label">Total Outstanding</div>
      <div class="meta-value">${fmtFull(total)}</div>
    </div></div>
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Aging Bucket</th><th class="num">Amount</th></tr></thead>
      <tbody>${rows.map(r => `<tr class="clickable" data-drill='${JSON.stringify({level:"aging_bucket", label: r.bucket, payload: r.bucket})}'>
        <td>${r.bucket}</td><td class="num">${fmtFull(r.amount)}</td></tr>`).join('')}</tbody>
    </table></div>`;
  content.querySelectorAll('tr.clickable').forEach(tr => {
    tr.addEventListener('click', () => drillTo(JSON.parse(tr.dataset.drill)));
  });
}

async function drillAP(titleEl, subEl, content) {
  const rows = await api('/api/ap-aging');
  titleEl.textContent = 'Accounts Payable'; subEl.textContent = 'Bills by aging';
  const total = rows.reduce((a,b) => a+b.amount, 0);
  content.innerHTML = `
    <div class="drill-summary"><div class="drill-summary-item">
      <div class="meta-label">Total Owed</div><div class="meta-value">${fmtFull(total)}</div>
    </div></div>
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Aging Bucket</th><th class="num">Amount</th></tr></thead>
      <tbody>${rows.map(r => `<tr><td>${r.bucket}</td><td class="num">${fmtFull(r.amount)}</td></tr>`).join('')}</tbody>
    </table></div>`;
}

async function drillRevenue(titleEl, subEl, content) {
  const data = await api('/api/top-customers' + qsNoCompare() + '&n=50');
  titleEl.textContent = 'Revenue Breakdown'; subEl.textContent = `By customer · ${data.length} customers`;
  const total = data.reduce((a,b) => a + b.revenue, 0);
  content.innerHTML = `
    <div class="drill-summary"><div class="drill-summary-item">
      <div class="meta-label">Total Revenue</div><div class="meta-value">${fmtFull(total)}</div>
    </div></div>
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Customer</th><th class="num">Invoices</th><th class="num">Revenue</th></tr></thead>
      <tbody>${data.map(r => `<tr class="clickable" data-drill='${JSON.stringify({level:"customer", label: r.customer, payload: r.customer})}'>
        <td>${r.customer}</td><td class="num">${r.invoices}</td><td class="num">${fmtFull(r.revenue)}</td>
      </tr>`).join('')}</tbody>
    </table></div>`;
  content.querySelectorAll('tr.clickable').forEach(tr => {
    tr.addEventListener('click', () => drillTo(JSON.parse(tr.dataset.drill)));
  });
}

async function drillExpenses(titleEl, subEl, content) {
  const data = await api('/api/expense-categories' + qsNoCompare());
  titleEl.textContent = 'Expense Breakdown'; subEl.textContent = 'By category';
  const total = data.reduce((a,b) => a + b.amount, 0);
  content.innerHTML = `
    <div class="drill-summary"><div class="drill-summary-item">
      <div class="meta-label">Total Expenses</div><div class="meta-value">${fmtFull(total)}</div>
    </div></div>
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Category</th><th class="num">Amount</th></tr></thead>
      <tbody>${data.map(r => `<tr class="clickable" data-drill='${JSON.stringify({level:"expense_category", label: r.category, payload: r.category})}'>
        <td>${r.category}</td><td class="num">${fmtFull(r.amount)}</td>
      </tr>`).join('')}</tbody>
    </table></div>`;
  content.querySelectorAll('tr.clickable').forEach(tr => {
    tr.addEventListener('click', () => drillTo(JSON.parse(tr.dataset.drill)));
  });
}

async function drillCustomer(top, titleEl, subEl, content) {
  const d = await api('/api/customer/' + encodeURIComponent(top.payload) + '/invoices');
  titleEl.textContent = d.customer;
  subEl.textContent = `${d.invoices.length} invoice${d.invoices.length===1?'':'s'}`;
  const total = d.invoices.reduce((a,b)=>a+b.subtotal, 0);
  const outstanding = d.invoices.reduce((a,b)=>a+b.balance_remaining, 0);
  content.innerHTML = `
    <div class="drill-summary">
      <div class="drill-summary-item"><div class="meta-label">Total Billed</div><div class="meta-value">${fmtFull(total)}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Outstanding</div><div class="meta-value" style="color:var(--accent)">${fmtFull(outstanding)}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Invoices</div><div class="meta-value">${d.invoices.length}</div></div>
    </div>
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Invoice #</th><th>Date</th><th>Due</th><th class="num">Amount</th><th class="num">Balance</th><th>Status</th></tr></thead>
      <tbody>${d.invoices.map(inv => `<tr class="clickable" data-drill='${JSON.stringify({level:"invoice", label: "Invoice " + inv.txn_number, payload: inv.txn_id})}'>
        <td>${inv.txn_number}</td><td>${fmtDate(inv.txn_date)}</td><td>${fmtDate(inv.due_date)}</td>
        <td class="num">${fmtFull(inv.subtotal)}</td><td class="num">${fmtFull(inv.balance_remaining)}</td>
        <td><span class="tag tag-${inv.is_paid?'paid':'open'}">${inv.is_paid?'Paid':'Open'}</span></td>
      </tr>`).join('')}</tbody>
    </table></div>`;
  content.querySelectorAll('tr.clickable').forEach(tr => {
    tr.addEventListener('click', () => drillTo(JSON.parse(tr.dataset.drill)));
  });
}

async function drillInvoice(top, titleEl, subEl, content) {
  const d = await api('/api/invoice/' + encodeURIComponent(top.payload) + '/lines');
  if (d.error) { content.innerHTML = '<div class="loading-state">Invoice not found.</div>'; return; }
  titleEl.textContent = 'Invoice ' + d.invoice.txn_number;
  subEl.textContent = d.invoice.customer_name;
  content.innerHTML = `
    <div class="drill-summary">
      <div class="drill-summary-item"><div class="meta-label">Date</div><div class="meta-value">${fmtDate(d.invoice.txn_date)}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Due</div><div class="meta-value">${fmtDate(d.invoice.due_date)}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Total</div><div class="meta-value">${fmtFull(d.invoice.subtotal)}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Balance</div><div class="meta-value" style="color:${d.invoice.balance_remaining>0?'var(--accent)':'var(--good)'}">${fmtFull(d.invoice.balance_remaining)}</div></div>
    </div>
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Item</th><th>Description</th><th class="num">Qty</th><th class="num">Rate</th><th class="num">Amount</th></tr></thead>
      <tbody>${d.lines.map(l => `<tr>
        <td>${l.item_name||'—'}</td><td>${l.description||'—'}</td>
        <td class="num">${l.quantity}</td><td class="num">${fmtFull(l.rate)}</td><td class="num">${fmtFull(l.amount)}</td>
      </tr>`).join('')}</tbody>
    </table></div>`;
}

async function drillAgingBucket(top, titleEl, subEl, content) {
  const d = await api('/api/aging-bucket/' + encodeURIComponent(top.payload));
  titleEl.textContent = d.bucket;
  subEl.textContent = `${d.invoices.length} invoice${d.invoices.length===1?'':'s'}`;
  const total = d.invoices.reduce((a,b)=>a+b.balance_remaining, 0);
  content.innerHTML = `
    <div class="drill-summary"><div class="drill-summary-item">
      <div class="meta-label">Total in bucket</div><div class="meta-value">${fmtFull(total)}</div>
    </div></div>
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Invoice #</th><th>Customer</th><th>Date</th><th>Due</th><th class="num">Days Overdue</th><th class="num">Balance</th></tr></thead>
      <tbody>${d.invoices.map(inv => `<tr class="clickable" data-drill='${JSON.stringify({level:"invoice", label: "Invoice " + inv.txn_number, payload: inv.txn_id})}'>
        <td>${inv.txn_number}</td><td>${inv.customer_name}</td>
        <td>${fmtDate(inv.txn_date)}</td><td>${fmtDate(inv.due_date)}</td>
        <td class="num">${inv.days_overdue}</td><td class="num">${fmtFull(inv.balance_remaining)}</td>
      </tr>`).join('')}</tbody>
    </table></div>`;
  content.querySelectorAll('tr.clickable').forEach(tr => {
    tr.addEventListener('click', () => drillTo(JSON.parse(tr.dataset.drill)));
  });
}

async function drillExpenseCategory(top, titleEl, subEl, content) {
  const d = await api('/api/expense-category/' + encodeURIComponent(top.payload) + '/bills');
  titleEl.textContent = d.category;
  subEl.textContent = `${d.bills.length} bill lines`;
  const total = d.bills.reduce((a,b)=>a+b.line_amount, 0);
  content.innerHTML = `
    <div class="drill-summary"><div class="drill-summary-item">
      <div class="meta-label">Total in category</div><div class="meta-value">${fmtFull(total)}</div>
    </div></div>
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Bill #</th><th>Vendor</th><th>Date</th><th>Description</th><th class="num">Amount</th></tr></thead>
      <tbody>${d.bills.map(b => `<tr>
        <td>${b.txn_number}</td><td>${b.vendor_name}</td><td>${fmtDate(b.txn_date)}</td>
        <td>${b.description||'—'}</td><td class="num">${fmtFull(b.line_amount)}</td>
      </tr>`).join('')}</tbody>
    </table></div>`;
}

async function drillPNL(titleEl, subEl, content) {
  const data = await api('/api/pnl' + qs());
  titleEl.textContent = 'Profit & Loss Statement';
  subEl.textContent = `${data.primary.from} to ${data.primary.to} (vs ${currentCompare.replace(/_/g,' ')})`;
  const p = data.primary, c = data.comparison;
  const del = (a, b) => b ? ((a-b)/Math.abs(b)*100) : 0;
  const row = (label, cur, prev, bold=false, indent=false) => {
    const d = del(cur, prev);
    const color = d > 0 ? 'var(--good)' : (d < 0 ? 'var(--bad)' : 'var(--muted)');
    const style = bold ? 'font-weight:700; background:var(--surface-2);' : '';
    const pad = indent ? 'padding-left:24px;' : '';
    return `<tr style="${style}">
      <td style="${pad}">${label}</td>
      <td class="num">${fmtFull(cur)}</td>
      <td class="num">${fmtFull(prev)}</td>
      <td class="num" style="color:${color};">${fmtPct(d)}</td>
    </tr>`;
  };
  const hasOther = (p.other_income || 0) > 0.01 || (c.other_income || 0) > 0.01;
  content.innerHTML = `
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Line Item</th><th class="num">Current</th><th class="num">Comparison</th><th class="num">Change</th></tr></thead>
      <tbody>
        ${row('REVENUE', p.total_revenue, c.total_revenue, true)}
        ${p.cogs_rows && p.cogs_rows.length ? `
          <tr style="background:var(--paper-2); font-weight:500;"><td colspan="4">Cost of Goods Sold</td></tr>
          ${p.cogs_rows.slice(0, 10).map(r => {
            const compRow = (c.cogs_rows || []).find(x => x.label === r.label);
            return row(r.label, r.amount, compRow ? compRow.amount : 0, false, true);
          }).join('')}
        ` : ''}
        ${row('Total Cost of Goods Sold', p.cogs_total, c.cogs_total, true)}
        ${row('GROSS PROFIT', p.gross_profit, c.gross_profit, true)}
        <tr style="background:var(--paper-2); font-weight:500;"><td colspan="4">Operating Expenses</td></tr>
        ${p.opex_rows.slice(0, 20).map(r => {
          const compRow = c.opex_rows.find(x => x.label === r.label);
          return row(r.label, r.amount, compRow ? compRow.amount : 0, false, true);
        }).join('')}
        ${row('Total Operating Expenses', p.opex_total, c.opex_total, true)}
        ${row('NET ORDINARY INCOME', p.net_ordinary_income, c.net_ordinary_income, true)}
        ${hasOther ? `
          <tr style="background:var(--paper-2); font-weight:500;"><td colspan="4">Other Income</td></tr>
          ${row('Other Income (Interest, etc.)', p.other_income, c.other_income, false, true)}
        ` : ''}
        ${row('NET INCOME', p.net_income, c.net_income, true)}
      </tbody>
    </table></div>`;
}

async function drillBS(titleEl, subEl, content) {
  const data = await api('/api/balance-sheet');
  titleEl.textContent = 'Balance Sheet'; subEl.textContent = 'As of ' + fmtDate(data.as_of);
  const section = (name, rows) => rows.length ? `
    <tr style="background:var(--paper-2); font-weight:500;"><td colspan="2">${name}</td></tr>
    ${rows.map(a => `<tr><td style="padding-left:24px;">${a.name}</td><td class="num">${fmtFull(a.balance)}</td></tr>`).join('')}
    <tr style="font-weight:500;"><td>Total ${name}</td><td class="num">${fmtFull(rows.reduce((a,b)=>a+b.balance,0))}</td></tr>` : '';
  content.innerHTML = `
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Account</th><th class="num">Amount</th></tr></thead>
      <tbody>
        ${section('Current Assets', data.sections['Current Assets'])}
        ${section('Fixed Assets', data.sections['Fixed Assets'])}
        ${section('Other Assets', data.sections['Other Assets'])}
        <tr style="font-weight:700; background:var(--accent-soft);"><td>TOTAL ASSETS</td><td class="num">${fmtFull(data.total_assets)}</td></tr>
        ${section('Current Liabilities', data.sections['Current Liabilities'])}
        ${section('Long-Term Liabilities', data.sections['Long-Term Liabilities'])}
        <tr style="font-weight:700;"><td>Total Liabilities</td><td class="num">${fmtFull(data.total_liabilities)}</td></tr>
        ${section('Equity', data.sections['Equity'])}
        <tr style="font-weight:700; background:var(--accent-soft);"><td>TOTAL LIAB + EQUITY</td><td class="num">${fmtFull(data.total_liab_equity)}</td></tr>
      </tbody>
    </table></div>`;
}

async function drillCF(titleEl, subEl, content) {
  const data = await api('/api/cash-flow' + qs());
  titleEl.textContent = 'Cash Flow';
  subEl.textContent = `${data.primary.from} to ${data.primary.to}`;
  const p = data.primary, c = data.comparison;
  const del = (a, b) => b ? ((a-b)/Math.abs(b)*100) : 0;
  const row = (label, cur, prev, bold=false, indent=false) => {
    const d = del(cur, prev);
    const color = d > 0 ? 'var(--good)' : (d < 0 ? 'var(--bad)' : 'var(--muted)');
    const style = bold ? 'font-weight:700; background:var(--accent-soft);' : '';
    const pad = indent ? 'padding-left:24px;' : '';
    return `<tr style="${style}">
      <td style="${pad}">${label}</td>
      <td class="num">${fmtFull(cur)}</td>
      <td class="num">${fmtFull(prev)}</td>
      <td class="num" style="color:${color};">${fmtPct(d)}</td></tr>`;
  };
  content.innerHTML = `
    <div class="table-wrap"><table class="data-table">
      <thead><tr><th>Line</th><th class="num">Current</th><th class="num">Comparison</th><th class="num">Change</th></tr></thead>
      <tbody>
        <tr style="background:var(--paper-2); font-weight:500;"><td colspan="4">Cash Inflow</td></tr>
        ${(p.inflow_breakdown || []).map(r => {
          const compRow = (c.inflow_breakdown || []).find(x => x.label === r.label);
          return row(r.label, r.amount, compRow ? compRow.amount : 0, false, true);
        }).join('')}
        ${row('Total Cash Inflow', p.inflow, c.inflow, true)}
        <tr style="background:var(--paper-2); font-weight:500;"><td colspan="4">Cash Outflow</td></tr>
        ${(p.outflow_breakdown || []).map(r => {
          const compRow = (c.outflow_breakdown || []).find(x => x.label === r.label);
          return row(r.label, r.amount, compRow ? compRow.amount : 0, false, true);
        }).join('')}
        ${row('Total Cash Outflow', p.outflow, c.outflow, true)}
        ${row('NET CASH FLOW', p.net_cash_flow, c.net_cash_flow, true)}
      </tbody>
    </table></div>`;
}

async function drillMonth(top, titleEl, subEl, content) {
  const [y, m] = top.payload.split('-');
  const from = `${y}-${m}-01`;
  const daysInMonth = new Date(parseInt(y), parseInt(m), 0).getDate();
  const to = `${y}-${m}-${String(daysInMonth).padStart(2,'0')}`;
  const data = await api(`/api/kpi?from=${from}&to=${to}&compare=${currentCompare}`);
  titleEl.textContent = top.label;
  subEl.textContent = `${from} to ${to}`;
  const p = data.primary;
  content.innerHTML = `
    <div class="drill-summary">
      <div class="drill-summary-item"><div class="meta-label">Revenue</div><div class="meta-value">${fmtFull(p.revenue)}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Expenses</div><div class="meta-value">${fmtFull(p.expenses)}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Net Income</div><div class="meta-value" style="color:${p.net_income>=0?'var(--good)':'var(--bad)'}">${fmtFull(p.net_income)}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Invoices</div><div class="meta-value">${p.invoices_count}</div></div>
      <div class="drill-summary-item"><div class="meta-label">Bills</div><div class="meta-value">${p.bills_count}</div></div>
    </div>
    <div style="padding:20px; color:var(--muted); font-size:13px; text-align:center;">Use breadcrumb above to return.</div>`;
}

// ============ Search ============
let searchTimer;
const searchBox = document.getElementById('global-search');
const searchResults = document.getElementById('search-results');

searchBox.addEventListener('input', (e) => {
  clearTimeout(searchTimer);
  const q = e.target.value.trim();
  if (q.length < 2) { searchResults.style.display = 'none'; return; }
  searchTimer = setTimeout(async () => {
    const results = await api('/api/search?q=' + encodeURIComponent(q));
    if (results.length === 0) {
      searchResults.innerHTML = '<div class="search-empty">No matches.</div>';
    } else {
      searchResults.innerHTML = results.map(r => `
        <div class="search-item" data-type="${r.type}" data-payload='${JSON.stringify(r.payload).replace(/'/g, "&#39;")}'>
          <div class="search-item-main">
            <strong>${r.title}</strong>
            <span class="tag tag-bill">${r.type_label}</span>
          </div>
          <div class="search-item-sub">${r.subtitle || ''} ${r.extra ? '· ' + r.extra : ''}</div>
        </div>`).join('');
      searchResults.querySelectorAll('.search-item').forEach(el => {
        el.addEventListener('click', () => {
          const type = el.dataset.type;
          const payload = JSON.parse(el.dataset.payload.replace(/&#39;/g, "'"));
          searchResults.style.display = 'none';
          searchBox.value = '';
          if (type === 'customer') drillTo({level:'customer', label: payload.customer, payload: payload.customer});
          else if (type === 'vendor') drillTo({level:'vendor', label: payload.vendor, payload: payload.vendor});
          else if (type === 'invoice') drillTo({level:'invoice', label: 'Invoice detail', payload: payload.invoice_id});
          else if (type === 'bill') drillTo({level:'bill', label: 'Bill detail', payload: payload.bill_id});
          else if (type === 'item') drillTo({level:'item', label: payload.item, payload: payload.item});
          else if (type === 'account') drillTo({level:'account', label: payload.account, payload: payload.account});
        });
      });
    }
    searchResults.style.display = 'block';
  }, 180);
});
document.addEventListener('click', (e) => {
  if (!e.target.closest('.search-bar')) searchResults.style.display = 'none';
});

// ============ Selectors ============
document.getElementById('period-select').addEventListener('change', (e) => {
  currentPreset = e.target.value;
  const wrap = document.getElementById('custom-range-wrap');
  if (currentPreset === 'custom') {
    wrap.style.display = 'flex';
    // Default custom range to current month if empty
    const fromEl = document.getElementById('custom-from');
    const toEl = document.getElementById('custom-to');
    if (!fromEl.value) {
      const t = new Date();
      fromEl.value = new Date(t.getFullYear(), t.getMonth(), 1).toISOString().slice(0, 10);
      toEl.value = t.toISOString().slice(0, 10);
    }
    customFrom = fromEl.value;
    customTo = toEl.value;
  } else {
    wrap.style.display = 'none';
  }
  refreshAll();
});

document.getElementById('custom-apply').addEventListener('click', () => {
  const fromEl = document.getElementById('custom-from');
  const toEl = document.getElementById('custom-to');
  if (!fromEl.value || !toEl.value) {
    alert('Please pick both a From and To date.');
    return;
  }
  if (fromEl.value > toEl.value) {
    alert('From date must be before or equal to To date.');
    return;
  }
  customFrom = fromEl.value;
  customTo = toEl.value;
  currentPreset = 'custom';
  refreshAll();
});

document.getElementById('compare-select').addEventListener('change', (e) => {
  currentCompare = e.target.value;
  refreshAll();
});

// Summary card clicks
document.querySelectorAll('.summary-card.clickable').forEach(card => {
  card.addEventListener('click', () => {
    const drill = JSON.parse(card.getAttribute('data-drill'));
    drillTo({level: drill.level, label: card.querySelector('.panel-title').textContent});
  });
});

// ============ Sync / status ============
async function refreshStatus() {
  const s = await api('/api/status');
  const pill = document.getElementById('connection-pill');
  const text = pill.querySelector('.pill-text');
  pill.classList.remove('live', 'demo', 'error');
  if (s.demo_mode) { pill.classList.add('demo'); text.textContent = 'Demo Mode'; }
  else if ((s.last_sync_status || '').toLowerCase().includes('error')) { pill.classList.add('error'); text.textContent = 'Sync Error'; }
  else { pill.classList.add('live'); text.textContent = 'Live · QuickBooks'; }
  const lastEl = document.getElementById('last-sync-text');
  if (lastEl && s.last_sync) {
    const d = new Date(s.last_sync);
    lastEl.textContent = 'Last sync: ' + d.toLocaleString() + ' · ' + (s.last_sync_status || '');
  }
}

document.getElementById('sync-button').addEventListener('click', async (e) => {
  const btn = e.currentTarget;
  const orig = btn.innerHTML;
  btn.innerHTML = 'Syncing…'; btn.disabled = true;
  try {
    await fetch('/api/sync-now', {method: 'POST'});
    await refreshAll();
  } finally {
    btn.innerHTML = orig; btn.disabled = false;
  }
});

async function refreshOverview() {
  await Promise.all([loadKPIs(), loadSummaries()]);
  await Promise.all([loadTrendChart(), loadAgingChart(), loadCustomersChart(), loadExpensesChart()]);
  await Promise.all([loadItems(), loadVendors(), loadRecent()]);
}

async function refreshAll() {
  await refreshStatus();
  if (drillStack.length === 0) await refreshOverview();
  else await renderDrill();
}

// ============ Boot ============
restoreFromHash();
refreshAll();
setInterval(refreshAll, 60000);

window.addEventListener('hashchange', () => {
  restoreFromHash();
  renderDrill();
});
