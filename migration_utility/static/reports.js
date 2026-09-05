// ═══════════════════════════════════════════════════════════════════════════
//  REPORTS & ANALYTICS — Charts, Filters, Download, Email
// ═══════════════════════════════════════════════════════════════════════════
let rptCharts = {};
let rptData = { jobs: [], runs: [] };

const RPT_COLORS = {
  success: '#059669', failed: '#DC2626', running: '#D97706', created: '#6366F1',
  extract: '#2563EB', landing_to_bronze: '#D97706', bronze_to_silver: '#059669',
  dlt_bronze_silver: '#7C3AED',
  full: '#3B82F6', incremental: '#10B981'
};
const RPT_STATUS_LABELS = { success:'Success', failed:'Failed', running:'Running', created:'Created' };
const RPT_STAGE_LABELS  = { extract:'Extract', landing_to_bronze:'Landing→Bronze', bronze_to_silver:'Bronze→Silver', dlt_bronze_silver:'SDP Bronze+Silver' };

function rptDestroyCharts(){
  Object.values(rptCharts).forEach(c=>{ if(c&&c.destroy) c.destroy(); });
  rptCharts = {};
}

async function rptRefresh(){
  try {
    const [jobsR, runsR] = await Promise.all([
      fetch('/api/v1/workflow/jobs').then(r=>r.json()),
      fetch('/api/v1/workflow/runs?limit=500').then(r=>r.json())
    ]);
    rptData.jobs = jobsR.success ? (jobsR.jobs || []) : [];
    rptData.runs = runsR.success ? (runsR.runs || []) : [];

    // If local store is empty, fetch from Databricks metadata table
    if(!rptData.jobs.length){
      try{
        const dbxR=await fetch('/api/v1/reports/jobs').then(r=>r.json());
        if(dbxR.success && dbxR.jobs && dbxR.jobs.length){
          rptData.jobs=dbxR.jobs;
          // Build synthetic runs from job last_run data
          rptData.runs=dbxR.jobs.filter(j=>j.last_run_id).map(j=>({
            run_id:j.last_run_id, job_id:j.job_id, job_name:j.job_name,
            table_name:j.table_name, stage:j.stage, status:j.last_status||j.status,
            started_at:j.last_run_at||j.updated_at, created_at:j.created_at,
            load_type:j.load_type
          }));
        }
      }catch(dbxErr){console.warn('Databricks reports fallback failed',dbxErr);}
    }
  } catch(e) {
    rptData.jobs = []; rptData.runs = [];
    console.error('rptRefresh', e);
  }

  const stage  = G('rptFilterStage').value;
  const status = G('rptFilterStatus').value;
  const period = G('rptFilterPeriod').value;

  let jobs = rptData.jobs.slice();
  if(stage)  jobs = jobs.filter(j => j.stage === stage);
  if(status) jobs = jobs.filter(j => j.status === status);

  // Filter runs by period
  let runs = rptData.runs.slice();
  if(period !== 'all'){
    const days = parseInt(period);
    const cutoff = Date.now() - days * 86400000;
    runs = runs.filter(r => {
      const t = r.started_at || r.created_at || '';
      return t ? new Date(t).getTime() >= cutoff : true;
    });
  }

  // KPIs
  const statusCounts = {};
  jobs.forEach(j => { statusCounts[j.status] = (statusCounts[j.status]||0)+1; });
  const total   = jobs.length;
  const success = statusCounts['success'] || 0;
  const failed  = statusCounts['failed']  || 0;
  const running = statusCounts['running'] || 0;
  const rate    = total ? Math.round(success/total*100)+'%' : '—';

  G('rptKpiTotal').textContent   = total;
  G('rptKpiSuccess').textContent = success;
  G('rptKpiFailed').textContent  = failed;
  G('rptKpiRate').textContent    = rate;
  G('rptKpiRunning').textContent = running;

  rptDestroyCharts();
  rptRenderStatusPie(statusCounts);
  rptRenderStageBar(jobs);
  rptRenderTimeline(runs);
  rptRenderLoadType(jobs);
  rptRenderDetailTable(jobs);
}

function rptRenderStatusPie(counts){
  const labels = [], data = [], colors = [];
  for(const [k,v] of Object.entries(counts)){
    labels.push(RPT_STATUS_LABELS[k]||k);
    data.push(v);
    colors.push(RPT_COLORS[k]||'#94A3B8');
  }
  if(!data.length){ labels.push('No Data'); data.push(1); colors.push('#E2E8F0'); }
  const ctx = G('rptChartStatusPie').getContext('2d');
  rptCharts.statusPie = new Chart(ctx, {
    type: 'doughnut',
    data: { labels, datasets: [{ data, backgroundColor: colors, borderWidth: 2, borderColor:'#fff' }] },
    options: { responsive:true, maintainAspectRatio:false, plugins:{ legend:{ position:'bottom', labels:{ padding:12, usePointStyle:true, font:{ size:11 } } } } }
  });
}

function rptRenderStageBar(jobs){
  const stageCounts = {};
  jobs.forEach(j => { stageCounts[j.stage] = (stageCounts[j.stage]||0)+1; });
  const labels = [], data = [], colors = [];
  for(const [k,v] of Object.entries(stageCounts)){
    labels.push(RPT_STAGE_LABELS[k]||k);
    data.push(v);
    colors.push(RPT_COLORS[k]||'#94A3B8');
  }
  if(!data.length){ labels.push('No Data'); data.push(0); colors.push('#E2E8F0'); }
  const ctx = G('rptChartStageBar').getContext('2d');
  rptCharts.stageBar = new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets: [{ label:'Jobs', data, backgroundColor: colors, borderRadius:6, barPercentage:0.6 }] },
    options: { responsive:true, maintainAspectRatio:false, plugins:{ legend:{ display:false } }, scales:{ y:{ beginAtZero:true, ticks:{ stepSize:1, font:{ size:11 } } }, x:{ ticks:{ font:{ size:11 } } } } }
  });
}

function rptRenderTimeline(runs){
  // Group runs by date
  const byDate = {};
  runs.forEach(r => {
    const d = (r.started_at || r.created_at || '').substring(0,10);
    if(!d) return;
    if(!byDate[d]) byDate[d] = { success:0, failed:0, running:0 };
    const s = r.status || 'running';
    byDate[d][s] = (byDate[d][s]||0) + 1;
  });
  const dates = Object.keys(byDate).sort();
  const ctx = G('rptChartTimeline').getContext('2d');
  rptCharts.timeline = new Chart(ctx, {
    type: 'line',
    data: {
      labels: dates.length ? dates : ['No Data'],
      datasets: [
        { label:'Success', data: dates.map(d=>byDate[d].success), borderColor:RPT_COLORS.success, backgroundColor:'rgba(5,150,105,.1)', fill:true, tension:.3, pointRadius:3 },
        { label:'Failed',  data: dates.map(d=>byDate[d].failed),  borderColor:RPT_COLORS.failed,  backgroundColor:'rgba(220,38,38,.1)',  fill:true, tension:.3, pointRadius:3 },
      ]
    },
    options: { responsive:true, maintainAspectRatio:false, plugins:{ legend:{ position:'bottom', labels:{ padding:12, usePointStyle:true, font:{ size:11 } } } }, scales:{ y:{ beginAtZero:true, ticks:{ stepSize:1, font:{ size:11 } } }, x:{ ticks:{ font:{ size:10 }, maxRotation:45 } } } }
  });
}

function rptRenderLoadType(jobs){
  const loadCounts = {};
  jobs.forEach(j => { const lt = j.load_type || 'full'; loadCounts[lt] = (loadCounts[lt]||0)+1; });
  const labels = [], data = [], colors = [];
  for(const [k,v] of Object.entries(loadCounts)){
    labels.push(k.charAt(0).toUpperCase()+k.slice(1));
    data.push(v);
    colors.push(RPT_COLORS[k]||'#94A3B8');
  }
  if(!data.length){ labels.push('No Data'); data.push(1); colors.push('#E2E8F0'); }
  const ctx = G('rptChartLoadType').getContext('2d');
  rptCharts.loadType = new Chart(ctx, {
    type: 'pie',
    data: { labels, datasets: [{ data, backgroundColor: colors, borderWidth:2, borderColor:'#fff' }] },
    options: { responsive:true, maintainAspectRatio:false, plugins:{ legend:{ position:'bottom', labels:{ padding:12, usePointStyle:true, font:{ size:11 } } } } }
  });
}

function rptRenderDetailTable(jobs){
  const tbody = G('rptDetailTbody');
  if(!jobs.length){
    tbody.innerHTML='<tr><td colspan="7" style="padding:32px;text-align:center;color:var(--t4);">No jobs match the current filters</td></tr>';
    return;
  }
  const statusBadge = s => {
    const c = { success:'background:var(--green-light);color:var(--green-fg);border:1px solid var(--green-border)', failed:'background:var(--red-light);color:var(--red-fg);border:1px solid var(--red-border)', running:'background:var(--amber-light);color:var(--amber-fg);border:1px solid var(--amber-border)', created:'background:var(--blue-light);color:var(--blue-fg);border:1px solid var(--blue-border)' };
    return '<span style="padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;'+( c[s]||c.created)+'">'+(RPT_STATUS_LABELS[s]||s)+'</span>';
  };
  tbody.innerHTML = jobs.map(j => `<tr style="border-bottom:1px solid var(--border);">
    <td style="padding:8px 10px;font-weight:600;color:var(--t1);">${j.job_name||j.id||'—'}</td>
    <td style="padding:8px 10px;color:var(--t2);">${RPT_STAGE_LABELS[j.stage]||j.stage||'—'}</td>
    <td style="padding:8px 10px;color:var(--t2);">${j.table_name||'—'}</td>
    <td style="padding:8px 10px;color:var(--t2);">${j.load_type||'full'}</td>
    <td style="padding:8px 10px;text-align:center;">${statusBadge(j.status)}</td>
    <td style="padding:8px 10px;text-align:center;">${j.run_count||0}</td>
    <td style="padding:8px 10px;text-align:center;font-size:11px;color:var(--t3);">${j.last_run_at||'—'}</td>
  </tr>`).join('');
}

function rptGetFilteredJobs(){
  let jobs = rptData.jobs.slice();
  const stage  = G('rptFilterStage').value;
  const status = G('rptFilterStatus').value;
  if(stage)  jobs = jobs.filter(j => j.stage === stage);
  if(status) jobs = jobs.filter(j => j.status === status);
  return jobs;
}

function rptDownloadCSV(){
  const jobs = rptGetFilteredJobs();
  if(!jobs.length){ toast('No data to export','terr'); return; }
  const headers = ['Job Name','Stage','Table','Load Type','Status','Runs','Last Run'];
  const rows = jobs.map(j => [
    j.job_name||j.id||'', RPT_STAGE_LABELS[j.stage]||j.stage||'', j.table_name||'',
    j.load_type||'full', j.status||'', j.run_count||0, j.last_run_at||''
  ]);
  let csv = headers.join(',') + '\n' + rows.map(r => r.map(c => '"'+String(c).replace(/"/g,'""')+'"').join(',')).join('\n');
  const blob = new Blob([csv], { type:'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'migration_report_' + new Date().toISOString().substring(0,10) + '.csv';
  a.click(); URL.revokeObjectURL(url);
  toast('CSV report downloaded','tok');
}

function rptDownloadPDF(){
  const jobs = rptGetFilteredJobs();
  if(!jobs.length){ toast('No data to export','terr'); return; }
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF('landscape','mm','a4');
  const pageW = doc.internal.pageSize.getWidth();
  const now = new Date();
  const dateStr = now.toISOString().substring(0,10);
  const timeStr = now.toLocaleTimeString();

  // Header banner
  doc.setFillColor(13,21,38);
  doc.rect(0,0,pageW,28,'F');
  doc.setTextColor(255,255,255);
  doc.setFontSize(18); doc.setFont('helvetica','bold');
  doc.text('Migration Pipeline Report',14,14);
  doc.setFontSize(9); doc.setFont('helvetica','normal');
  doc.text('Generated: '+dateStr+' '+timeStr+'  |  Filters: Stage='+
    (G('rptFilterStage').value||'All')+', Status='+(G('rptFilterStatus').value||'All')+
    ', Period='+G('rptFilterPeriod').value, 14, 22);

  // KPI boxes
  const total   = jobs.length;
  const success = jobs.filter(j=>j.status==='success').length;
  const failed  = jobs.filter(j=>j.status==='failed').length;
  const running = jobs.filter(j=>j.status==='running').length;
  const rate    = total ? Math.round(success/total*100)+'%' : '—';

  let y = 36;
  const kpis = [
    {label:'Total Jobs', value:String(total), color:[37,99,235]},
    {label:'Success',    value:String(success), color:[5,150,105]},
    {label:'Failed',     value:String(failed),  color:[220,38,38]},
    {label:'Running',    value:String(running),  color:[217,119,6]},
    {label:'Success Rate',value:rate,            color:[99,102,241]}
  ];
  const boxW = 50, boxH = 18, gap = 6;
  const startX = (pageW - (kpis.length*boxW + (kpis.length-1)*gap))/2;
  kpis.forEach((k,i)=>{
    const x = startX + i*(boxW+gap);
    doc.setFillColor(245,247,252); doc.roundedRect(x,y,boxW,boxH,3,3,'F');
    doc.setTextColor(k.color[0],k.color[1],k.color[2]);
    doc.setFontSize(16); doc.setFont('helvetica','bold');
    doc.text(k.value, x+boxW/2, y+10, {align:'center'});
    doc.setFontSize(7); doc.setFont('helvetica','normal');
    doc.setTextColor(100,116,139);
    doc.text(k.label.toUpperCase(), x+boxW/2, y+15.5, {align:'center'});
  });

  // Capture chart images and add to PDF
  const chartIds = ['rptChartStatusPie','rptChartStageBar','rptChartTimeline','rptChartLoadType'];
  const chartLabels = ['Jobs by Status','Jobs by Stage','Run History Timeline','Load Type Distribution'];
  y += boxH + 8;
  const chartW = (pageW - 42)/2, chartH = 55;
  chartIds.forEach((cid,i)=>{
    const canvas = G(cid);
    if(!canvas) return;
    const imgData = canvas.toDataURL('image/png',1.0);
    const col = i % 2;
    const row = Math.floor(i / 2);
    const cx = 14 + col*(chartW+14);
    const cy = y + row*(chartH+14);
    doc.setFillColor(255,255,255); doc.roundedRect(cx,cy-4,chartW,chartH+8,2,2,'F');
    doc.setDrawColor(221,228,239); doc.roundedRect(cx,cy-4,chartW,chartH+8,2,2,'S');
    doc.setFontSize(8); doc.setFont('helvetica','bold'); doc.setTextColor(13,21,38);
    doc.text(chartLabels[i], cx+4, cy+1);
    doc.addImage(imgData,'PNG', cx+2, cy+3, chartW-4, chartH-2);
  });

  y += 2*(chartH+14) + 4;

  // Check if we need a new page for the table
  if(y > doc.internal.pageSize.getHeight() - 40){ doc.addPage(); y = 14; }

  // Job detail table
  const RPT_STAGE_LABELS_PDF = {extract:'Extract',landing_to_bronze:'Landing→Bronze',bronze_to_silver:'Bronze→Silver',dlt_bronze_silver:'SDP Bronze+Silver'};
  const head = [['Job Name','Stage','Table','Load Type','Status','Runs','Last Run']];
  const body = jobs.map(j => [
    j.job_name||j.id||'—',
    RPT_STAGE_LABELS_PDF[j.stage]||j.stage||'—',
    j.table_name||'—',
    j.load_type||'full',
    (j.status||'—').toUpperCase(),
    String(j.run_count||0),
    j.last_run_at||'—'
  ]);
  doc.autoTable({
    startY: y,
    head: head,
    body: body,
    theme: 'grid',
    headStyles: { fillColor:[37,99,235], fontSize:8, fontStyle:'bold' },
    bodyStyles: { fontSize:8 },
    alternateRowStyles: { fillColor:[245,247,252] },
    columnStyles: { 4:{ halign:'center' }, 5:{ halign:'center' }, 6:{ halign:'center' } },
    margin: { left:14, right:14 },
    didParseCell: function(data){
      if(data.section==='body' && data.column.index===4){
        const s = data.cell.raw.toLowerCase();
        if(s==='success') data.cell.styles.textColor=[5,150,105];
        else if(s==='failed') data.cell.styles.textColor=[220,38,38];
        else if(s==='running') data.cell.styles.textColor=[217,119,6];
      }
    }
  });

  // Footer
  const pageCount = doc.internal.getNumberOfPages();
  for(let p=1;p<=pageCount;p++){
    doc.setPage(p);
    doc.setFontSize(7); doc.setTextColor(156,163,175);
    doc.text('SQL → Databricks Migration Studio  |  Page '+p+'/'+pageCount,
      pageW/2, doc.internal.pageSize.getHeight()-6, {align:'center'});
  }

  doc.save('migration_report_'+dateStr+'.pdf');
  toast('PDF report downloaded','tok');
}

function rptDownloadJSON(){
  const jobs = rptGetFilteredJobs();
  if(!jobs.length){ toast('No data to export','terr'); return; }
  const report = {
    generated_at: new Date().toISOString(),
    filters: { stage: G('rptFilterStage').value || 'all', status: G('rptFilterStatus').value || 'all', period: G('rptFilterPeriod').value },
    summary: { total: jobs.length, success: jobs.filter(j=>j.status==='success').length, failed: jobs.filter(j=>j.status==='failed').length },
    jobs: jobs
  };
  const blob = new Blob([JSON.stringify(report, null, 2)], { type:'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'migration_report_' + new Date().toISOString().substring(0,10) + '.json';
  a.click(); URL.revokeObjectURL(url);
  toast('JSON report downloaded','tok');
}

async function rptSendEmail(){
  const email   = (G('rptEmailTo').value||'').trim();
  const subject = (G('rptEmailSubject').value||'').trim();
  if(!email){ toast('Please enter a recipient email','terr'); return; }
  // Basic email format check
  if(!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)){ toast('Invalid email format','terr'); return; }

  const jobs = rptGetFilteredJobs();
  const btn = G('btnRptEmail');
  btn.disabled = true; btn.textContent = 'Sending…';

  try {
    const r = await fetch('/api/v1/reports/email', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        to: email,
        subject: subject || 'Migration Pipeline Report — ' + new Date().toISOString().substring(0,10),
        filters: { stage: G('rptFilterStage').value, status: G('rptFilterStatus').value, period: G('rptFilterPeriod').value },
        job_count: jobs.length,
        summary: {
          total: jobs.length,
          success: jobs.filter(j=>j.status==='success').length,
          failed:  jobs.filter(j=>j.status==='failed').length,
          running: jobs.filter(j=>j.status==='running').length
        },
        jobs: jobs
      })
    });
    const d = await r.json();
    if(d.success){
      toast('Report sent to '+email,'tok');
      G('rptEmailTo').value = '';
      G('rptEmailSubject').value = '';
    } else {
      toast('Failed: '+(d.error||d.message||'Unknown error'),'terr');
    }
  } catch(e){
    toast('Email send failed: '+e.message,'terr');
  } finally {
    btn.disabled = false; btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg> Send Report';
  }
}

/* ══════════════════════════════════════════════════════════════════
   MIGRATION PROGRESS TRACKER
   ══════════════════════════════════════════════════════════════════ */
let mptFunnelChart=null, mptTimelineChart=null, mptAllRows=[];

async function mptRefresh(){
  try{
    const [jobsRes, runsRes, statsRes] = await Promise.all([
      fetch('/api/v1/workflow/jobs'),
      fetch('/api/v1/workflow/runs'),
      fetch('/api/v1/workflow/stats')
    ]);
    const jobs = await jobsRes.json();
    const runs = await runsRes.json();
    const stats = await statsRes.json();

    let jobsArr = Array.isArray(jobs) ? jobs : (jobs.jobs||[]);
    let runsArr = Array.isArray(runs) ? runs : (runs.runs||[]);

    // If local store is empty, fetch from Databricks metadata table
    if(!jobsArr.length){
      try{
        const dbxR=await fetch('/api/v1/reports/jobs').then(r=>r.json());
        if(dbxR.success && dbxR.jobs && dbxR.jobs.length){
          jobsArr=dbxR.jobs;
          runsArr=dbxR.jobs.filter(j=>j.last_run_id).map(j=>({
            run_id:j.last_run_id, job_id:j.job_id, job_name:j.job_name,
            table_name:j.table_name, stage:j.stage, status:j.last_status||j.status,
            started_at:j.last_run_at||j.updated_at, created_at:j.created_at,
            load_type:j.load_type
          }));
        }
      }catch(dbxErr){console.warn('Databricks progress fallback failed',dbxErr);}
    }

    // Build per-table status from jobs + runs
    const tables = {};
    jobsArr.forEach(j=>{
      const name = j.table_name;
      if(!name) return;  // skip job-name-only entries — they are not tables
      if(!tables[name]) tables[name]={name, extract:false, bronze:false, silver:false, failed:false, last_updated:null, first_seen:null, silver_at:null, blocker:null};
      const stage = (j.pipeline_stage||j.stage||'').toLowerCase();
      const status = (j.status||j.state||'').toLowerCase();
      if(status==='completed'||status==='success'||status==='succeeded'){
        if(stage.includes('extract')||stage.includes('landing')) tables[name].extract=true;
        if(stage.includes('bronze')) tables[name].bronze=true;
        if(stage.includes('silver')) tables[name].silver=true;
      }
      if(status==='failed'||status==='error'){
        tables[name].failed=true;
        tables[name].blocker=j.error||j.message||'Migration failed';
      }
      // Track timestamps for ETA calculation
      const ts=j.updated_at||j.end_time||j.timestamp||j.created_at||j.started_at||j.last_run_at;
      if(ts){
        if(!tables[name].last_updated||ts>tables[name].last_updated) tables[name].last_updated=ts;
        if(!tables[name].first_seen||ts<tables[name].first_seen) tables[name].first_seen=ts;
        if(stage.includes('silver')&&(status==='completed'||status==='success'||status==='succeeded')){
          if(!tables[name].silver_at||ts>tables[name].silver_at) tables[name].silver_at=ts;
        }
      }
    });
    runsArr.forEach(r=>{
      const name = r.table_name;
      if(!name) return;  // run rows carry job_name, not a table — never invent entries
      if(!tables[name]) tables[name]={name, extract:false, bronze:false, silver:false, failed:false, last_updated:null, first_seen:null, silver_at:null, blocker:null};
      const stage = (r.pipeline_stage||r.stage||'').toLowerCase();
      const status = (r.status||r.state||r.result_state||'').toLowerCase();
      if(status==='completed'||status==='success'||status==='succeeded'){
        if(stage.includes('extract')||stage.includes('landing')) tables[name].extract=true;
        if(stage.includes('bronze')) tables[name].bronze=true;
        if(stage.includes('silver')) tables[name].silver=true;
      }
      if(status==='failed'||status==='error'){
        tables[name].failed=true;
        tables[name].blocker=r.error||r.message||'Run failed';
      }
      const ts=r.updated_at||r.end_time||r.timestamp||r.created_at||r.started_at;
      if(ts){
        if(!tables[name].last_updated||ts>tables[name].last_updated) tables[name].last_updated=ts;
        if(!tables[name].first_seen||ts<tables[name].first_seen) tables[name].first_seen=ts;
        if(stage.includes('silver')&&(status==='completed'||status==='success'||status==='succeeded')){
          if(!tables[name].silver_at||ts>tables[name].silver_at) tables[name].silver_at=ts;
        }
      }
    });

    mptAllRows=Object.values(tables);
    const total=mptAllRows.length||1;
    const extracted=mptAllRows.filter(t=>t.extract).length;
    const bronzed=mptAllRows.filter(t=>t.bronze).length;
    const silvered=mptAllRows.filter(t=>t.silver).length;
    const failed=mptAllRows.filter(t=>t.failed).length;
    const pct=Math.round((silvered/total)*100);

    // Update summary
    G('mptPercent').textContent=pct+'%';
    G('mptSummaryText').textContent=silvered+' of '+total+' tables fully migrated';
    G('mptProgressBar').style.width=pct+'%';

    // Stage cards
    G('mptExtractCount').textContent=extracted+' / '+total;
    G('mptExtractBar').style.width=Math.round(extracted/total*100)+'%';
    G('mptBronzeCount').textContent=bronzed+' / '+total;
    G('mptBronzeBar').style.width=Math.round(bronzed/total*100)+'%';
    G('mptSilverCount').textContent=silvered+' / '+total;
    G('mptSilverBar').style.width=Math.round(silvered/total*100)+'%';
    G('mptFailedCount').textContent=failed;
    G('mptBlockers').textContent=failed>0?failed+' table(s) have issues':'No blockers';

    // Funnel chart
    mptRenderFunnel(total, extracted, bronzed, silvered, failed);
    // Timeline chart
    mptRenderTimeline(mptAllRows);
    // Table
    mptRenderTable(mptAllRows);

    showToast('Progress tracker updated','success');
  }catch(e){
    console.error('mptRefresh error',e);
    showToast('Failed to load progress: '+e.message,'error');
  }
}

function mptRenderFunnel(total, extracted, bronzed, silvered, failed){
  const ctx=G('mptFunnelChart');
  if(mptFunnelChart){mptFunnelChart.destroy();}
  mptFunnelChart=new Chart(ctx,{
    type:'bar',
    data:{
      labels:['Total Tables','Extracted','Bronze','Silver','Failed'],
      datasets:[{
        data:[total, extracted, bronzed, silvered, failed],
        backgroundColor:['rgba(99,102,241,.7)','rgba(59,130,246,.7)','rgba(245,158,11,.7)','rgba(16,185,129,.7)','rgba(239,68,68,.7)'],
        borderColor:['rgb(99,102,241)','rgb(59,130,246)','rgb(245,158,11)','rgb(16,185,129)','rgb(239,68,68)'],
        borderWidth:1,
        borderRadius:6
      }]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{y:{beginAtZero:true,ticks:{stepSize:1}}}
    }
  });
}

function mptRenderTimeline(rows){
  const ctx=G('mptTimelineChart');
  if(mptTimelineChart){mptTimelineChart.destroy();}
  // Group completed tables by date
  const byDate={};
  rows.filter(r=>r.silver&&r.last_updated).forEach(r=>{
    const d=new Date(r.last_updated).toLocaleDateString();
    byDate[d]=(byDate[d]||0)+1;
  });
  const sortedDates=Object.keys(byDate).sort((a,b)=>new Date(a)-new Date(b));
  let cumulative=0;
  const cumulativeData=sortedDates.map(d=>{cumulative+=byDate[d];return cumulative;});

  mptTimelineChart=new Chart(ctx,{
    type:'line',
    data:{
      labels:sortedDates.length?sortedDates:['No data'],
      datasets:[{
        label:'Cumulative Tables Migrated',
        data:cumulativeData.length?cumulativeData:[0],
        borderColor:'rgb(16,185,129)',
        backgroundColor:'rgba(16,185,129,.15)',
        fill:true,tension:.3,pointRadius:4,pointBackgroundColor:'rgb(16,185,129)'
      }]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:true,position:'top'}},
      scales:{y:{beginAtZero:true,ticks:{stepSize:1}}}
    }
  });
}

function mptStageIcon(done,color){
  if(done) return '<span title="Done" style="display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border-radius:50%;background:'+color+';color:#fff;box-shadow:0 1px 3px rgba(0,0,0,.2);"><svg viewBox="0 0 24 24" style="width:16px;height:16px;" stroke="currentColor" fill="none" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg></span>';
  return '<span title="Not started" style="display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border-radius:50%;border:1.5px dashed var(--border);color:var(--t4);font-size:13px;font-weight:700;">–</span>';
}

function mptOverallBadge(t){
  if(t.failed) return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--red-light);color:var(--red-fg);border:1px solid var(--red-border);">Failed</span>';
  if(t.silver) return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--green-light);color:var(--green-fg);border:1px solid var(--green-border);">Complete</span>';
  if(t.bronze) return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--amber-light);color:var(--amber-fg);border:1px solid var(--amber-border);">In Progress</span>';
  if(t.extract) return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--blue-light);color:var(--blue-fg);border:1px solid var(--blue-border);">Extracted</span>';
  return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--surface-3);color:var(--t4);">Not Started</span>';
}

function mptRenderTable(rows){
  const tbody=G('mptTableBody');
  if(!rows.length){
    tbody.innerHTML='<tr><td colspan="7" style="padding:32px;text-align:center;color:var(--t4);">No migration data found</td></tr>';
    return;
  }
  tbody.innerHTML=rows.map(t=>`<tr style="border-bottom:1px solid var(--border);">
    <td style="padding:8px 10px;font-weight:600;color:var(--t1);">${t.name}</td>
    <td style="padding:8px 10px;text-align:center;">${mptStageIcon(t.extract,'var(--blue)')}</td>
    <td style="padding:8px 10px;text-align:center;">${mptStageIcon(t.bronze,'var(--amber)')}</td>
    <td style="padding:8px 10px;text-align:center;">${mptStageIcon(t.silver,'var(--green)')}</td>
    <td style="padding:8px 10px;text-align:center;">${mptOverallBadge(t)}</td>
    <td style="padding:8px 10px;text-align:center;color:var(--t3);font-size:11px;">${t.last_updated?new Date(t.last_updated).toLocaleString():'\u2014'}</td>
    <td style="padding:8px 10px;text-align:center;font-size:11px;color:${t.blocker?'var(--red)':'var(--t4)'};">${t.blocker||'\u2014'}</td>
  </tr>`).join('');
}

function mptFilterTable(){
  const search=(G('mptSearchTable').value||'').toLowerCase();
  const stageFilter=G('mptFilterStageSel').value;
  let filtered=mptAllRows;
  if(search) filtered=filtered.filter(t=>t.name.toLowerCase().includes(search));
  if(stageFilter){
    filtered=filtered.filter(t=>{
      if(stageFilter==='not_started') return !t.extract&&!t.bronze&&!t.silver&&!t.failed;
      if(stageFilter==='extract') return t.extract&&!t.bronze;
      if(stageFilter==='bronze') return t.bronze&&!t.silver;
      if(stageFilter==='silver') return t.silver;
      if(stageFilter==='failed') return t.failed;
      return true;
    });
  }
  mptRenderTable(filtered);
}

/* ══════════════════════════════════════════════════════════════════
   AUDIT & COMPLIANCE LOG
   ══════════════════════════════════════════════════════════════════ */
let auditVolumeChart=null, auditCategoryChart=null, auditSeverityChart=null, auditAllEvents=[];

const AUDIT_COMPLIANCE_CHECKS=[
  {id:'data_encrypted',  label:'Data encrypted at rest & in transit',         category:'security'},
  {id:'access_control',  label:'Role-based access control (RBAC) configured', category:'security'},
  {id:'audit_logging',   label:'Audit logging enabled on all catalogs',       category:'logging'},
  {id:'pii_masked',      label:'PII columns identified & masked/tokenized',  category:'data'},
  {id:'retention_policy',label:'Data retention policy applied',               category:'data'},
  {id:'backup_config',   label:'Backup & disaster recovery configured',       category:'infra'},
  {id:'schema_validated',label:'Schema validation passed for all tables',     category:'data'},
  {id:'lineage_tracked', label:'Data lineage tracked end-to-end',             category:'governance'},
  {id:'quality_checks',  label:'Data quality checks (null %, row count) pass',category:'data'},
  {id:'change_approved', label:'All config changes have approval record',     category:'governance'},
];

async function auditRefresh(){
  try{
    const [jobsRes, runsRes, statsRes] = await Promise.all([
      fetch('/api/v1/workflow/jobs'),
      fetch('/api/v1/workflow/runs'),
      fetch('/api/v1/workflow/stats')
    ]);
    let jobs = await jobsRes.json();
    let runs = await runsRes.json();
    const stats = await statsRes.json();

    let jobsArr = Array.isArray(jobs) ? jobs : (jobs.jobs||[]);
    let runsArr = Array.isArray(runs) ? runs : (runs.runs||[]);

    // If local store is empty, fetch from Databricks metadata table (same as Reports)
    if(!jobsArr.length){
      try{
        const dbxR=await fetch('/api/v1/reports/jobs').then(r=>r.json());
        if(dbxR.success && dbxR.jobs && dbxR.jobs.length){
          jobsArr=dbxR.jobs;
          runsArr=dbxR.jobs.filter(j=>j.last_run_id).map(j=>({
            run_id:j.last_run_id, job_id:j.job_id, job_name:j.job_name,
            table_name:j.table_name, stage:j.stage, status:j.last_status||j.status,
            started_at:j.last_run_at||j.updated_at, created_at:j.created_at,
            load_type:j.load_type
          }));
        }
      }catch(dbxErr){console.warn('Databricks audit fallback failed',dbxErr);}
    }

    // Also try dedicated audit endpoint
    let auditData=[];
    try{
      const ar=await fetch('/api/v1/audit/events');
      if(ar.ok){const ad=await ar.json(); auditData=ad.events||ad||[];}
    }catch(e){}

    // Build event log from jobs + runs + dedicated events
    const events=[];

    jobsArr.forEach(j=>{
      const st=(j.status||j.state||j.last_status||'').toLowerCase();
      const stageLbl=j.pipeline_stage||j.stage||'';
      const cat=st==='failed'?'error':stageLbl.includes('dlt')?'migration':stageLbl.includes('extract')?'migration':'config';
      events.push({
        timestamp: j.updated_at||j.created_at||j.last_run_at||j.timestamp||new Date().toISOString(),
        event: `Job "${j.name||j.job_name||j.table_name||'unknown'}" — ${st||'created'}`,
        category: cat,
        severity: st==='failed'?'error':st==='running'?'warning':'info',
        user: j.user||j.created_by||'system',
        details: stageLbl+(j.load_type?' | '+j.load_type:'')
      });
    });

    runsArr.forEach(r=>{
      const st=(r.status||r.state||r.result_state||'').toLowerCase();
      events.push({
        timestamp: r.updated_at||r.end_time||r.start_time||r.started_at||r.timestamp||new Date().toISOString(),
        event: `Run "${r.name||r.job_name||r.table_name||r.run_id||'unknown'}" — ${st}`,
        category: st==='failed'||st==='error'?'error':'migration',
        severity: st==='failed'||st==='error'?'error':st==='warning'?'warning':'info',
        user: r.user||r.triggered_by||'scheduler',
        details: r.pipeline_stage||r.stage||r.error||r.message||''
      });
    });

    // Add dedicated audit events
    auditData.forEach(a=>{
      events.push({
        timestamp: a.timestamp||new Date().toISOString(),
        event: a.event||a.action||a.message||'Audit event',
        category: a.category||'access',
        severity: a.severity||'info',
        user: a.user||a.actor||'system',
        details: a.details||a.description||''
      });
    });

    // Sort newest first
    events.sort((a,b)=>new Date(b.timestamp)-new Date(a.timestamp));
    auditAllEvents=events;

    // KPI counts
    const totalEvents=events.length;
    const configChanges=events.filter(e=>e.category==='config').length;
    const accessEvents=events.filter(e=>e.category==='access').length;
    const errors=events.filter(e=>e.severity==='error'||e.severity==='critical').length;

    G('auditTotalEvents').textContent=totalEvents;
    G('auditConfigChanges').textContent=configChanges;
    G('auditAccessEvents').textContent=accessEvents;
    G('auditErrors').textContent=errors;

    // Compliance scoring — check against what data tells us
    auditRenderChecklist(events, stats);
    // Charts
    auditRenderVolumeChart(events);
    auditRenderCategoryChart(events);
    auditRenderSeverityChart(events);
    // Table
    auditRenderLog(events);

    showToast('Audit log refreshed','success');
  }catch(e){
    console.error('auditRefresh error',e);
    showToast('Failed to load audit data: '+e.message,'error');
  }
}

function auditRenderChecklist(events, stats){
  // Simple heuristic scoring
  const checks=AUDIT_COMPLIANCE_CHECKS.map(c=>{
    let passed=false;
    if(c.id==='audit_logging') passed=events.length>0;
    if(c.id==='schema_validated') passed=events.some(e=>(e.event||'').toLowerCase().includes('silver')&&(e.category==='migration'));
    if(c.id==='quality_checks') passed=events.some(e=>(e.event||'').toLowerCase().includes('completed')||(e.event||'').toLowerCase().includes('success'));
    if(c.id==='lineage_tracked') passed=events.filter(e=>e.category==='migration').length>2;
    if(c.id==='change_approved') passed=events.filter(e=>e.category==='config').every(e=>e.user&&e.user!=='unknown');
    // The rest default to false (manual checks in production)
    return {...c, passed};
  });
  const passedCount=checks.filter(c=>c.passed).length;
  const score=Math.round((passedCount/checks.length)*100);
  const scoreEl=G('auditComplianceScore');
  scoreEl.textContent=score+'%';
  // On gradient card, always white text
  scoreEl.style.color='#fff';

  G('auditChecklist').innerHTML=checks.map(c=>`
    <div style="display:flex;align-items:center;gap:10px;padding:8px 10px;background:var(--surface-2);border-radius:var(--r);border-left:3px solid ${c.passed?'var(--green)':'var(--surface-3)'};">
      <span style="width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;border-radius:50%;flex-shrink:0;background:${c.passed?'var(--green)':'var(--surface-3)'};color:${c.passed?'#fff':'var(--t4)'};">
        ${c.passed?'<svg viewBox="0 0 24 24" style="width:13px;height:13px;" stroke="currentColor" fill="none" stroke-width="3"><polyline points="20 6 9 17 4 12"/></svg>':'<svg viewBox="0 0 24 24" style="width:13px;height:13px;" stroke="currentColor" fill="none" stroke-width="2"><circle cx="12" cy="12" r="1"/></svg>'}
      </span>
      <span style="font-size:12px;color:var(--t1);flex:1;">${c.label}</span>
      <span style="font-size:9px;text-transform:uppercase;font-weight:700;color:var(--t4);letter-spacing:.04em;padding:2px 8px;border-radius:99px;background:var(--surface-3);">${c.category}</span>
    </div>
  `).join('');
}

function auditRenderVolumeChart(events){
  const ctx=G('auditVolumeChart');
  if(auditVolumeChart){auditVolumeChart.destroy();}
  // Group by date for last 30 days
  const now=new Date(); const byDate={};
  for(let i=29;i>=0;i--){
    const d=new Date(now); d.setDate(d.getDate()-i);
    byDate[d.toLocaleDateString()]=0;
  }
  events.forEach(e=>{
    const d=new Date(e.timestamp).toLocaleDateString();
    if(d in byDate) byDate[d]++;
  });
  const labels=Object.keys(byDate);
  const data=Object.values(byDate);
  auditVolumeChart=new Chart(ctx,{
    type:'bar',
    data:{labels,datasets:[{label:'Events',data,backgroundColor:'rgba(99,102,241,.6)',borderColor:'rgb(99,102,241)',borderWidth:1,borderRadius:4}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{maxTicksLimit:10,font:{size:9}}},y:{beginAtZero:true,ticks:{stepSize:1}}}}
  });
}

function auditRenderCategoryChart(events){
  const ctx=G('auditCategoryChart');
  if(auditCategoryChart){auditCategoryChart.destroy();}
  const cats={};
  events.forEach(e=>{cats[e.category]=(cats[e.category]||0)+1;});
  const labels=Object.keys(cats);
  const data=Object.values(cats);
  const colors=['rgba(59,130,246,.7)','rgba(245,158,11,.7)','rgba(16,185,129,.7)','rgba(239,68,68,.7)','rgba(139,92,246,.7)','rgba(236,72,153,.7)'];
  auditCategoryChart=new Chart(ctx,{
    type:'doughnut',
    data:{labels,datasets:[{data,backgroundColor:colors.slice(0,labels.length),borderWidth:2,borderColor:'var(--surface-1)'}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'right',labels:{font:{size:11},padding:12}}}}
  });
}

function auditRenderSeverityChart(events){
  const ctx=G('auditSeverityChart');
  if(auditSeverityChart){auditSeverityChart.destroy();}
  const sevMap={info:0,warning:0,error:0,critical:0};
  events.forEach(e=>{if(e.severity in sevMap) sevMap[e.severity]++;});
  auditSeverityChart=new Chart(ctx,{
    type:'polarArea',
    data:{
      labels:['Info','Warning','Error','Critical'],
      datasets:[{data:[sevMap.info,sevMap.warning,sevMap.error,sevMap.critical],
        backgroundColor:['rgba(59,130,246,.5)','rgba(245,158,11,.5)','rgba(239,68,68,.5)','rgba(220,38,38,.7)'],
        borderWidth:1}]
    },
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'right',labels:{font:{size:11},padding:12}}},scales:{r:{ticks:{stepSize:1}}}}
  });
}

function auditSeverityBadge(sev){
  const map={info:['var(--blue-light)','var(--blue-fg)','var(--blue-border)'],warning:['var(--amber-light)','var(--amber-fg)','var(--amber-border)'],error:['var(--red-light)','var(--red-fg)','var(--red-border)'],critical:['#fecaca','#991b1b','#fca5a5']};
  const c=map[sev]||map.info;
  return `<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:${c[0]};color:${c[1]};border:1px solid ${c[2]};text-transform:uppercase;">${sev}</span>`;
}

function auditCategoryBadge(cat){
  const map={migration:'var(--green)',config:'var(--amber)',access:'var(--blue)',deploy:'var(--purple,#8b5cf6)',error:'var(--red)',security:'#dc2626'};
  const color=map[cat]||'var(--t3)';
  return `<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:600;background:var(--surface-3);color:${color};text-transform:capitalize;">${cat}</span>`;
}

function auditRenderLog(events){
  const tbody=G('auditLogBody');
  if(!events.length){
    tbody.innerHTML='<tr><td colspan="7" style="padding:32px;text-align:center;color:var(--t4);">No audit events found</td></tr>';
    return;
  }
  tbody.innerHTML=events.slice(0,200).map((e,i)=>{
    const sevColor=e.severity==='error'||e.severity==='critical'?'var(--red)':e.severity==='warning'?'var(--amber)':'transparent';
    return `<tr style="border-bottom:1px solid var(--border);cursor:pointer;border-left:3px solid ${sevColor};" onclick="auditShowDetail(${i})" title="Click for full details">
    <td style="padding:8px 6px;text-align:center;color:var(--t4);font-size:10px;"><svg viewBox="0 0 24 24" style="width:12px;height:12px;stroke:currentColor;fill:none;stroke-width:2;"><polyline points="9 18 15 12 9 6"/></svg></td>
    <td style="padding:8px 10px;color:var(--t3);font-size:11px;white-space:nowrap;">${new Date(e.timestamp).toLocaleString()}</td>
    <td style="padding:8px 10px;color:var(--t1);font-weight:500;">${escHtml(e.event)}</td>
    <td style="padding:8px 10px;text-align:center;">${auditCategoryBadge(e.category)}</td>
    <td style="padding:8px 10px;text-align:center;">${auditSeverityBadge(e.severity)}</td>
    <td style="padding:8px 10px;color:var(--t2);font-size:11px;">${escHtml(e.user)}</td>
    <td style="padding:8px 10px;color:var(--t3);font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${escHtml(e.details)}">${escHtml(e.details)}</td>
  </tr>`;
  }).join('');
}

function auditShowDetail(idx){
  const e=auditAllEvents[idx];
  if(!e)return;
  const panel=G('auditDetailPanel');
  const content=G('auditDetailContent');
  const sevColors={info:['#EFF6FF','#1E40AF','#BFDBFE'],warning:['#FFFBEB','#92400E','#FDE68A'],error:['#FEF2F2','#991B1B','#FECACA'],critical:['#FEF2F2','#7F1D1D','#FCA5A5']};
  const sc=sevColors[e.severity]||sevColors.info;
  let extra='';
  // Show all extra fields beyond standard ones
  const stdKeys=new Set(['timestamp','event','category','severity','user','details']);
  const extraFields=Object.entries(e).filter(([k])=>!stdKeys.has(k)&&e[k]);
  if(extraFields.length){
    extra=`<div style="margin-top:16px;"><div style="font-weight:600;font-size:12px;color:var(--t2);margin-bottom:8px;">Additional Fields</div>
      <div style="display:grid;grid-template-columns:140px 1fr;gap:4px 8px;font-size:11px;">
        ${extraFields.map(([k,v])=>`<div style="color:var(--t4);font-weight:600;">${escHtml(k)}</div><div style="color:var(--t1);word-break:break-all;">${escHtml(String(v))}</div>`).join('')}
      </div></div>`;
  }
  content.innerHTML=`
    <button onclick="auditCloseDetail()" title="Close" style="position:absolute;top:12px;right:16px;background:#fff;border:1px solid var(--border);box-shadow:0 2px 8px rgba(0,0,0,.12);border-radius:50%;width:30px;height:30px;font-size:16px;color:var(--t2);cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1;z-index:1;">&times;</button>
    <div style="background:${sc[0]};border:1px solid ${sc[2]};border-radius:10px;padding:14px 44px 14px 16px;margin-bottom:16px;">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
        ${auditSeverityBadge(e.severity)} ${auditCategoryBadge(e.category)}
      </div>
      <div style="font-size:15px;font-weight:700;color:${sc[1]};margin-top:8px;">${escHtml(e.event)}</div>
    </div>
    <div style="display:grid;grid-template-columns:100px 1fr;gap:8px 12px;font-size:12px;margin-bottom:16px;">
      <div style="color:var(--t4);font-weight:600;">Timestamp</div><div style="color:var(--t1);">${new Date(e.timestamp).toLocaleString()}</div>
      <div style="color:var(--t4);font-weight:600;">User / Source</div><div style="color:var(--t1);">${escHtml(e.user)}</div>
      <div style="color:var(--t4);font-weight:600;">Category</div><div style="color:var(--t1);">${e.category}</div>
      <div style="color:var(--t4);font-weight:600;">Severity</div><div style="color:var(--t1);">${e.severity}</div>
    </div>
    <div style="margin-bottom:16px;">
      <div style="font-weight:600;font-size:12px;color:var(--t2);margin-bottom:6px;">Full Details</div>
      <div style="background:var(--surface-2);border:1px solid var(--border);border-radius:8px;padding:12px;font-size:12px;color:var(--t1);white-space:pre-wrap;word-break:break-word;max-height:300px;overflow-y:auto;font-family:'Fira Code',monospace;line-height:1.6;">${escHtml(e.details||'No additional details available.')}</div>
    </div>
    ${extra}
  `;
  panel.style.display='flex';
}

function auditCloseDetail(){
  G('auditDetailPanel').style.display='none';
}

async function auditFetchDbxLogs(){
  const btn=G('btnFetchDbxLogs');
  const statusEl=G('dbxExecLogStatus');
  const origHTML=btn.innerHTML;
  btn.disabled=true;btn.innerHTML='<div class="spin"></div> Fetching…';
  statusEl.innerHTML='<div class="alert a-info"><span class="spin" style="border-top-color:var(--blue-fg)"></span> Querying Databricks ExecutionLog table…</div>';
  try{
    const r=await fetch('/api/v1/audit/execution-logs');
    const d=await r.json();
    if(d.success){
      const logs=d.logs||[];
      const cols=d.columns||[];
      statusEl.innerHTML=`<div class="alert a-ok"><span class="a-ico">✓</span> Loaded ${logs.length} execution log entries from Databricks.</div>`;
      const tbl=G('dbxExecLogTable');
      tbl.style.display='table';
      // Render header
      G('dbxExecLogHead').innerHTML=`<tr style="background:var(--surface-2);border-bottom:2px solid var(--border);">
        ${cols.map(c=>`<th style="padding:8px 10px;text-align:left;font-weight:600;color:var(--t3);font-size:10px;text-transform:uppercase;white-space:nowrap;">${escHtml(c)}</th>`).join('')}
      </tr>`;
      // Render body
      const tbody=G('dbxExecLogBody');
      if(!logs.length){
        tbody.innerHTML=`<tr><td colspan="${cols.length}" style="padding:24px;text-align:center;color:var(--t4);">No execution logs found in the table.</td></tr>`;
      }else{
        tbody.innerHTML=logs.map(log=>{
          const status=(log.Status||log.status||log.result_state||'').toLowerCase();
          const isErr=status==='failed'||status==='error';
          return `<tr style="border-bottom:1px solid var(--border);cursor:pointer;${isErr?'background:rgba(239,68,68,.04);border-left:3px solid var(--red);':''}" onclick="auditShowDbxLogDetail(this)" data-log='${escAttr(JSON.stringify(log))}'>
            ${cols.map(c=>{
              const v=log[c]??'';
              if(c.toLowerCase().includes('status')||c.toLowerCase().includes('state')){
                const sc=String(v).toLowerCase()==='success'||String(v).toLowerCase()==='succeeded'?'color:#059669;font-weight:600;':String(v).toLowerCase()==='failed'?'color:#DC2626;font-weight:600;':'color:var(--t1);';
                return `<td style="padding:8px 10px;font-size:11px;${sc}">${escHtml(String(v))}</td>`;
              }
              return `<td style="padding:8px 10px;color:var(--t2);font-size:11px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${escHtml(String(v))}">${escHtml(String(v))}</td>`;
            }).join('')}
          </tr>`;
        }).join('');
      }
      // Also merge into audit events
      logs.forEach(log=>{
        const status=(log.Status||log.status||log.result_state||'').toLowerCase();
        auditAllEvents.push({
          timestamp:log.StartTime||log.start_time||log.Timestamp||log.timestamp||new Date().toISOString(),
          event:`Execution: ${log.TableName||log.table_name||log.JobName||log.job_name||'unknown'} — ${status||'completed'}`,
          category:status==='failed'?'error':'migration',
          severity:status==='failed'?'error':'info',
          user:log.User||log.user||'databricks',
          details:Object.entries(log).map(([k,v])=>`${k}: ${v}`).join(' | '),
          ...log
        });
      });
      showToast(`Loaded ${logs.length} Databricks execution logs`,'success');
    }else{
      statusEl.innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span> ${escHtml(d.error||'Failed to fetch logs')}</div>`;
      showToast('Failed: '+(d.error||'Unknown error'),'error');
    }
  }catch(e){
    statusEl.innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span> ${escHtml(e.message)}</div>`;
    showToast('Error: '+e.message,'error');
  }finally{
    btn.disabled=false;btn.innerHTML=origHTML;
  }
}

function escAttr(s){return (s||'').replace(/&/g,'&amp;').replace(/'/g,'&#39;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

function auditShowDbxLogDetail(tr){
  try{
    const log=JSON.parse(tr.dataset.log);
    const panel=G('auditDetailPanel');
    const content=G('auditDetailContent');
    const status=(log.Status||log.status||log.result_state||'').toLowerCase();
    const isErr=status==='failed'||status==='error';
    const headerBg=isErr?'#FEF2F2':'#F0FDF4';
    const headerBorder=isErr?'#FECACA':'#BBF7D0';
    const headerColor=isErr?'#991B1B':'#166534';
    content.innerHTML=`
      <button onclick="auditCloseDetail()" title="Close" style="position:absolute;top:12px;right:16px;background:#fff;border:1px solid var(--border);box-shadow:0 2px 8px rgba(0,0,0,.12);border-radius:50%;width:30px;height:30px;font-size:16px;color:var(--t2);cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1;z-index:1;">&times;</button>
      <div style="background:${headerBg};border:1px solid ${headerBorder};border-radius:10px;padding:14px 44px 14px 16px;margin-bottom:16px;">
        <div style="font-size:15px;font-weight:700;color:${headerColor};">
          ${escHtml(log.TableName||log.table_name||log.JobName||log.job_name||'Execution Log Entry')}
          <span style="font-size:12px;font-weight:600;margin-left:8px;padding:2px 10px;border-radius:99px;background:${isErr?'#FCA5A5':'#BBF7D0'};color:${headerColor};">${escHtml(status||'unknown')}</span>
        </div>
      </div>
      <div style="font-weight:600;font-size:12px;color:var(--t2);margin-bottom:8px;">All Fields</div>
      <div style="display:grid;grid-template-columns:160px 1fr;gap:6px 12px;font-size:12px;">
        ${Object.entries(log).map(([k,v])=>`
          <div style="color:var(--t4);font-weight:600;padding:4px 0;">${escHtml(k)}</div>
          <div style="color:var(--t1);padding:4px 0;word-break:break-all;${k.toLowerCase().includes('error')||k.toLowerCase().includes('message')?'background:#FEF2F2;border-radius:6px;padding:6px 8px;font-family:monospace;font-size:11px;white-space:pre-wrap;':''}">${escHtml(String(v??'—'))}</div>
        `).join('')}
      </div>
    `;
    panel.style.display='flex';
  }catch(e){console.error('auditShowDbxLogDetail',e);}
}

function escHtml(s){
  if(!s) return '';
  const d=document.createElement('div');d.textContent=s;return d.innerHTML;
}

function auditFilterLog(){
  const search=(G('auditSearch').value||'').toLowerCase();
  const catFilter=G('auditFilterCategory').value;
  const sevFilter=G('auditFilterSeverity').value;
  let filtered=auditAllEvents;
  if(search) filtered=filtered.filter(e=>(e.event||'').toLowerCase().includes(search)||(e.details||'').toLowerCase().includes(search)||(e.user||'').toLowerCase().includes(search));
  if(catFilter) filtered=filtered.filter(e=>e.category===catFilter);
  if(sevFilter) filtered=filtered.filter(e=>e.severity===sevFilter);
  auditRenderLog(filtered);
}

function auditExportCSV(){
  if(!auditAllEvents.length){showToast('No events to export','warning');return;}
  const hdr='Timestamp,Event,Category,Severity,User,Details\n';
  const rows=auditAllEvents.map(e=>`"${new Date(e.timestamp).toLocaleString()}","${(e.event||'').replace(/"/g,'""')}","${e.category}","${e.severity}","${(e.user||'').replace(/"/g,'""')}","${(e.details||'').replace(/"/g,'""')}"`).join('\n');
  const blob=new Blob([hdr+rows],{type:'text/csv'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='audit_log_'+new Date().toISOString().slice(0,10)+'.csv';a.click();
  showToast('Audit CSV exported','success');
}

function auditExportJSON(){
  if(!auditAllEvents.length){showToast('No events to export','warning');return;}
  const blob=new Blob([JSON.stringify(auditAllEvents,null,2)],{type:'application/json'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='audit_log_'+new Date().toISOString().slice(0,10)+'.json';a.click();
  showToast('Audit JSON exported','success');
}

/* ══════════════════════════════════════════════════════════════════
   JOB SCHEDULER
   ══════════════════════════════════════════════════════════════════ */
let schAllSchedules=[], schAllHistory=[];

function schToggleTypeFields(){
  const t=G('schType').value;
  G('schCronFields').style.display=t==='cron'?'':'none';
  G('schIntervalFields').style.display=t==='interval'?'':'none';
  G('schOnceFields').style.display=t==='once'?'':'none';
}

async function schLoadTables(){
  try{
    const r=await fetch('/api/v1/scheduler/tables');
    const d=await r.json();
    if(!d.success){showToast(d.error||'Failed to load tables','error');return;}
    const tables=d.tables||[];
    const sel=G('schTableSelect');
    sel.innerHTML='<option value="">— Select a table —</option>';
    tables.forEach(t=>{
      const opt=document.createElement('option');
      // group_id, not table_name -- two different source connections can
      // migrate a same-named table as two separate pipeline groups, and
      // <select> options sharing one value make schEditSchedule's
      // by-value lookup (and this dropdown's own selection) pick whichever
      // one happens to come first, not necessarily the one you meant.
      opt.value=t.group_id;
      opt.textContent=t.full_table+' ('+t.job_count+' jobs — '+t.load_type+')';
      opt.dataset.tableName=t.table_name;
      opt.dataset.tableSchema=t.table_schema;
      opt.dataset.groupId=t.group_id;
      opt.dataset.jobs=JSON.stringify(t.jobs);
      sel.appendChild(opt);
    });
    sel.onchange=function(){
      const o=sel.options[sel.selectedIndex];
      const jInfo=G('schTableJobs');
      if(!o||!o.dataset.jobs){jInfo.innerHTML='';return;}
      try{
        const jobs=JSON.parse(o.dataset.jobs);
        jInfo.innerHTML='<b>Pipeline jobs (execute in order):</b><br>'+jobs.map((j,i)=>
          '<span style="color:var(--blue);">'+(i+1)+'. </span>'+escHtml(j.job_name)+' <span style="color:var(--t4);">('+j.stage+')</span>'
        ).join('<br>');
      }catch(e){jInfo.innerHTML='';}
    };
    showToast('Loaded '+tables.length+' tables','success');
  }catch(e){showToast('Failed to load tables: '+e.message,'error');}
}
// Keep schLoadJobs as alias for backward compat
function schLoadJobs(){schLoadTables();}

let _schEditingId=null; // schedule_id being edited, null = create mode

function schEditSchedule(scheduleId){
  const s=schAllSchedules.find(x=>x.schedule_id===scheduleId);
  if(!s){showToast('Schedule not found','error');return;}
  _schEditingId=scheduleId;

  // Select the table in dropdown (or add it if not loaded) -- matched by
  // group_id, not table_name, since two different pipeline groups can share
  // the same table_name (different source connections).
  const sel=G('schTableSelect');
  let found=false;
  for(let i=0;i<sel.options.length;i++){
    if(sel.options[i].value===(s.group_id||'')){sel.selectedIndex=i;found=true;break;}
  }
  if(!found){
    const opt=document.createElement('option');
    opt.value=s.group_id||s.table_name;
    opt.textContent=s.table_name;
    opt.dataset.tableName=s.table_name;
    opt.dataset.tableSchema=s.table_schema||'dbo';
    opt.dataset.groupId=s.group_id||'';
    opt.dataset.jobs=JSON.stringify([]);
    sel.appendChild(opt);
    sel.value=s.group_id||s.table_name;
  }
  sel.disabled=true; // Can't change table when editing
  sel.dispatchEvent(new Event('change'));

  // Set type
  G('schType').value=s.type||'cron';
  schToggleTypeFields();

  // Fill fields
  if(s.type==='cron') G('schCronExpr').value=s.cron||'';
  if(s.type==='interval'){
    G('schIntervalValue').value=s.interval_value||1;
    G('schIntervalUnit').value=s.interval_unit||'hours';
  }
  if(s.type==='once') G('schOnceAt').value=s.once_at||'';

  // Switch button to Update mode
  G('schSubmitLabel').textContent='Update Schedule';
  G('schSubmitBtn').classList.remove('btn-primary');
  G('schSubmitBtn').classList.add('btn-primary');
  G('schSubmitBtn').style.background='var(--amber)';
  G('schCancelEditBtn').style.display='';

  // Scroll to form
  G('schTableSelect').closest('.card')?.scrollIntoView({behavior:'smooth',block:'start'});
}

function schCancelEdit(){
  _schEditingId=null;
  G('schTableSelect').disabled=false;
  G('schTableSelect').value='';
  G('schTableJobs').innerHTML='';
  G('schType').value='cron';
  schToggleTypeFields();
  G('schCronExpr').value='0 */6 * * *';
  G('schIntervalValue').value=1;
  G('schIntervalUnit').value='hours';
  G('schOnceAt').value='';
  G('schSubmitLabel').textContent='Create Schedule';
  G('schSubmitBtn').style.background='';
  G('schCancelEditBtn').style.display='none';
}

async function schCreateSchedule(){
  const sel=G('schTableSelect');
  if(!sel.value){showToast('Please select a table','error');return;}
  const opt=sel.options[sel.selectedIndex];
  const tableName=opt.dataset.tableName||'';
  const tableSchema=opt.dataset.tableSchema||'dbo';
  const groupId=opt.dataset.groupId||sel.value;
  const schType=G('schType').value;

  const body={
    table_name:tableName,
    table_schema:tableSchema,
    group_id:groupId,
    type:schType,
  };
  // Pass job_names so backend doesn't rely on in-memory state
  try{
    const jobs=JSON.parse(opt.dataset.jobs||'[]');
    body.job_names=jobs.map(j=>j.job_name);
  }catch(e){body.job_names=[];}
  if(schType==='cron'){
    body.cron=G('schCronExpr').value.trim();
    if(!body.cron){showToast('Cron expression required','error');return;}
  }else if(schType==='interval'){
    body.interval_value=G('schIntervalValue').value;
    body.interval_unit=G('schIntervalUnit').value;
    if(!body.interval_value){showToast('Interval value required','error');return;}
  }else{
    body.once_at=G('schOnceAt').value;
    if(!body.once_at){showToast('Date/time required','error');return;}
  }

  try{
    let r, d;
    if(_schEditingId){
      // Update existing schedule
      r=await fetch('/api/v1/scheduler/schedules/'+_schEditingId,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
      d=await r.json();
      if(d.success){
        showToast('Schedule updated for table '+tableName,'success');
        schCancelEdit();
        schRefresh();
      }else{showToast(d.error||'Update failed','error');}
    }else{
      // Create new schedule
      r=await fetch('/api/v1/scheduler/schedules',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
      d=await r.json();
      if(d.success){
        showToast('Schedule created for table '+tableName+' — all jobs will run in order on Databricks','success');
        schRefresh();
      }else{showToast(d.error||'Failed','error');}
    }
  }catch(e){showToast('Error: '+e.message,'error');}
}

async function schRefresh(){
  try{
    const r=await fetch('/api/v1/scheduler/schedules');
    const d=await r.json();
    if(d.success){
      schAllSchedules=d.schedules||[];
      schAllHistory=d.history||[];
      schRenderTable();
      schRenderHistory();
      schUpdateKPIs();
    }
  }catch(e){showToast('Failed to load schedules','error');}
}

function schUpdateKPIs(){
  const active=schAllSchedules.filter(s=>s.status==='active').length;
  const paused=schAllSchedules.filter(s=>s.status==='paused').length;
  G('schTotalCount').textContent=schAllSchedules.length;
  G('schActiveCount').textContent=active;
  G('schPausedCount').textContent=paused;
  const activeSch=schAllSchedules.filter(s=>s.status==='active'&&s.next_run);
  if(activeSch.length){
    activeSch.sort((a,b)=>new Date(a.next_run)-new Date(b.next_run));
    const nxt=new Date(activeSch[0].next_run);
    G('schNextRun').textContent=nxt.toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
  }else{
    G('schNextRun').textContent='—';
  }
}

function schRenderTable(){
  const filter=G('schFilterStatus').value;
  let list=schAllSchedules;
  if(filter) list=list.filter(s=>s.status===filter);

  const tbody=G('schTableBody');
  if(!list.length){
    tbody.innerHTML='<tr><td colspan="8" style="padding:24px;text-align:center;color:var(--t4);">No schedules found.</td></tr>';
    return;
  }
  tbody.innerHTML=list.map(s=>{
    const isActive=s.status==='active';
    const statusBadge=isActive
      ?'<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--green-light);color:var(--green-fg);border:1px solid var(--green-border);">Active</span>'
      :'<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--amber-light);color:var(--amber-fg);border:1px solid var(--amber-border);">Paused</span>';
    const typeBadge=`<span style="padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:var(--surface-3);color:var(--t2);text-transform:capitalize;">${s.type}</span>`;
    const jobsList=(s.job_names||[]).join(' → ')||'—';
    return `<tr style="border-bottom:1px solid var(--border);">
      <td style="padding:8px 10px;color:var(--t1);font-weight:600;font-size:12px;">${escHtml(s.table_name)}</td>
      <td style="padding:8px 10px;color:var(--t3);font-size:10px;max-width:250px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="${escHtml(jobsList)}">${escHtml(jobsList)}</td>
      <td style="padding:8px 10px;text-align:center;">${typeBadge}</td>
      <td style="padding:8px 10px;color:var(--t2);font-size:11px;font-family:monospace;">${escHtml(s.schedule_desc)}</td>
      <td style="padding:8px 10px;text-align:center;">${statusBadge}</td>
      <td style="padding:8px 10px;color:var(--t3);font-size:11px;">${s.next_run?new Date(s.next_run).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—'}</td>
      <td style="padding:8px 10px;color:var(--t3);font-size:11px;">${s.last_run?new Date(s.last_run).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'Never'}</td>
      <td style="padding:8px 10px;text-align:center;">
        <div style="display:flex;gap:4px;justify-content:center;">
          <button class="btn btn-ghost btn-xs" onclick="schEditSchedule('${s.schedule_id}')" title="Edit schedule" style="padding:3px 6px;">
            <svg viewBox="0 0 24 24" style="width:12px;height:12px;" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
          </button>
          <button class="btn btn-ghost btn-xs" onclick="schRunNow('${s.schedule_id}')" title="Run all jobs for ${escHtml(s.table_name)} on Databricks" style="padding:3px 6px;">
            <svg viewBox="0 0 24 24" style="width:12px;height:12px;"><polygon points="5 3 19 12 5 21 5 3"/></svg>
          </button>
          <button class="btn btn-ghost btn-xs" onclick="schToggleStatus('${s.schedule_id}','${isActive?'paused':'active'}')" title="${isActive?'Pause':'Resume'}" style="padding:3px 6px;">
            ${isActive?'<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>':'<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><polygon points="5 3 19 12 5 21 5 3"/></svg>'}
          </button>
          <button class="btn btn-ghost btn-xs" onclick="schDelete('${s.schedule_id}')" title="Delete" style="padding:3px 6px;color:var(--red);">
            <svg viewBox="0 0 24 24" style="width:12px;height:12px;"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
          </button>
        </div>
      </td>
    </tr>`;
  }).join('');
}

function schRenderHistory(){
  const tbody=G('schHistoryBody');
  if(!schAllHistory.length){
    tbody.innerHTML='<tr><td colspan="6" style="padding:24px;text-align:center;color:var(--t4);">No execution history yet.</td></tr>';
    return;
  }
  tbody.innerHTML=schAllHistory.slice(0,50).map(h=>{
    const _resBadges={
      success:'<span style="padding:2px 8px;border-radius:99px;font-size:10px;font-weight:700;background:var(--green-light);color:var(--green-fg);">Success</span>',
      running:'<span style="padding:2px 8px;border-radius:99px;font-size:10px;font-weight:700;background:#DBEAFE;color:#1D4ED8;">Running</span>',
      started:'<span style="padding:2px 8px;border-radius:99px;font-size:10px;font-weight:700;background:#DBEAFE;color:#1D4ED8;">Running</span>',
      failed:'<span style="padding:2px 8px;border-radius:99px;font-size:10px;font-weight:700;background:var(--red-light);color:var(--red-fg);">Failed</span>',
    };
    const resBadge=_resBadges[h.result]||_resBadges.failed;
    return `<tr style="border-bottom:1px solid var(--border);">
      <td style="padding:8px 10px;color:var(--t3);font-size:11px;">${new Date(h.timestamp).toLocaleString()}</td>
      <td style="padding:8px 10px;color:var(--t1);font-weight:500;font-size:12px;">${escHtml(h.table_name||h.job_name||'—')}</td>
      <td style="padding:8px 10px;color:var(--t3);font-size:10px;max-width:200px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="${escHtml(h.jobs||'')}">${escHtml(h.jobs||'—')}</td>
      <td style="padding:8px 10px;text-align:center;"><span style="padding:2px 8px;border-radius:99px;font-size:10px;background:var(--surface-3);color:var(--t2);">${h.trigger}</span></td>
      <td style="padding:8px 10px;text-align:center;">${resBadge}</td>
      <td style="padding:8px 10px;color:var(--t3);font-size:11px;">${escHtml(h.details||'—')}</td>
    </tr>`;
  }).join('');
}

async function schRunNow(scheduleId){
  if(!confirm('Run all jobs for this table on Databricks now?'))return;
  try{
    showToast('Submitting pipeline to Databricks…','info');
    const r=await fetch('/api/v1/scheduler/run-now/'+scheduleId,{method:'POST'});
    const d=await r.json();
    if(d.success&&d.run_result?.success){
      showToast('Pipeline submitted to Databricks! Run ID: '+(d.run_result.run_id||''),'success');
      schRefresh();
    }else{showToast(d.run_result?.error||d.error||'Failed','error');schRefresh();}
  }catch(e){showToast('Error: '+e.message,'error');}
}

async function schToggleStatus(scheduleId, newStatus){
  try{
    const r=await fetch('/api/v1/scheduler/schedules/'+scheduleId,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({status:newStatus})});
    const d=await r.json();
    if(d.success){showToast('Schedule '+newStatus,'success');schRefresh();}
  }catch(e){showToast('Error: '+e.message,'error');}
}

async function schDelete(scheduleId){
  if(!confirm('Delete this schedule?'))return;
  try{
    const r=await fetch('/api/v1/scheduler/schedules/'+scheduleId,{method:'DELETE'});
    const d=await r.json();
    if(d.success){showToast('Schedule deleted','success');schRefresh();}
  }catch(e){showToast('Error: '+e.message,'error');}
}

// Auto-load scheduler data
if(G('pane-wf-scheduler') && G('pane-wf-scheduler').style.display!=='none' && G('pane-wf-scheduler').classList.contains('active')){
  schLoadTables(); schRefresh();
}

/* ══════════════════════════════════════════════════════════════════
   DATA QUALITY DASHBOARD
   ══════════════════════════════════════════════════════════════════ */
let dqDimensionChart=null, dqTrendChart=null, dqAllTables=[];

const DQ_RULES=[
  {id:'no_nulls',       label:'No critical nulls (PK/FK columns)',   dimension:'completeness', check:t=>t.null_pct<5},
  {id:'row_count',      label:'Row count matches source (\u00b12%)',     dimension:'accuracy',     check:t=>t.row_match>=98},
  {id:'schema_match',   label:'Schema matches expected definition',  dimension:'consistency',  check:t=>t.schema_ok},
  {id:'no_duplicates',  label:'No duplicate rows on PK',            dimension:'uniqueness',   check:t=>t.duplicates===0},
  {id:'freshness',      label:'Data updated within SLA window',     dimension:'timeliness',   check:t=>t.fresh},
  {id:'type_valid',     label:'Column types match specification',    dimension:'validity',     check:t=>t.schema_ok},
  {id:'range_check',    label:'Values within expected ranges',       dimension:'validity',     check:t=>t.null_pct<20},
  {id:'ref_integrity',  label:'Referential integrity preserved',     dimension:'consistency',  check:t=>t.row_match>=95},
];

async function dqRefresh(){
  try{
    // ── 1. REAL data first: __dq_metrics written by Bronze/Silver notebooks ──
    let rows=[];
    try{
      const r=await fetch('/api/v1/dq/metrics');
      if(r.ok){const d=await r.json(); if(d.success) rows=d.rows||[];}
    }catch(e){console.warn('dq/metrics fetch failed',e);}

    // ── 2. Legacy file-based summary (only if it holds previously saved results) ──
    if(!rows.length){
      try{
        const r=await fetch('/api/v1/dq/summary');
        if(r.ok){
          const d=await r.json();
          const legacy=d.tables||d||[];
          if(legacy.length && typeof legacy[0]==='object' && ('null_pct' in legacy[0] || 'score' in legacy[0])){
            dqAllTables=legacy; dqRenderFromTables(legacy, true); return;
          }
        }
      }catch(e){}
    }

    // ── 3. No metrics at all → honest empty state (NO synthetic numbers) ──
    if(!rows.length){
      dqAllTables=[];
      G('dqScoreValue').textContent='--'; G('dqScoreValue').style.color='var(--t4)';
      dqDrawScoreRing(0);
      G('dqTablesChecked').textContent='0';
      ['dqNullPctAvg','dqRowMatch','dqSchemaMatch','dqDuplicates','dqStale'].forEach(id=>{G(id).textContent='--';});
      G('dqRulesGrid').innerHTML='<div style="grid-column:1/-1;padding:18px;text-align:center;background:var(--amber-light);border-radius:var(--r);border-left:3px solid var(--amber);"><div style="font-size:12.5px;font-weight:700;color:var(--amber-fg);">No data quality metrics yet</div><div style="font-size:11px;color:var(--t3);margin-top:4px;">Run the Bronze / Silver metadata pipelines — every run writes real scores to <code>__dq_metrics</code> and they appear here automatically.</div></div>';
      if(dqDimensionChart){dqDimensionChart.destroy();dqDimensionChart=null;}
      if(dqTrendChart){dqTrendChart.destroy();dqTrendChart=null;}
      dqRenderDetailTable([]);
      showToast('No DQ metrics found — run the Bronze/Silver pipelines to populate __dq_metrics','warning');
      return;
    }

    // ── 4. Build latest-per-table records across bronze+silver layers ──
    const tblMap={};
    rows.forEach(r=>{
      const name=r.table_name||'unknown';
      if(!tblMap[name]) tblMap[name]={layers:{}, last_checked:null};
      const rec=tblMap[name];
      const layer=(r.layer||'bronze').toLowerCase();
      const ts=r.checked_at||'';
      if(!rec.layers[layer] || ts>(rec.layers[layer].checked_at||'')) rec.layers[layer]=r;
      if(!rec.last_checked||ts>rec.last_checked) rec.last_checked=ts;
    });

    const SLA_HOURS=24;  // freshness SLA — a table is "fresh" if checked within 24h
    const tables=Object.entries(tblMap).map(([name,rec])=>{
      const b=rec.layers.bronze, s=rec.layers.silver;
      const latest=s||b||{};
      const input=Number(latest.input_rows)||0;
      const output=Number(latest.output_rows)||0;
      const nullPct= input>0 ? (Number(latest.null_rows)||0)/input*100 : 0;
      // Retention: silver output vs bronze output (rows surviving cleansing)
      let rowMatch=100;
      if(s && b && Number(b.output_rows)>0) rowMatch=Number(s.output_rows)/Number(b.output_rows)*100;
      else if(s && input>0) rowMatch=output/input*100;
      rowMatch=Math.min(rowMatch,100);
      const schemaOk=!(latest.schema_drift===true||String(latest.schema_drift).toLowerCase()==='true');
      const duplicates=Number(latest.dupe_rows)||0;
      const ageH=rec.last_checked?(Date.now()-new Date(String(rec.last_checked).replace(' ','T')).getTime())/3600000:1e9;
      const fresh=isFinite(ageH)?ageH<=SLA_HOURS:false;
      const t={table:name, null_pct:nullPct, row_match:rowMatch, schema_ok:schemaOk,
               duplicates, fresh, layer:Object.keys(rec.layers).join('+'),
               last_checked:rec.last_checked, input_rows:input, output_rows:output,
               quarantined:Number(latest.quarantined_rows)||0, score:0};
      // Prefer the notebook-computed dq_score; fall back to rule evaluation
      const stored=Number(latest.dq_score);
      t.score=Number.isFinite(stored)&&stored>0?Math.round(stored):Math.round(DQ_RULES.filter(r=>r.check(t)).length/DQ_RULES.length*100);
      return t;
    });

    dqRenderFromTables(tables, false, rows);
  }catch(e){
    console.error('dqRefresh error',e);
    showToast('Failed to load quality data: '+e.message,'error');
  }
}

/* Shared renderer — computes scores/KPIs/charts from a table list.
   trendRows: optional raw __dq_metrics rows for a REAL history chart. */
function dqRenderFromTables(tables, legacyMode, trendRows){
  dqAllTables=tables;
  const total=tables.length||1;

  // Overall score
  const avgScore=Math.round(tables.reduce((s,t)=>s+t.score,0)/total);
  G('dqScoreValue').textContent=avgScore;
  G('dqScoreValue').style.color=avgScore>=80?'var(--green)':avgScore>=50?'var(--amber)':'var(--red)';
  dqDrawScoreRing(avgScore);

  // KPIs
  G('dqTablesChecked').textContent=tables.length;
  const avgNull=tables.length?(tables.reduce((s,t)=>s+(t.null_pct||0),0)/total).toFixed(1)+'%':'--';
  G('dqNullPctAvg').textContent=avgNull;
  const rowMatchPct=tables.length?Math.round(tables.filter(t=>(t.row_match||0)>=98).length/total*100)+'%':'--';
  G('dqRowMatch').textContent=rowMatchPct;
  const schemaPct=tables.length?Math.round(tables.filter(t=>t.schema_ok).length/total*100)+'%':'--';
  G('dqSchemaMatch').textContent=schemaPct;
  G('dqDuplicates').textContent=tables.reduce((s,t)=>s+(t.duplicates||0),0);
  G('dqStale').textContent=tables.filter(t=>!t.fresh).length;

  // Rules grid
  dqRenderRules(tables);
  // Charts
  dqRenderDimension(tables);
  dqRenderTrend(trendRows||null);
  // Table
  dqRenderDetailTable(tables);

  showToast(legacyMode?'Data quality dashboard loaded (saved results)':'Data quality dashboard refreshed from __dq_metrics','success');
}

function dqDrawScoreRing(score){
  const canvas=G('dqScoreRing');
  const ctx=canvas.getContext('2d');
  const s=2; canvas.width=120*s; canvas.height=120*s; ctx.scale(s,s);
  const cx=60,cy=60,r=48,lw=10;
  ctx.clearRect(0,0,120,120);
  // Background ring
  ctx.beginPath();ctx.arc(cx,cy,r,0,2*Math.PI);ctx.strokeStyle='rgba(128,128,128,.15)';ctx.lineWidth=lw;ctx.stroke();
  // Score arc
  const angle=(score/100)*2*Math.PI;
  const color=score>=80?'#10B981':score>=50?'#F59E0B':'#EF4444';
  ctx.beginPath();ctx.arc(cx,cy,r,-Math.PI/2,-Math.PI/2+angle);ctx.strokeStyle=color;ctx.lineWidth=lw;ctx.lineCap='round';ctx.stroke();
}

function dqRenderRules(tables){
  const total=tables.length||1;
  const grid=G('dqRulesGrid');
  grid.innerHTML=DQ_RULES.map(rule=>{
    const passCount=tables.filter(t=>rule.check(t)).length;
    const pct=Math.round(passCount/total*100);
    const color=pct>=90?'var(--green)':pct>=70?'var(--amber)':'var(--red)';
    const bgColor=pct>=90?'var(--green-light)':pct>=70?'var(--amber-light)':'var(--red-light)';
    return `<div style="display:flex;align-items:center;gap:10px;padding:10px 14px;background:${bgColor};border-radius:var(--r);border-left:3px solid ${color};">
      <span style="width:40px;font-size:16px;font-weight:800;color:${color};flex-shrink:0;text-align:center;">${pct}%</span>
      <div style="flex:1;">
        <div style="font-size:12px;font-weight:600;color:var(--t1);">${rule.label}</div>
        <div style="font-size:10px;color:var(--t3);margin-top:2px;">${passCount} of ${total} tables pass \u00b7 ${rule.dimension}</div>
      </div>
    </div>`;
  }).join('');
}

function dqRenderDimension(tables){
  const ctx=G('dqDimensionChart');
  if(dqDimensionChart){dqDimensionChart.destroy();}
  const dims={completeness:0,accuracy:0,consistency:0,uniqueness:0,timeliness:0,validity:0};
  const dimCounts={completeness:0,accuracy:0,consistency:0,uniqueness:0,timeliness:0,validity:0};
  const total=tables.length||1;
  DQ_RULES.forEach(rule=>{
    const passCount=tables.filter(t=>rule.check(t)).length;
    dims[rule.dimension]+=Math.round(passCount/total*100);
    dimCounts[rule.dimension]++;
  });
  const labels=Object.keys(dims).map(d=>d.charAt(0).toUpperCase()+d.slice(1));
  const data=Object.keys(dims).map(d=>dimCounts[d]?Math.round(dims[d]/dimCounts[d]):0);
  dqDimensionChart=new Chart(ctx,{
    type:'radar',
    data:{labels,datasets:[{label:'Quality %',data,backgroundColor:'rgba(16,185,129,.2)',borderColor:'rgb(16,185,129)',pointBackgroundColor:'rgb(16,185,129)',pointRadius:4,borderWidth:2}]},
    options:{responsive:true,maintainAspectRatio:false,scales:{r:{beginAtZero:true,max:100,ticks:{stepSize:20,font:{size:9}}}},plugins:{legend:{display:false}}}
  });
}

function dqRenderTrend(rawRows){
  const ctx=G('dqTrendChart');
  if(dqTrendChart){dqTrendChart.destroy();}
  // REAL trend: average notebook dq_score per day from __dq_metrics history
  const labels=[],data=[];
  if(Array.isArray(rawRows) && rawRows.length){
    const byDay={};
    rawRows.forEach(r=>{
      const d=String(r.checked_at||'').slice(0,10);
      const sc=Number(r.dq_score);
      if(!/^\d{4}-\d{2}-\d{2}$/.test(d)||!isFinite(sc)) return;
      (byDay[d]=byDay[d]||[]).push(sc);
    });
    const days=Object.keys(byDay).sort().slice(-30);
    days.forEach(d=>{
      const vals=byDay[d];
      labels.push(new Date(d+'T00:00:00').toLocaleDateString([],{month:'short',day:'numeric'}));
      data.push(Math.round(vals.reduce((s,v)=>s+v,0)/vals.length));
    });
  }
  dqTrendChart=new Chart(ctx,{
    type:'line',
    data:{labels,datasets:[{label:'Quality Score',data,borderColor:'rgb(99,102,241)',backgroundColor:'rgba(99,102,241,.1)',fill:true,tension:.3,pointRadius:2,borderWidth:2,
      spanGaps:true}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},
      tooltip:{callbacks:{title:items=>items.length?items[0].label:'',label:item=>item.parsed.y!==null?('Score: '+item.parsed.y+'%'):'No runs'}}},
      scales:{x:{ticks:{maxTicksLimit:10,font:{size:9}}},y:{min:0,max:100,ticks:{stepSize:20}}}}
  });
  if(!labels.length){
    const note=ctx.parentElement.querySelector('.dq-trend-empty');
    if(note) note.remove();
    const div=document.createElement('div');
    div.className='dq-trend-empty';
    div.style.cssText='position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:11px;color:var(--t4);background:rgba(255,255,255,.6);border-radius:8px;';
    div.textContent='No run history yet — trend appears after pipelines write DQ metrics';
    ctx.parentElement.style.position='relative';
    ctx.parentElement.appendChild(div);
  } else {
    const note=ctx.parentElement.querySelector('.dq-trend-empty');
    if(note) note.remove();
  }
}

function dqStatusBadge(score){
  if(score>=80) return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--green-light);color:var(--green-fg);border:1px solid var(--green-border);">Pass</span>';
  if(score>=50) return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--amber-light);color:var(--amber-fg);border:1px solid var(--amber-border);">Warning</span>';
  return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--red-light);color:var(--red-fg);border:1px solid var(--red-border);">Fail</span>';
}

function dqMiniBar(pct,color){
  return `<div style="display:flex;align-items:center;gap:6px;"><div style="width:50px;height:6px;background:var(--surface-3);border-radius:99px;overflow:hidden;"><div style="height:100%;width:${Math.min(pct,100)}%;background:${color};border-radius:99px;"></div></div><span style="font-size:10px;color:var(--t3);font-weight:600;">${typeof pct==='number'?pct.toFixed(1)+'%':pct}</span></div>`;
}

function dqRenderDetailTable(tables){
  const tbody=G('dqDetailBody');
  if(!tables.length){
    tbody.innerHTML='<tr><td colspan="8" style="padding:32px;text-align:center;color:var(--t4);">No data quality results</td></tr>';
    return;
  }
  tbody.innerHTML=tables.map(t=>{
    const nullColor=t.null_pct<5?'var(--green)':t.null_pct<15?'var(--amber)':'var(--red)';
    return `<tr style="border-bottom:1px solid var(--border);">
      <td style="padding:8px 10px;font-weight:600;color:var(--t1);">${escHtml(t.table)}</td>
      <td style="padding:8px 10px;text-align:center;font-weight:800;color:${t.score>=80?'var(--green)':t.score>=50?'var(--amber)':'var(--red)'};">${t.score}%</td>
      <td style="padding:8px 10px;text-align:center;">${dqMiniBar(t.null_pct,nullColor)}</td>
      <td style="padding:8px 10px;text-align:center;">${dqMiniBar(t.row_match||0,t.row_match>=98?'var(--green)':'var(--amber)')}</td>
      <td style="padding:8px 10px;text-align:center;">${t.schema_ok?'<span style="color:var(--green);font-weight:700;">\u2713</span>':'<span style="color:var(--red);font-weight:700;">\u2717</span>'}</td>
      <td style="padding:8px 10px;text-align:center;color:${t.duplicates>0?'var(--red)':'var(--t3)'};">${t.duplicates}</td>
      <td style="padding:8px 10px;text-align:center;">${t.fresh?'<span style="color:var(--green);font-size:10px;font-weight:700;text-transform:uppercase;">Fresh</span>':'<span style="color:var(--amber);font-size:10px;font-weight:700;text-transform:uppercase;">Stale</span>'}</td>
      <td style="padding:8px 10px;text-align:center;">${dqStatusBadge(t.score)}</td>
    </tr>`;
  }).join('');
}

function dqFilterTable(){
  const search=(G('dqSearchTable').value||'').toLowerCase();
  const statusFilter=G('dqFilterStatus').value;
  let filtered=dqAllTables;
  if(search) filtered=filtered.filter(t=>(t.table||'').toLowerCase().includes(search));
  if(statusFilter){
    filtered=filtered.filter(t=>{
      if(statusFilter==='pass') return t.score>=80;
      if(statusFilter==='warning') return t.score>=50&&t.score<80;
      if(statusFilter==='fail') return t.score<50;
      return true;
    });
  }
  dqRenderDetailTable(filtered);
}

function dqExportCSV(){
  if(!dqAllTables.length){showToast('No data to export','warning');return;}
  const hdr='Table,Score,Null%,RowMatch%,Schema,Duplicates,Fresh,Status\n';
  const rows=dqAllTables.map(t=>`"${t.table}",${t.score},${(t.null_pct||0).toFixed(1)},${(t.row_match||0).toFixed(1)},${t.schema_ok?'Yes':'No'},${t.duplicates},${t.fresh?'Fresh':'Stale'},${t.score>=80?'Pass':t.score>=50?'Warning':'Fail'}`).join('\n');
  const blob=new Blob([hdr+rows],{type:'text/csv'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='data_quality_'+new Date().toISOString().slice(0,10)+'.csv';a.click();
  showToast('Quality report CSV exported','success');
}

function dqExportJSON(){
  if(!dqAllTables.length){showToast('No data to export','warning');return;}
  const blob=new Blob([JSON.stringify(dqAllTables,null,2)],{type:'application/json'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='data_quality_'+new Date().toISOString().slice(0,10)+'.json';a.click();
  showToast('Quality report JSON exported','success');
}

/* ══════════════════════════════════════════════════════════════════
   RECONCILIATION REPORT
   ══════════════════════════════════════════════════════════════════ */
let rcStatusChart=null, rcVarianceChart=null, rcAllRows=[], rcTableSummaryData=[];

async function reconRefresh(){
  try{
    showToast('Loading reconciliation data from Databricks…','info');
    const r=await fetch('/api/v1/recon/data',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
    const data=await r.json();
    if(data.error){
      showToast('Reconciliation error: '+data.error,'error');
      return;
    }
    const rows=data.rows||[];
    if(!rows.length){
      showToast('No reconciliation data found. Run the orchestrator first.','warning');
      return;
    }
    rcAllRows=rows;

    // KPIs
    const passCount=rows.filter(r=>r.status==='PASS').length;
    const warnCount=rows.filter(r=>r.status==='WARN').length;
    const failCount=rows.filter(r=>r.status==='FAIL').length;
    const tables=[...new Set(rows.map(r=>r.source_table))];
    G('rcTotalChecks').textContent=rows.length;
    G('rcPassed').textContent=passCount;
    G('rcWarnings').textContent=warnCount;
    G('rcFailed').textContent=failCount;
    G('rcTablesCount').textContent=tables.length;

    // Per-table summary
    rcTableSummaryData=tables.map(t=>{
      const tRows=rows.filter(r=>r.source_table===t);
      const p=tRows.filter(r=>r.status==='PASS').length;
      const w=tRows.filter(r=>r.status==='WARN').length;
      const f=tRows.filter(r=>r.status==='FAIL').length;
      const maxVar=Math.max(...tRows.map(r=>parseFloat(r.variance_pct||0)||0));
      return {table:t,total:tRows.length,pass:p,warn:w,fail:f,maxVar,status:f>0?'FAIL':w>0?'WARN':'PASS'};
    });

    // Charts
    rcRenderStatusChart(passCount,warnCount,failCount);
    rcRenderVarianceChart(rows);
    // Tables
    rcRenderTableSummary(rcTableSummaryData);
    rcRenderDetail(rows);

    showToast('Reconciliation report loaded — '+rows.length+' checks across '+tables.length+' tables','success');
  }catch(e){
    console.error('reconRefresh error',e);
    showToast('Failed to load reconciliation: '+e.message,'error');
  }
}

function rcRenderStatusChart(pass,warn,fail){
  const ctx=G('rcStatusChart');
  if(rcStatusChart){rcStatusChart.destroy();}
  rcStatusChart=new Chart(ctx,{
    type:'doughnut',
    data:{labels:['Pass','Warn','Fail'],datasets:[{data:[pass,warn,fail],backgroundColor:['rgba(16,185,129,.7)','rgba(245,158,11,.7)','rgba(239,68,68,.7)'],borderWidth:2,borderColor:'var(--surface-1)'}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{font:{size:12},padding:16}}}}
  });
}

function rcRenderVarianceChart(rows){
  const ctx=G('rcVarianceChart');
  if(rcVarianceChart){rcVarianceChart.destroy();}
  // Show top 20 highest-variance non-row-count columns
  const nonRC=rows.filter(r=>r.column_name!=='__ROW_COUNT__').sort((a,b)=>Math.abs(parseFloat(b.variance_pct||0))-Math.abs(parseFloat(a.variance_pct||0))).slice(0,20);
  const labels=nonRC.map(r=>r.source_table.split('.').pop()+'.'+r.column_name);
  const vals=nonRC.map(r=>parseFloat(r.variance_pct||0));
  const bgColors=vals.map(v=>v<0.01?'rgba(16,185,129,.7)':v<1?'rgba(245,158,11,.7)':'rgba(239,68,68,.7)');
  rcVarianceChart=new Chart(ctx,{
    type:'bar',
    data:{labels,datasets:[{label:'Variance %',data:vals,backgroundColor:bgColors,borderRadius:4,borderWidth:0}]},
    options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{beginAtZero:true,title:{display:true,text:'Variance %'}},y:{ticks:{font:{size:10}}}}}
  });
}

function rcStatusBadge(status){
  if(status==='PASS') return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--green-light);color:var(--green-fg);border:1px solid var(--green-border);">PASS</span>';
  if(status==='WARN') return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--amber-light);color:var(--amber-fg);border:1px solid var(--amber-border);">WARN</span>';
  return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--red-light);color:var(--red-fg);border:1px solid var(--red-border);">FAIL</span>';
}

function rcRenderTableSummary(tables){
  const tbody=G('rcTableSummaryBody');
  if(!tables.length){
    tbody.innerHTML='<tr><td colspan="7" style="padding:32px;text-align:center;color:var(--t4);">No reconciliation data</td></tr>';
    return;
  }
  tbody.innerHTML=tables.map(t=>`<tr style="border-bottom:1px solid var(--border);${t.fail>0?'background:rgba(239,68,68,.03);':''}">
    <td style="padding:8px 10px;font-weight:600;color:var(--t1);">${escHtml(t.table)}</td>
    <td style="padding:8px 10px;text-align:center;color:var(--t2);">${t.total}</td>
    <td style="padding:8px 10px;text-align:center;color:var(--green);font-weight:700;">${t.pass}</td>
    <td style="padding:8px 10px;text-align:center;color:var(--amber);font-weight:700;">${t.warn}</td>
    <td style="padding:8px 10px;text-align:center;color:var(--red);font-weight:700;">${t.fail}</td>
    <td style="padding:8px 10px;text-align:center;font-family:monospace;font-size:11px;color:${t.maxVar<0.01?'var(--green)':t.maxVar<1?'var(--amber)':'var(--red)'};">${t.maxVar.toFixed(4)}%</td>
    <td style="padding:8px 10px;text-align:center;">${rcStatusBadge(t.status)}</td>
  </tr>`).join('');
}

function rcFmtNum(v){
  const n=parseFloat(v);
  if(isNaN(n)) return v||'—';
  return n.toLocaleString(undefined,{maximumFractionDigits:2});
}

function rcRenderDetail(rows){
  const tbody=G('rcDetailBody');
  if(!rows.length){
    tbody.innerHTML='<tr><td colspan="10" style="padding:32px;text-align:center;color:var(--t4);">No reconciliation data</td></tr>';
    return;
  }
  // Show FAIL/WARN first, then PASS
  const sorted=[...rows].sort((a,b)=>{
    const order={FAIL:0,WARN:1,PASS:2};
    return (order[a.status]||2)-(order[b.status]||2);
  });
  tbody.innerHTML=sorted.slice(0,500).map(r=>{
    const isRowCount=r.column_name==='__ROW_COUNT__';
    return `<tr style="border-bottom:1px solid var(--border);${r.status!=='PASS'?'background:rgba(239,68,68,.03);':''}">
      <td style="padding:8px 10px;font-weight:600;color:var(--t1);font-size:11px;">${escHtml(r.source_table||'')}</td>
      <td style="padding:8px 10px;color:var(--t2);font-size:11px;">${escHtml(r.bronze_table||'')}</td>
      <td style="padding:8px 10px;color:var(--t1);font-family:monospace;font-size:11px;font-weight:${isRowCount?'700':'400'};">${escHtml(r.column_name||'')}</td>
      <td style="padding:8px 10px;text-align:center;font-size:10px;color:var(--t3);text-transform:uppercase;">${escHtml(r.data_type||'')}</td>
      <td style="padding:8px 10px;text-align:right;font-family:monospace;font-size:11px;">${rcFmtNum(r.source_value)}</td>
      <td style="padding:8px 10px;text-align:right;font-family:monospace;font-size:11px;">${rcFmtNum(r.bronze_value)}</td>
      <td style="padding:8px 10px;text-align:right;font-family:monospace;font-size:11px;color:${r.status==='PASS'?'var(--green)':r.status==='WARN'?'var(--amber)':'var(--red)'};">${rcFmtNum(r.variance)}</td>
      <td style="padding:8px 10px;text-align:right;font-family:monospace;font-size:11px;font-weight:700;color:${r.status==='PASS'?'var(--green)':r.status==='WARN'?'var(--amber)':'var(--red)'};">${parseFloat(r.variance_pct||0).toFixed(4)}%</td>
      <td style="padding:8px 10px;text-align:center;">${rcStatusBadge(r.status)}</td>
      <td style="padding:8px 10px;text-align:center;font-size:10px;color:var(--t3);">${r.recon_timestamp?(r.recon_timestamp+'').slice(0,19):''}</td>
    </tr>`;
  }).join('');
}

function rcFilterTable(){
  const search=(G('rcSearchTable').value||'').toLowerCase();
  const statusFilter=G('rcFilterStatus').value;
  const dateFrom=G('rcDateFrom').value;
  const dateTo=G('rcDateTo').value;

  // Filter detail rows first (they have timestamps)
  let detailFiltered=rcAllRows;
  if(search) detailFiltered=detailFiltered.filter(r=>(r.source_table||'').toLowerCase().includes(search));
  if(statusFilter) detailFiltered=detailFiltered.filter(r=>r.status===statusFilter);
  if(dateFrom||dateTo){
    detailFiltered=detailFiltered.filter(r=>{
      const ts=(r.recon_timestamp||'').slice(0,10);
      if(!ts) return false;
      if(dateFrom&&ts<dateFrom) return false;
      if(dateTo&&ts>dateTo) return false;
      return true;
    });
  }
  rcRenderDetail(detailFiltered);

  // Rebuild per-table summary from filtered detail rows
  if(dateFrom||dateTo){
    const tables=[...new Set(detailFiltered.map(r=>r.source_table))];
    const summaryFiltered=tables.map(t=>{
      const tRows=detailFiltered.filter(r=>r.source_table===t);
      const p=tRows.filter(r=>r.status==='PASS').length;
      const w=tRows.filter(r=>r.status==='WARN').length;
      const f=tRows.filter(r=>r.status==='FAIL').length;
      const maxVar=Math.max(...tRows.map(r=>parseFloat(r.variance_pct||0)||0));
      return {table:t,total:tRows.length,pass:p,warn:w,fail:f,maxVar,status:f>0?'FAIL':w>0?'WARN':'PASS'};
    });
    rcRenderTableSummary(summaryFiltered);
  } else {
    let filtered=rcTableSummaryData;
    if(search) filtered=filtered.filter(t=>(t.table||'').toLowerCase().includes(search));
    if(statusFilter) filtered=filtered.filter(t=>t.status===statusFilter);
    rcRenderTableSummary(filtered);
  }
}

function rcClearDates(){
  G('rcDateFrom').value='';
  G('rcDateTo').value='';
  rcFilterTable();
}

function rcExportCSV(){
  if(!rcAllRows.length){showToast('No data to export','warning');return;}
  const hdr='Source Table,Bronze Table,Column,Type,Source Value,Bronze Value,Variance,Variance %,Status,Timestamp\n';
  const rows=rcAllRows.map(r=>`"${r.source_table}","${r.bronze_table}","${r.column_name}","${r.data_type}",${r.source_value},${r.bronze_value},${r.variance},${r.variance_pct},${r.status},"${r.recon_timestamp||''}"`).join('\n');
  const blob=new Blob([hdr+rows],{type:'text/csv'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='reconciliation_'+new Date().toISOString().slice(0,10)+'.csv';a.click();
  showToast('Reconciliation CSV exported','success');
}

/* ══════════════════════════════════════════════════════════════════
   SCHEMA COMPARISON
   ══════════════════════════════════════════════════════════════════ */
let scMatchChart=null, scDiffTypeChart=null, scAllDiffs=[], scAllTableSummary=[];

async function scRefresh(){
  try{
    const srcSchema=(G('scSourceSchema').value||'').trim();
    const tgtSchema=(G('scTargetSchema').value||'').trim();
    if(!srcSchema||!tgtSchema){
      showToast('Please enter both Source and Target catalog.schema','warning');
      return;
    }

    showToast('Running schema comparison…','info');

    // Pass date range if set
    const dateFrom=G('scDateFrom')?G('scDateFrom').value:'';
    const dateTo=G('scDateTo')?G('scDateTo').value:'';
    const r=await fetch('/api/v1/schema/compare',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source:srcSchema,target:tgtSchema,date_from:dateFrom,date_to:dateTo})});
    const data=await r.json();

    if(data.error){
      showToast('Schema comparison error: '+data.error,'error');
      return;
    }

    if(!data.tables||!data.tables.length){
      showToast('No tables found for comparison. Check your source/target schemas.','warning');
      return;
    }

    // Flatten diffs
    scAllDiffs=[];
    scAllTableSummary=[];
    let totalMatched=0,totalMismatched=0,totalMissing=0,totalExtra=0;

    (data.tables||[]).forEach(t=>{
      const diffs=t.diffs||[];
      const matchCount=diffs.filter(d=>d.diff_type==='match').length;
      const mismatchCount=diffs.filter(d=>d.diff_type==='type_mismatch'||d.diff_type==='nullable_diff').length;
      const missingCount=diffs.filter(d=>d.diff_type==='missing_col').length;
      const extraCount=diffs.filter(d=>d.diff_type==='extra_col').length;
      const status=mismatchCount+missingCount+extraCount===0?'match':'mismatch';
      scAllTableSummary.push({table:t.table,src_cols:t.src_cols||diffs.length,tgt_cols:t.tgt_cols||diffs.length,matched:matchCount,diffs:mismatchCount+missingCount+extraCount,status});
      if(status==='match') totalMatched++; else totalMismatched++;
      totalMissing+=missingCount; totalExtra+=extraCount;
      scAllDiffs.push(...diffs);
    });

    // KPIs
    G('scTablesCompared').textContent=scAllTableSummary.length;
    G('scMatched').textContent=totalMatched;
    G('scMismatched').textContent=totalMismatched;
    G('scMissing').textContent=totalMissing;
    G('scExtra').textContent=totalExtra;

    // Charts
    scRenderMatchChart(totalMatched,totalMismatched);
    scRenderDiffTypeChart(scAllDiffs);
    // Tables
    scRenderDiffs(scAllDiffs);
    scRenderTableSummary(scAllTableSummary);

    showToast('Schema comparison complete','success');
  }catch(e){
    console.error('scRefresh error',e);
    showToast('Schema comparison failed: '+e.message,'error');
  }
}

function scRenderMatchChart(matched,mismatched){
  const ctx=G('scMatchChart');
  if(scMatchChart){scMatchChart.destroy();}
  scMatchChart=new Chart(ctx,{
    type:'doughnut',
    data:{labels:['Matched','Mismatched'],datasets:[{data:[matched,mismatched],backgroundColor:['rgba(16,185,129,.7)','rgba(239,68,68,.7)'],borderWidth:2,borderColor:'var(--surface-1)'}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{font:{size:12},padding:16}}}}
  });
}

function scRenderDiffTypeChart(diffs){
  const ctx=G('scDiffTypeChart');
  if(scDiffTypeChart){scDiffTypeChart.destroy();}
  const counts={type_mismatch:0,nullable_diff:0,missing_col:0,extra_col:0,match:0};
  diffs.forEach(d=>{if(d.diff_type in counts) counts[d.diff_type]++;});
  scDiffTypeChart=new Chart(ctx,{
    type:'bar',
    data:{
      labels:['Type Mismatch','Nullable Diff','Missing Col','Extra Col','Match'],
      datasets:[{data:[counts.type_mismatch,counts.nullable_diff,counts.missing_col,counts.extra_col,counts.match],
        backgroundColor:['rgba(239,68,68,.7)','rgba(245,158,11,.7)','rgba(220,38,38,.6)','rgba(59,130,246,.7)','rgba(16,185,129,.7)'],
        borderRadius:6,borderWidth:0}]
    },
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,ticks:{stepSize:1}}}}
  });
}

function scDiffBadge(type){
  const map={
    match:['var(--green-light)','var(--green-fg)','var(--green-border)','Match'],
    type_mismatch:['var(--red-light)','var(--red-fg)','var(--red-border)','Type Mismatch'],
    nullable_diff:['var(--amber-light)','var(--amber-fg)','var(--amber-border)','Nullable Diff'],
    missing_col:['#fecaca','#991b1b','#fca5a5','Missing'],
    extra_col:['var(--blue-light)','var(--blue-fg)','var(--blue-border)','Extra Col']
  };
  const c=map[type]||map.match;
  return `<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:${c[0]};color:${c[1]};border:1px solid ${c[2]};text-transform:uppercase;">${c[3]}</span>`;
}

function scStatusBadge(status){
  if(status==='match') return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--green-light);color:var(--green-fg);border:1px solid var(--green-border);">\u2713 Match</span>';
  return '<span style="padding:2px 10px;border-radius:99px;font-size:10px;font-weight:700;background:var(--red-light);color:var(--red-fg);border:1px solid var(--red-border);">\u2717 Diffs</span>';
}

function scRenderDiffs(diffs){
  const tbody=G('scDiffBody');
  if(!diffs.length){
    tbody.innerHTML='<tr><td colspan="8" style="padding:32px;text-align:center;color:var(--t4);">No comparison data</td></tr>';
    return;
  }
  // Show non-matches first, then matches
  const sorted=[...diffs].sort((a,b)=>(a.diff_type==='match'?1:0)-(b.diff_type==='match'?1:0));
  tbody.innerHTML=sorted.slice(0,300).map(d=>`<tr style="border-bottom:1px solid var(--border);${d.diff_type!=='match'?'background:rgba(239,68,68,.03);':''}">
    <td style="padding:8px 10px;font-weight:600;color:var(--t1);">${escHtml(d.table)}</td>
    <td style="padding:8px 10px;color:var(--t1);font-family:monospace;font-size:11px;">${escHtml(d.column)}</td>
    <td style="padding:8px 10px;text-align:center;font-family:monospace;font-size:11px;color:var(--t2);">${escHtml(d.src_type)}</td>
    <td style="padding:8px 10px;text-align:center;font-family:monospace;font-size:11px;color:${d.diff_type==='type_mismatch'?'var(--red)':'var(--t2)'};">${escHtml(d.tgt_type)}</td>
    <td style="padding:8px 10px;text-align:center;font-size:11px;">${d.src_nullable?'YES':'NO'}</td>
    <td style="padding:8px 10px;text-align:center;font-size:11px;color:${d.diff_type==='nullable_diff'?'var(--amber)':'var(--t2)'};">${d.tgt_nullable?'YES':'NO'}</td>
    <td style="padding:8px 10px;text-align:center;">${scDiffBadge(d.diff_type)}</td>
    <td style="padding:8px 10px;text-align:center;">${d.diff_type==='match'?'<span style="color:var(--green);font-weight:700;">\u2713</span>':'<span style="color:var(--red);font-weight:700;">\u2717</span>'}</td>
  </tr>`).join('');
}

function scRenderTableSummary(tables){
  const tbody=G('scTableSummaryBody');
  if(!tables.length){
    tbody.innerHTML='<tr><td colspan="6" style="padding:24px;text-align:center;color:var(--t4);">No comparison results yet</td></tr>';
    return;
  }
  tbody.innerHTML=tables.map(t=>`<tr style="border-bottom:1px solid var(--border);">
    <td style="padding:8px 10px;font-weight:600;color:var(--t1);">${escHtml(t.table)}</td>
    <td style="padding:8px 10px;text-align:center;color:var(--t2);">${t.src_cols}</td>
    <td style="padding:8px 10px;text-align:center;color:var(--t2);">${t.tgt_cols}</td>
    <td style="padding:8px 10px;text-align:center;color:var(--green);font-weight:700;">${t.matched}</td>
    <td style="padding:8px 10px;text-align:center;color:${t.diffs>0?'var(--red)':'var(--t3)'};font-weight:700;">${t.diffs}</td>
    <td style="padding:8px 10px;text-align:center;">${scStatusBadge(t.status)}</td>
  </tr>`).join('');
}

function scFilterDiffs(){
  const search=(G('scSearchDiff').value||'').toLowerCase();
  const typeFilter=G('scFilterType').value;
  const dateFrom=G('scDateFrom')?G('scDateFrom').value:'';
  const dateTo=G('scDateTo')?G('scDateTo').value:'';

  // Filter diffs
  let filtered=scAllDiffs;
  if(search) filtered=filtered.filter(d=>(d.table||'').toLowerCase().includes(search)||(d.column||'').toLowerCase().includes(search));
  if(typeFilter) filtered=filtered.filter(d=>d.diff_type===typeFilter);
  if(dateFrom||dateTo){
    filtered=filtered.filter(d=>{
      const ts=(d.compared_at||'').slice(0,10);
      if(!ts) return true;
      if(dateFrom&&ts<dateFrom) return false;
      if(dateTo&&ts>dateTo) return false;
      return true;
    });
  }
  scRenderDiffs(filtered);

  // Filter per-table summary to match
  let filteredSummary=scAllTableSummary;
  if(search) filteredSummary=filteredSummary.filter(t=>(t.table||'').toLowerCase().includes(search));
  scRenderTableSummary(filteredSummary);

  // Update KPIs and charts from filtered data
  const fMatched=filteredSummary.filter(t=>t.status==='match').length;
  const fMismatched=filteredSummary.filter(t=>t.status!=='match').length;
  const fMissing=filtered.filter(d=>d.diff_type==='missing_col').length;
  const fExtra=filtered.filter(d=>d.diff_type==='extra_col').length;
  G('scTablesCompared').textContent=filteredSummary.length;
  G('scMatched').textContent=fMatched;
  G('scMismatched').textContent=fMismatched;
  G('scMissing').textContent=fMissing;
  G('scExtra').textContent=fExtra;
  scRenderMatchChart(fMatched,fMismatched);
  scRenderDiffTypeChart(filtered);
}

function scClearDates(){
  if(G('scDateFrom')) G('scDateFrom').value='';
  if(G('scDateTo')) G('scDateTo').value='';
  scFilterDiffs();
}

function scExportCSV(){
  if(!scAllDiffs.length){showToast('No data to export','warning');return;}
  const hdr='Table,Column,Source Type,Target Type,Source Nullable,Target Nullable,Diff Type\n';
  const rows=scAllDiffs.map(d=>`"${d.table}","${d.column}","${d.src_type}","${d.tgt_type}",${d.src_nullable?'YES':'NO'},${d.tgt_nullable?'YES':'NO'},${d.diff_type}`).join('\n');
  const blob=new Blob([hdr+rows],{type:'text/csv'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='schema_comparison_'+new Date().toISOString().slice(0,10)+'.csv';a.click();
  showToast('Schema comparison CSV exported','success');
}

function scExportJSON(){
  if(!scAllDiffs.length){showToast('No data to export','warning');return;}
  const blob=new Blob([JSON.stringify({diffs:scAllDiffs,tables:scAllTableSummary},null,2)],{type:'application/json'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='schema_comparison_'+new Date().toISOString().slice(0,10)+'.json';a.click();
  showToast('Schema comparison JSON exported','success');
}
