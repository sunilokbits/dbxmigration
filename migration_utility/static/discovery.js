/* ═══════════════════════════════════════════════════════════════════════════
   Discovery — Frontend Logic
   Handles: scan trigger, KPI cards, BOM table, charts, D3 dependency graph,
   RCA slide-in panel, and export actions.
   ═══════════════════════════════════════════════════════════════════════════ */

/* ── State ── */
let _discReport = null;   // cached report from API
let _discGraph  = null;   // graph data for D3
let _d3Sim      = null;   // D3 force simulation handle

/* ── Helpers ── */
/* G(id) is already declared in main.js — reuse it */

function _discToast(msg, type){
  if(typeof showToast==='function') showToast(msg, type);
  else console.log('[Discovery]', msg);
}

/* ═══════════════════════════════════════════════════════════════════════════
   RUN SCAN
   ═══════════════════════════════════════════════════════════════════════════ */

/* Read authoritative source-db config from the Settings card (cfg*),
   falling back to legacy hidden inputs (srcType / wfSrcType) for back-compat.
   This ensures the Discovery live scan honours whatever the user chose in the
   "Source Database" card in Settings (Azure SQL, SQL Server, MySQL, etc.).   */
function _discReadSourceConfig(){
  const pick = (...ids) => { for(const id of ids){ const el=G(id); if(el && (el.value||'').trim()) return el.value.trim(); } return ''; };
  const st = pick('cfgSrcType','srcType','wfSrcType') || 'sqlserver';
  const _nonSql = (st==='sharepoint'||st==='api');
  const cfg = {
    source_type: st,
    server:      pick('cfgSrcServer','srcServer','wfSrcServer'),
    database:    st==='snowflake' ? pick('cfgSrcSnowDb','srcSnowDb','wfSrcSnowDb') || pick('cfgSrcDb','srcDb','wfSrcDb') : (_nonSql ? '' : pick('cfgSrcDb','srcDb','wfSrcDb')),
    username:    pick('cfgSrcUser','srcUser','wfSrcUser'),
    password:    pick('cfgSrcPass','srcPass','wfSrcPass'),
  };
  if(st==='snowflake'){
    cfg.account   = pick('cfgSrcAccount','srcAccount','wfSrcAccount');
    cfg.warehouse = pick('cfgSrcWarehouse','srcWarehouse','wfSrcWarehouse');
    cfg.role      = pick('cfgSrcRole','srcRole','wfSrcRole');
  }
  if(st==='sharepoint'){
    cfg.tenant_id = pick('cfgSrcSpTenantId','srcTenantId','wfSrcTenantId');
  }
  if(st==='api'){
    const _authSel = G('cfgSrcApiAuthType')||G('srcApiAuthType')||G('wfSrcApiAuthType');
    cfg.api_auth_type = (_authSel && _authSel.value) || 'none';
    cfg.api_key_header = pick('cfgSrcApiKeyHeader','srcApiKeyHeader','wfSrcApiKeyHeader');
  }
  return cfg;
}

/* Supported source systems for live Discovery scan. The backend
   sql_pool.get_connection() currently uses pyodbc, so only ODBC-compatible
   sources are supported for live scans today. SharePoint and REST API are
   scanned via their own REST connectors. */
const _DISC_LIVE_SUPPORTED = new Set(['azuresql','sqlserver','synapse','snowflake','redshift','sharepoint','api']);
/* Non-SQL sources — they don't need a database name, just a URL */
const _DISC_NON_SQL = new Set(['sharepoint','api']);

/* Refresh the "Source: Azure SQL · server.db" badge shown next to the
   source selector so users see exactly what Discovery will connect to. */
function _discUpdateSourceBadge(){
  const badge = G('discSourceBadge'); if(!badge) return;
  const sel = G('discSourceSelect'); const mode = sel?.value || 'static';
  if(mode === 'static'){
    badge.innerHTML = '<span style="color:var(--t4);">Using built-in demo objects</span>';
    badge.style.background = 'rgba(148,163,184,.1)';
    return;
  }
  const cfg = _discReadSourceConfig();
  const label = ({azuresql:'Azure SQL',sqlserver:'SQL Server',synapse:'Synapse',mysql:'MySQL',postgresql:'PostgreSQL',oracle:'Oracle',snowflake:'Snowflake',sharepoint:'SharePoint',api:'REST API'})[cfg.source_type] || cfg.source_type;
  const _needsDb = !(cfg.source_type==='snowflake'||_DISC_NON_SQL.has(cfg.source_type));
  const _srvRaw = cfg.source_type==='snowflake' ? (cfg.account||'') : (cfg.server||'');
  const srv = _srvRaw ? (_srvRaw.length>32?_srvRaw.slice(0,32)+'…':_srvRaw) : '<not set>';
  const db  = _needsDb ? (cfg.database || '<no db>') : '—';
  const ok  = _DISC_LIVE_SUPPORTED.has(cfg.source_type) && (cfg.source_type==='snowflake' ? cfg.account : cfg.server) && (!_needsDb || cfg.database);
  const warn = !_DISC_LIVE_SUPPORTED.has(cfg.source_type)
    ? ' ⚠ live scan supports Azure SQL / SQL Server / Synapse / Snowflake / SharePoint / API'
    : (cfg.source_type==='snowflake' ? (!cfg.account ? ' ⚠ set Account in Settings' : '')
      : (_DISC_NON_SQL.has(cfg.source_type) ? (!cfg.server ? ' ⚠ set the URL in Settings' : '')
      : (!cfg.server || !cfg.database ? ' ⚠ set Server & Database in Settings' : '')));
  badge.innerHTML = '<b style="color:'+(ok?'#3B82F6':'#F59E0B')+';">'+label+'</b> · <code style="font-size:10px;">'+srv+' / '+db+'</code>'+
    (warn?'<span style="color:#F59E0B;font-weight:600;">'+warn+'</span>':'');
  badge.style.background = ok ? 'rgba(59,130,246,.08)' : 'rgba(245,158,11,.1)';
}

/* Install change listeners once so the badge updates live when users change
   the Source Type in Settings or the Discovery source mode. */
function _discInstallSourceWatchers(){
  if(window._discSourceWatched) return; window._discSourceWatched = true;
  ['discSourceSelect','cfgSrcType','cfgSrcServer','cfgSrcDb'].forEach(id=>{
    const el = G(id); if(!el) return;
    el.addEventListener('change', _discUpdateSourceBadge);
    el.addEventListener('input',  _discUpdateSourceBadge);
  });
  _discUpdateSourceBadge();
}
/* ── Load Schemas from source DB ── */
async function discLoadSchemas(){
  const sel = G('discSchemaSelect'); if(!sel) return;
  const cfg = _discReadSourceConfig();
  sel.innerHTML = '<option value="">Loading…</option>';
  try{
    const r = await fetch('/api/v1/discovery/schemas',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source_config:cfg})});
    const d = await r.json();
    if(d.success && d.schemas){
      sel.innerHTML = '<option value="">All Schemas</option>' + d.schemas.map(s=>'<option value="'+s+'">'+s+'</option>').join('');
      _discToast('Loaded '+d.schemas.length+' schemas','tok');
    } else {
      sel.innerHTML = '<option value="">All Schemas</option>';
      _discToast(d.error||'Failed to load schemas','err');
    }
  }catch(e){
    sel.innerHTML = '<option value="">All Schemas</option>';
    _discToast('Schema load error: '+e.message,'err');
  }
}
window.discLoadSchemas = discLoadSchemas;

/* Auto-install after DOM settles */
if(typeof document !== 'undefined'){
  document.addEventListener('DOMContentLoaded', _discInstallSourceWatchers);
  setTimeout(_discInstallSourceWatchers, 600);
  // Auto-load schemas once config has settled (config loads async ~1-2s after page)
  setTimeout(()=>{ if(_DISC_LIVE_SUPPORTED.has((_discReadSourceConfig().source_type||''))) discLoadSchemas(); }, 2500);
}

async function discRunScan(){
  const btn = G('discScanBtn');
  const srcType = G('discSourceSelect')?.value || 'static';
  btn.disabled = true;
  btn.innerHTML = '<svg style="width:16px;height:16px;animation:discSpin 1s linear infinite;" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" fill="none" stroke="currentColor" stroke-width="2.5" stroke-dasharray="32" stroke-linecap="round"/></svg> Scanning…';
  G('discStatus').textContent = 'Running…';
  G('discStatus').className = 'disc-status running';

  const body = { source: srcType };

  // Pull source config from the Settings card (cfg*) — the authoritative
  // "Source Database" card — with legacy fallback. This guarantees Discovery
  // connects using whatever the user configured (e.g. Azure SQL).
  if(srcType === 'live' || srcType === 'both'){
    const cfg = _discReadSourceConfig();
    if(!_DISC_LIVE_SUPPORTED.has(cfg.source_type)){
      btn.disabled = false;
      btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:16px;height:16px;stroke:currentColor;fill:none;stroke-width:2.5;"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg> Run Discovery Scan';
      G('discStatus').textContent = 'Unsupported'; G('discStatus').className = 'disc-status fail';
      _discToast('Live Discovery scan currently supports Azure SQL, SQL Server, Synapse, Snowflake, Redshift, SharePoint and REST API. Your Settings source is "'+cfg.source_type+'". Switch to Static Demo Objects to preview.', 'err');
      return;
    }
    const _isNonSql = _DISC_NON_SQL.has(cfg.source_type);
    const _needsServer = cfg.source_type!=='snowflake' && !_isNonSql && !cfg.server;
    const _needsAccount = cfg.source_type==='snowflake' && !cfg.account;
    const _needsUrl = _isNonSql && !cfg.server;
    const _needsDb = !cfg.database && !_isNonSql;
    if(_needsServer || _needsAccount || _needsUrl || _needsDb){
      btn.disabled = false;
      btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:16px;height:16px;stroke:currentColor;fill:none;stroke-width:2.5;"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg> Run Discovery Scan';
      G('discStatus').textContent = 'Missing config'; G('discStatus').className = 'disc-status fail';
      const hint = cfg.source_type==='snowflake' ? 'Set Account and Database in Settings → Source Database first.'
        : _isNonSql ? 'Set the source URL in Settings → Source Database first.'
        : 'Set Server and Database in Settings → Source Database first.';
      _discToast(hint, 'err');
      return;
    }
    body.source_config = cfg;
    const _selSchema = G('discSchemaSelect')?.value || '';
    if(_selSchema) body.schema_filter = _selSchema;
  }

  try {
    const controller = new AbortController();
    const tid = setTimeout(()=>controller.abort(), 120000);
    const r = await fetch('/api/v1/discovery/scan', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    clearTimeout(tid);
    const d = await r.json();
    if(!d.success) throw new Error(d.error || 'Scan failed');

    _discReport = d.report;
    _discRender();
    G('discStatus').textContent = 'Completed';
    G('discStatus').className = 'disc-status done';
    G('discLastScan').textContent = new Date().toLocaleString();
    _discToast('Discovery scan complete — ' + (_discReport.total_objects||0) + ' objects analysed', 'ok');
    if(typeof discProfileLoadTables === 'function') discProfileLoadTables();
  } catch(e) {
    G('discStatus').textContent = 'Failed';
    G('discStatus').className = 'disc-status fail';
    _discToast('Scan failed: ' + e.message, 'err');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:16px;height:16px;stroke:currentColor;fill:none;stroke-width:2.5;"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg> Run Discovery Scan';
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   RENDER ALL SECTIONS
   ═══════════════════════════════════════════════════════════════════════════ */
function _discRender(){
  if(!_discReport) return;
  _discRenderKPI();
  _discRenderBOM();
  _discRenderCharts();
  _discRenderGraph();
}

/* ── KPI Cards ── */
function _discRenderKPI(){
  const r = _discReport;
  const bt = r.by_type || {};
  const bk = r.by_tier || {};
  const br = r.by_risk || {};

  G('discKpiTotal').textContent     = r.total_objects || 0;
  G('discKpiSP').textContent        = bt.stored_procedure || 0;
  G('discKpiViews').textContent     = bt.view || 0;
  G('discKpiUDFs').textContent      = bt.udf || 0;
  G('discKpiTables').textContent    = bt.table || 0;
  G('discKpiSimple').textContent    = bk.Simple || 0;
  G('discKpiModerate').textContent  = bk.Moderate || 0;
  G('discKpiComplex').textContent   = bk.Complex || 0;
  G('discKpiVComplex').textContent  = bk['Very Complex'] || 0;
  G('discKpiEffort').textContent    = r.total_effort_points || 0;
  G('discKpiAvgScore').textContent  = r.avg_complexity_score || 0;
  G('discKpiReadiness').textContent = (r.readiness_pct || 0) + '%';
  G('discKpiHighRisk').textContent  = (br.High || 0) + (br.Critical || 0);
}

/* ── BOM Table ── */
function _discRenderBOM(){
  const tbody = G('discBomBody');
  if(!tbody) return;
  const objs = _discReport.objects || [];
  const fType = G('discFilterType')?.value || 'all';
  const fTier = G('discFilterTier')?.value || 'all';
  const fRisk = G('discFilterRisk')?.value || 'all';
  const search = (G('discSearch')?.value || '').toLowerCase();

  let filtered = objs.filter(a => {
    if(fType !== 'all' && a.object_type !== fType) return false;
    if(fTier !== 'all' && a.complexity_tier !== fTier) return false;
    if(fRisk !== 'all' && a.risk_level !== fRisk) return false;
    if(search && !a.name.toLowerCase().includes(search)) return false;
    return true;
  });

  // Update count badge
  const cntEl = G('discBomCount');
  if(cntEl) cntEl.textContent = filtered.length + ' of ' + objs.length + ' objects';

  const tierCls = {Simple:'green',Moderate:'yellow',Complex:'orange','Very Complex':'red'};
  const riskCls = {Low:'green',Medium:'yellow',High:'orange',Critical:'red'};
  const readCls = {'Auto-Convert Ready':'green','Manual Review Required':'yellow','Requires Rewrite':'red','Good to Migrate':'green','Minor Fix Needed':'yellow','Needs Fix Before Migration':'orange','Major Rework Required':'red'};
  const typeIco = {stored_procedure:'⚙️',view:'👁️',udf:'ƒ',table:'🗃️'};

  tbody.innerHTML = filtered.map((a,i) => {
    const isTable = a.object_type === 'table';
    const col4 = isTable ? (a.constructs?.column_count || a.column_count || '-') : a.param_count;
    const col5 = isTable ? (a.constructs?.row_count || a.row_count || 0).toLocaleString() : a.table_ref_count;
    return `<tr class="disc-bom-row" data-name="${_he(a.name)}" onclick="discShowRCA('${_he(a.name)}')">
    <td style="color:var(--t3);font-size:11px;font-weight:600;">${i+1}</td>
    <td><strong style="color:var(--t1);">${_he(a.name)}</strong></td>
    <td><span style="font-size:11px;">${typeIco[a.object_type]||''} ${_he(a.object_type)}</span></td>
    <td>${a.line_count}</td>
    <td>${col4}</td>
    <td>${col5}</td>
    <td><strong style="font-size:13px;">${a.complexity_score}</strong></td>
    <td><span class="disc-tag ${tierCls[a.complexity_tier]||''}">${_he(a.complexity_tier)}</span></td>
    <td>${_he(a.effort_hours)}h</td>
    <td><span class="disc-tag ${riskCls[a.risk_level]||''}">${_he(a.risk_level)}</span></td>
    <td><span class="disc-tag ${readCls[a.migration_readiness]||''}">${_he(a.migration_readiness)}</span></td>
    <td><button style="background:none;border:none;cursor:pointer;font-size:14px;padding:2px 6px;" title="View RCA Details" onclick="event.stopPropagation();discShowRCA('${_he(a.name)}')">🔍</button></td>
  </tr>`}).join('');
}

/* ── Charts (Chart.js) ── */
let _discDonut = null;
let _discBar   = null;

function _discRenderCharts(){
  if(typeof Chart === 'undefined') return;
  _discRenderDonut();
  _discRenderBar();
}

function _discRenderDonut(){
  const ctx = G('discChartDonut');
  if(!ctx) return;
  const bt = _discReport.by_tier || {};
  const data = [bt.Simple||0, bt.Moderate||0, bt.Complex||0, bt['Very Complex']||0];
  if(_discDonut) _discDonut.destroy();
  _discDonut = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: ['Simple','Moderate','Complex','Very Complex'],
      datasets: [{data, backgroundColor:['#38a169','#d69e2e','#dd6b20','#e53e3e'], borderWidth:0}],
    },
    options: {
      responsive:true, maintainAspectRatio:false,
      plugins:{legend:{position:'bottom',labels:{color:'#4a5568',font:{size:11}}}},
      cutout:'60%',
    },
  });
}

function _discRenderBar(){
  const ctx = G('discChartBar');
  if(!ctx) return;
  const objs = (_discReport.objects||[]).slice(0,15); // top 15
  if(_discBar) _discBar.destroy();
  const tierColor = {Simple:'#38a169',Moderate:'#d69e2e',Complex:'#dd6b20','Very Complex':'#e53e3e'};
  _discBar = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: objs.map(a=>a.name.length>25?a.name.substring(0,22)+'…':a.name),
      datasets: [{
        label: 'Complexity Score',
        data: objs.map(a=>a.complexity_score),
        backgroundColor: objs.map(a=>tierColor[a.complexity_tier]||'#718096'),
        borderRadius: 4,
      }],
    },
    options: {
      indexAxis:'y', responsive:true, maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{x:{grid:{color:'#e2e8f020'},ticks:{color:'#718096'}},y:{ticks:{color:'#4a5568',font:{size:11}}}},
    },
  });
}

/* ═══════════════════════════════════════════════════════════════════════════
   DEPENDENCY GRAPH (D3.js v7 force-directed)
   ═══════════════════════════════════════════════════════════════════════════ */
function _discRenderGraph(){
  const container = G('discGraphSvg');
  if(!container || typeof d3 === 'undefined') return;
  container.innerHTML = '';

  const graph = _discReport.dependency_graph;
  if(!graph || !graph.nodes || graph.nodes.length === 0) {
    container.innerHTML = '<text x="50%" y="50%" text-anchor="middle" fill="#718096" font-size="13">No dependency data — run a scan first</text>';
    return;
  }

  // Size the canvas larger than the viewport so graph has room to breathe + becomes scrollable
  const n = graph.nodes.length;
  const box = container.parentElement;
  const visW = (box && box.clientWidth)  || 900;
  const visH = (box && box.clientHeight) || 440;
  const width  = Math.max(visW, 260 + n * 90);
  const height = Math.max(visH, 220 + n * 55);

  const svg = d3.select(container)
    .attr('viewBox', [0, 0, width, height])
    .attr('width', width)
    .attr('height', height)
    .style('min-width', width + 'px')
    .style('min-height', height + 'px');

  // Arrow marker
  svg.append('defs').append('marker')
    .attr('id','arrowhead').attr('viewBox','0 -5 10 10')
    .attr('refX',22).attr('refY',0)
    .attr('markerWidth',6).attr('markerHeight',6)
    .attr('orient','auto')
    .append('path').attr('d','M0,-5L10,0L0,5').attr('fill','#a0aec0');

  const sim = d3.forceSimulation(graph.nodes)
    .force('link', d3.forceLink(graph.edges).id(d=>d.id).distance(120))
    .force('charge', d3.forceManyBody().strength(-300))
    .force('center', d3.forceCenter(width/2, height/2))
    .force('collision', d3.forceCollide(30));

  const link = svg.append('g')
    .selectAll('line')
    .data(graph.edges)
    .join('line')
    .attr('stroke','#a0aec0')
    .attr('stroke-width',1.5)
    .attr('marker-end','url(#arrowhead)');

  const node = svg.append('g')
    .selectAll('g')
    .data(graph.nodes)
    .join('g')
    .call(d3.drag()
      .on('start', (e,d)=>{if(!e.active)sim.alphaTarget(.3).restart();d.fx=d.x;d.fy=d.y;})
      .on('drag',  (e,d)=>{d.fx=e.x;d.fy=e.y;})
      .on('end',   (e,d)=>{if(!e.active)sim.alphaTarget(0);d.fx=null;d.fy=null;})
    );

  node.append('circle')
    .attr('r', d => 8 + Math.min(d.score / 5, 12))
    .attr('fill', d => d.color)
    .attr('stroke', d => d.tierColor)
    .attr('stroke-width', 2.5)
    .attr('cursor','pointer')
    .on('click', (e, d) => discShowRCA(d.id));

  node.append('text')
    .text(d => d.label.length > 20 ? d.label.substring(0,18)+'…' : d.label)
    .attr('x', 0)
    .attr('y', d => -(12 + Math.min(d.score / 5, 12)))
    .attr('text-anchor','middle')
    .attr('font-size','10px')
    .attr('fill','#4a5568');

  sim.on('tick', () => {
    link.attr('x1',d=>d.source.x).attr('y1',d=>d.source.y)
        .attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);
    node.attr('transform', d=>`translate(${d.x},${d.y})`);
  });

  _d3Sim = sim;
}

/* ═══════════════════════════════════════════════════════════════════════════
   RCA SLIDE-IN PANEL
   ═══════════════════════════════════════════════════════════════════════════ */
function discShowRCA(name){
  var panel = document.getElementById('discRcaPanel');
  var overlay = document.getElementById('discRcaOverlay');
  if(!panel){ alert('RCA panel element not found'); return; }
  if(!_discReport){ alert('No scan data loaded'); return; }
  const obj = (_discReport.objects||[]).find(a => a.name === name);
  if(!obj){ discCloseRCA(); return; }

  const tierCls = {Simple:'green',Moderate:'yellow',Complex:'orange','Very Complex':'red'};
  const riskCls = {Low:'green',Medium:'yellow',High:'orange',Critical:'red'};
  const readCls = {'Auto-Convert Ready':'green','Manual Review Required':'yellow','Requires Rewrite':'red','Good to Migrate':'green','Minor Fix Needed':'yellow','Needs Fix Before Migration':'orange','Major Rework Required':'red'};

  let html = `<div class="disc-rca-header">
    <div>
      <h3>${_he(obj.name)}</h3>
      <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap;">
        <span class="disc-tag ${tierCls[obj.complexity_tier]||''}">${_he(obj.complexity_tier)}</span>
        <span class="disc-tag ${riskCls[obj.risk_level]||''}">${_he(obj.risk_level)}</span>
        <span class="disc-tag" style="background:#EFF6FF;color:#1D4ED8;">Score: ${obj.complexity_score}</span>
      </div>
    </div>
    <button class="disc-rca-close" onclick="discCloseRCA()" title="Close">✕</button>
  </div>`;

  html += `<div class="disc-rca-meta">
    <div><strong>Type:</strong> ${_he(obj.object_type)}</div>
    <div><strong>Lines:</strong> ${obj.line_count}</div>`;
  if(obj.object_type === 'table'){
    html += `<div><strong>Columns:</strong> ${obj.constructs?.column_count || obj.column_count || '-'}</div>`;
    html += `<div><strong>Rows:</strong> ${(obj.constructs?.row_count || obj.row_count || 0).toLocaleString()}</div>`;
    html += `<div><strong>Indexes:</strong> ${obj.constructs?.index_count || 0}</div>`;
    html += `<div><strong>Foreign Keys:</strong> ${obj.constructs?.fk_count || 0}</div>`;
  } else {
    html += `<div><strong>Parameters:</strong> ${obj.param_count}</div>`;
    html += `<div><strong>Table References:</strong> ${obj.table_ref_count}</div>`;
    html += `<div><strong>Object References:</strong> ${obj.obj_ref_count}</div>`;
  }
  html += `<div><strong>Complexity Score:</strong> ${obj.complexity_score}</div>
    <div><strong>Estimated Effort:</strong> ${_he(obj.effort_hours)} hours (${obj.effort_points} story pts)</div>
    <div><strong>Readiness:</strong> <span class="disc-tag ${readCls[obj.migration_readiness]||''}">${_he(obj.migration_readiness)}</span></div>
  </div>`;

  // Construct breakdown
  const cs = obj.constructs || {};
  let constructRows;
  if(obj.object_type === 'table'){
    constructRows = [
      ['Identity Column', cs.has_identity, '-'],
      ['Computed Columns', cs.has_computed_cols, '-'],
      ['Primary Key', cs.has_primary_key, '-'],
      ['Foreign Keys', cs.has_foreign_keys, cs.fk_count || '-'],
      ['CHECK Constraints', cs.has_check_constraints, cs.check_count || '-'],
      ['UNIQUE Constraints', cs.has_unique, '-'],
      ['Default Values', cs.has_defaults, '-'],
      ['Triggers', cs.has_triggers, '-'],
      ['Temporal (System-Versioned)', cs.has_temporal, '-'],
      ['FILESTREAM / FileTable', cs.has_filestream, '-'],
      ['Memory-Optimized', cs.has_memory_optimized, '-'],
      ['Partitioning', cs.has_partitioning, '-'],
      ['Columnstore Index', cs.has_columnstore, '-'],
      ['XML Index', cs.has_xml_index, '-'],
      ['Spatial Index', cs.has_spatial_index, '-'],
      ['Unsupported Data Types', cs.unsupported_dtype_count > 0, cs.unsupported_dtype_count || '-'],
    ];
  } else {
    constructRows = [
      ['CTE', cs.cte, cs.cte_count],
      ['Cursor', cs.cursor, cs.cursor_count],
      ['Temp Tables', cs.temp_table, cs.temp_table_count],
      ['MERGE', cs.merge, cs.merge_count],
      ['Dynamic SQL', cs.dynamic_sql, '-'],
      ['Window Functions', cs.window_functions, cs.window_count],
      ['PIVOT/UNPIVOT', cs.pivot, '-'],
      ['TRY-CATCH', cs.try_catch, '-'],
      ['Transactions', cs.transactions, '-'],
      ['CROSS/OUTER APPLY', cs.cross_apply, '-'],
      ['EXISTS Subquery', cs.exists_subquery, '-'],
      ['OPENQUERY', cs.openquery, '-'],
      ['GOTO', cs.goto, '-'],
      ['WHILE Loop', cs.while_loop, '-'],
      ['WITH(NOLOCK)', cs.nolock, '-'],
    ];
  }

  html += `<h4>Construct Breakdown</h4>
  <table class="disc-rca-table"><thead><tr><th>Construct</th><th>Detected</th><th>Count</th></tr></thead><tbody>`;
  for(const [label, detected, count] of constructRows){
    const cls = detected ? 'disc-detected' : '';
    html += `<tr class="${cls}"><td>${label}</td><td>${detected ? '✓ Yes' : '—'}</td><td>${detected ? count : '—'}</td></tr>`;
  }
  html += '</tbody></table>';

  // Unsupported flags
  if(obj.unsupported_flags && obj.unsupported_flags.length > 0){
    html += '<h4>Unsupported in PySpark</h4><ul class="disc-rca-flags">';
    for(const f of obj.unsupported_flags){
      html += `<li>⚠ ${_he(f)}</li>`;
    }
    html += '</ul>';
  }

  // Risk factors
  if(obj.risk_factors && obj.risk_factors.length > 0){
    html += '<h4>Risk Factors</h4><ul class="disc-rca-flags">';
    for(const f of obj.risk_factors){
      html += `<li>🔴 ${_he(f)}</li>`;
    }
    html += '</ul>';
  }

  // RCA
  if(obj.rca && obj.rca.length > 0){
    html += '<h4>Root Cause Analysis — Why This Object Is Complex</h4>';
    html += '<table class="disc-rca-table"><thead><tr><th>Construct</th><th>Impact</th><th>Root Cause</th></tr></thead><tbody>';
    for(const r of obj.rca){
      html += `<tr><td><strong>${_he(r.construct)}</strong></td><td>${_he(r.impact)}</td><td>${_he(r.reason)}</td></tr>`;
    }
    html += '</tbody></table>';
  }

  // Table references
  if(obj.table_references && obj.table_references.length > 0){
    html += '<h4>Referenced Tables</h4><div class="disc-rca-refs">' + obj.table_references.map(t=>`<span class="disc-ref-chip">${_he(t)}</span>`).join('') + '</div>';
  }

  // Source Code (CREATE statement)
  if(obj.code && obj.code.trim()){
    html += '<h4>\u{1F4DC} SQL Source Code (CREATE Statement)</h4>';
    html += '<pre style="background:#1e293b;color:#e2e8f0;padding:14px;border-radius:8px;font-size:11px;overflow-x:auto;max-height:400px;overflow-y:auto;white-space:pre-wrap;word-break:break-word;line-height:1.5;">' + _he(obj.code) + '</pre>';
  }

  panel.innerHTML = html;
  panel.classList.add('open');
  if(overlay) overlay.classList.add('open');
}

function discCloseRCA(){
  const p = G('discRcaPanel');
  const o = G('discRcaOverlay');
  if(p) p.classList.remove('open');
  if(o) o.classList.remove('open');
}

/* ── HTML escape ── */
function _he(s){ return s==null?'':String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

/* ═══════════════════════════════════════════════════════════════════════════
   EXPORTS
   ═══════════════════════════════════════════════════════════════════════════ */
window.discExportHTML = function(){
  window.open('/api/v1/discovery/export/html','_blank');
};
window.discExportBOM = function(){
  window.open('/api/v1/discovery/export/bom','_blank');
};

/* ═══════════════════════════════════════════════════════════════════════════
   INIT — called when tab is switched to discovery
   ═══════════════════════════════════════════════════════════════════════════ */
function discInit(){
  if(_discReport){ _discRender(); }
  else {
    fetch('/api/v1/discovery/results',{method:'GET'})
      .then(function(r){return r.json();})
      .then(function(d){
        if(d.success && d.report){ _discReport=d.report; _discRender();
          var s=document.getElementById('discStatus'); if(s){s.textContent='Completed';s.className='disc-status done';}
        }
      }).catch(function(){});
  }
  _discUpdateSourceBadge();
}

/* ═══════════════════════════════════════════════════════════════════════════
   DATA PROFILE — column-level stats, PII detection, suggested DQ rules
   ═══════════════════════════════════════════════════════════════════════════ */

let _discProfileCurrent = null;   // currently-open profile
let _discProfileTables  = [];     // list of profilable tables

function _discProfileMode(){
  // Reuse the Discovery source selector — "static" => demo profile,
  // "live"/"both" => live profile against Settings source config.
  const s = G('discSourceSelect')?.value || 'static';
  return (s === 'live' || s === 'both') ? 'live' : 'demo';
}

async function discProfileLoadTables(){
  const box = G('discProfileTableList');
  const mode = _discProfileMode();
  const badge = G('discProfileSourceBadge');
  if(badge){
    if(mode === 'live'){
      const cfg = (typeof _discReadSourceConfig === 'function') ? _discReadSourceConfig() : {};
      badge.textContent = (cfg.source_type || 'live') + ' · ' + (cfg.database || '<no db>');
      badge.style.background = 'rgba(59,130,246,.1)';
      badge.style.color = '#1D4ED8';
    } else {
      badge.textContent = 'demo · built-in sample tables';
      badge.style.background = 'rgba(148,163,184,.12)';
      badge.style.color = 'var(--t2)';
    }
  }
  box.innerHTML = '<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">Loading tables…</div>';
  const body = { mode };
  if(mode === 'live' && typeof _discReadSourceConfig === 'function'){
    body.source_config = _discReadSourceConfig();
    const _selSchema = G('discSchemaSelect')?.value || '';
    if(_selSchema) body.schema_filter = _selSchema;
  }
  try{
    const r = await fetch('/api/v1/discovery/profile/tables', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if(!d.success) throw new Error(d.error || 'Failed');
    _discProfileTables = d.tables || [];
    _discRenderProfileTableList();
  }catch(e){
    box.innerHTML = '<div style="padding:24px;text-align:center;color:#EF4444;font-size:11px;">Error: '+e.message+'</div>';
  }
}

function _discRenderProfileTableList(){
  const box = G('discProfileTableList');
  if(!_discProfileTables.length){
    box.innerHTML = '<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">No tables found</div>';
    return;
  }
  let html = '<table style="width:100%;border-collapse:collapse;font-size:11px;">'+
    '<thead><tr style="background:var(--bg2);border-bottom:1px solid var(--border);">'+
    '<th style="padding:8px 10px;text-align:left;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;font-size:10px;">Table</th>'+
    '<th style="padding:8px 10px;text-align:right;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;font-size:10px;">Rows</th>'+
    '<th style="padding:8px 10px;text-align:right;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;font-size:10px;">Columns</th>'+
    '<th style="padding:8px 10px;text-align:left;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;font-size:10px;">Description</th>'+
    '<th style="padding:8px 10px;text-align:center;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;font-size:10px;">Action</th>'+
    '</tr></thead><tbody>';
  _discProfileTables.forEach((t,i)=>{
    const tesc = (t.name||'').replace(/'/g,"\\'");
    const sesc = (t.schema||'').replace(/'/g,"\\'");
    const displayName = t.full_name || t.name;
    html += '<tr style="border-bottom:1px solid var(--border);cursor:pointer;" onmouseover="this.style.background=\'rgba(236,72,153,.05)\'" onmouseout="this.style.background=\'transparent\'" onclick="discProfileOpen(\''+tesc+'\',\''+sesc+'\')">'+
      '<td style="padding:8px 10px;font-family:\'SF Mono\',Consolas,monospace;color:var(--t1);font-weight:600;">'+displayName+'</td>'+
      '<td style="padding:8px 10px;text-align:right;color:var(--t2);">'+Number(t.row_count||0).toLocaleString()+'</td>'+
      '<td style="padding:8px 10px;text-align:right;color:var(--t2);">'+(t.column_count||'—')+'</td>'+
      '<td style="padding:8px 10px;color:var(--t3);font-size:10px;">'+(t.description||'').slice(0,70)+'</td>'+
      '<td style="padding:8px 10px;text-align:center;"><button class="btn btn-ghost btn-xs" style="font-size:10px;color:#EC4899;border:1px solid #EC489944;" onclick="event.stopPropagation();discProfileOpen(\''+tesc+'\',\''+sesc+'\')">📊 Profile</button></td>'+
      '</tr>';
  });
  html += '</tbody></table>';
  box.innerHTML = html;
}

async function discProfileOpen(tableName, schema){
  const panel = G('discProfilePanel');
  const overlay = G('discProfileOverlay');
  panel.innerHTML = '<button onclick="discProfileClose()" title="Close (Esc)" style="position:fixed;top:14px;right:14px;z-index:1002;background:#fff;border:1px solid var(--border);box-shadow:0 4px 12px rgba(0,0,0,.15);border-radius:50%;width:36px;height:36px;font-size:20px;color:#EC4899;cursor:pointer;display:flex;align-items:center;justify-content:center;font-weight:700;line-height:1;">×</button>'+
    '<div style="padding:32px;text-align:center;color:var(--t3);">'+
    '<div style="font-size:20px;margin-bottom:12px;">⏳</div>Profiling <b>'+tableName+'</b>…</div>';
  panel.style.right = '0';
  overlay.classList.add('active');
  overlay.style.display = 'block';
  // Install Esc-to-close once
  if(!window._discProfileEscBound){
    window._discProfileEscBound = true;
    document.addEventListener('keydown', (e)=>{
      if(e.key==='Escape' && G('discProfilePanel')?.style.right==='0px'){ discProfileClose(); }
    });
  }

  const body = { mode: _discProfileMode() };
  if(body.mode === 'live' && typeof _discReadSourceConfig === 'function'){
    body.source_config = _discReadSourceConfig();
    if(schema) body.schema = schema;
  }
  try{
    const r = await fetch('/api/v1/discovery/profile/'+encodeURIComponent(tableName), {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if(!d.success) throw new Error(d.error || 'Failed');
    _discProfileCurrent = d.profile;
    _discRenderProfilePanel(d.profile);
  }catch(e){
    panel.innerHTML = '<div style="padding:32px;color:#EF4444;">Error: '+e.message+'<br><br><button class="btn btn-ghost" onclick="discProfileClose()">Close</button></div>';
  }
}

function discProfileClose(){
  G('discProfilePanel').style.right = '-720px';
  const ov = G('discProfileOverlay');
  ov.classList.remove('active');
  ov.style.display = 'none';
}

/* ── Render the slide-in profile panel ─────────────────────────────────── */
function _discRenderProfilePanel(prof){
  const panel = G('discProfilePanel');
  const totalRules = (prof.columns||[]).reduce((a,c)=>a+(c.suggested_rules||[]).length,0);
  const totalPII   = (prof.columns||[]).filter(c=>(c.pii_tags||[]).length>0).length;

  let html = '';
  // Always-visible floating close button (independent of sticky header / scroll)
  html += '<button onclick="discProfileClose()" title="Close (Esc)" style="position:fixed;top:14px;right:14px;z-index:1002;background:#fff;border:1px solid var(--border);box-shadow:0 4px 12px rgba(0,0,0,.15);border-radius:50%;width:36px;height:36px;font-size:20px;color:#EC4899;cursor:pointer;display:flex;align-items:center;justify-content:center;font-weight:700;line-height:1;">×</button>';
  // Header
  html += '<div style="padding:14px 22px 16px 22px;border-bottom:1px solid var(--border);background:linear-gradient(135deg,rgba(236,72,153,.06),rgba(139,92,246,.06));position:sticky;top:0;z-index:10;">';
  html += '<div style="display:flex;align-items:center;gap:10px;padding-right:56px;">';
  html += '<div style="font-size:10px;color:#EC4899;font-weight:800;letter-spacing:.12em;text-transform:uppercase;">📊 Data Profile</div>';
  html += '<button onclick="discProfileClose()" style="margin-left:auto;background:#fff;border:1px solid var(--border);border-radius:6px;padding:4px 10px;font-size:11px;color:var(--t2);cursor:pointer;">✕ Close</button>';
  html += '</div>';
  html += '<div style="font-size:22px;font-weight:700;color:var(--t1);margin-top:10px;font-family:\'SF Mono\',Consolas,monospace;line-height:1.25;word-break:break-all;">'+prof.table+'</div>';
  html += '<div style="font-size:11px;color:var(--t3);margin-top:4px;">'+(prof.description||'')+'</div>';
  // KPI strip
  html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:14px;">';
  html += _profKpi('Rows', Number(prof.row_count||0).toLocaleString(), '#3B82F6');
  html += _profKpi('Columns', prof.column_count||0, '#8B5CF6');
  html += _profKpi('PII Columns', totalPII, totalPII>0?'#EC4899':'#10B981');
  html += _profKpi('Suggested Rules', totalRules, '#F59E0B');
  html += '</div>';
  // Actions
  html += '<div style="display:flex;gap:8px;margin-top:12px;">';
  html += '<button class="btn btn-primary btn-xs" onclick="discProfilePromoteAll()" style="background:#EC4899;border-color:#EC4899;">✓ Promote All '+totalRules+' Rules to DQ</button>';
  html += '<button class="btn btn-ghost btn-xs" onclick="discProfileExport()">⬇ Export JSON</button>';
  html += '<button class="btn btn-ghost btn-xs" onclick="discProfileExportCSV()">📊 Export CSV</button>';
  html += '</div>';
  html += '</div>';

  // Column cards
  html += '<div style="padding:16px 22px;">';
  html += '<div style="font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:var(--t3);margin-bottom:8px;">Column Statistics</div>';
  (prof.columns||[]).forEach(c => { html += _profColumnCard(c, prof.table); });
  html += '</div>';

  panel.innerHTML = html;
}

function _profKpi(label, val, color){
  return '<div style="background:#fff;border:1px solid '+color+'33;border-radius:8px;padding:10px 12px;">'+
    '<div style="font-size:9px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:'+color+';">'+label+'</div>'+
    '<div style="font-size:20px;font-weight:700;color:var(--t1);margin-top:3px;">'+val+'</div></div>';
}

function _profColumnCard(c, table){
  const piiTags = (c.pii_tags||[]);
  const rules   = (c.suggested_rules||[]);
  const top     = (c.top_values||[]).slice(0,5);
  const maxCnt  = top[0]?.count || 1;

  const border = piiTags.length ? '#EC4899' : (rules.length ? '#F59E0B' : 'var(--border)');
  const bgHue  = piiTags.length ? 'rgba(236,72,153,.04)' : (rules.length ? 'rgba(245,158,11,.04)' : '#fff');

  let h = '<div style="background:'+bgHue+';border:1px solid '+border+';border-radius:10px;padding:12px 14px;margin-bottom:10px;">';
  // Header row
  h += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;flex-wrap:wrap;">';
  h += '<span style="font-family:\'SF Mono\',Consolas,monospace;font-weight:700;font-size:12px;color:var(--t1);">'+c.name+'</span>';
  h += '<span style="font-size:10px;color:var(--t4);font-family:monospace;">'+c.data_type+'</span>';
  if(c.type_class){
    const tcColor = {numeric:'#3B82F6', string:'#8B5CF6', date:'#14B8A6', other:'#94A3B8'}[c.type_class]||'#94A3B8';
    h += '<span style="font-size:9px;padding:1px 6px;border-radius:6px;background:'+tcColor+'22;color:'+tcColor+';font-weight:700;">'+c.type_class.toUpperCase()+'</span>';
  }
  piiTags.forEach(p => {
    h += '<span style="font-size:9px;padding:1px 7px;border-radius:6px;background:#EC489922;color:#EC4899;font-weight:700;letter-spacing:.05em;">⚠ '+p.toUpperCase()+'</span>';
  });
  h += '</div>';

  // Stat grid
  h += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;font-size:11px;">';
  h += _profStat('Null %',    (c.null_pct||0).toFixed(2)+'%', c.null_pct>20?'#EF4444':(c.null_pct>5?'#F59E0B':'#10B981'));
  h += _profStat('Distinct',  Number(c.distinct_count||0).toLocaleString(), '#3B82F6');
  h += _profStat('Min',       _trunc(c.min, 15), 'var(--t2)');
  h += _profStat('Max',       _trunc(c.max, 15), 'var(--t2)');
  h += '</div>';

  // Top values histogram (sparkline)
  if(top.length){
    h += '<div style="margin-top:10px;">';
    h += '<div style="font-size:9px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--t3);margin-bottom:4px;">Top Values</div>';
    top.forEach(tv => {
      const pct = Math.max(4, Math.round((tv.count/maxCnt)*100));
      h += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:3px;font-size:10px;">';
      h += '<span style="flex-basis:120px;font-family:monospace;color:var(--t2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="'+String(tv.value)+'">'+_trunc(tv.value,18)+'</span>';
      h += '<div style="flex:1;height:12px;background:var(--bg2);border-radius:3px;position:relative;overflow:hidden;">';
      h += '<div style="width:'+pct+'%;height:100%;background:linear-gradient(90deg,#8B5CF6,#EC4899);"></div>';
      h += '</div>';
      h += '<span style="flex-basis:70px;text-align:right;color:var(--t3);font-variant-numeric:tabular-nums;">'+Number(tv.count).toLocaleString()+'</span>';
      h += '</div>';
    });
    h += '</div>';
  }

  // Suggested rules
  if(rules.length){
    h += '<div style="margin-top:10px;padding-top:10px;border-top:1px dashed var(--border);">';
    h += '<div style="font-size:9px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:#F59E0B;margin-bottom:6px;">🎯 Suggested Rules ('+rules.length+')</div>';
    rules.forEach((rule, i) => {
      const sevColor = {critical:'#EC4899', error:'#EF4444', warn:'#F59E0B', info:'#3B82F6'}[rule.severity]||'#94A3B8';
      h += '<div style="background:#fff;border:1px solid '+sevColor+'22;border-radius:6px;padding:6px 10px;margin-bottom:5px;">';
      h += '<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px;">';
      h += '<span style="font-size:9px;padding:1px 6px;border-radius:4px;background:'+sevColor+'22;color:'+sevColor+';font-weight:800;">'+rule.severity.toUpperCase()+'</span>';
      h += '<span style="font-size:10px;font-weight:700;color:var(--t2);">'+rule.rule_type+'</span>';
      h += '</div>';
      h += '<div style="font-family:monospace;font-size:10px;color:var(--t1);background:var(--bg2);padding:3px 6px;border-radius:3px;margin:3px 0;">'+rule.expression+'</div>';
      h += '<div style="font-size:10px;color:var(--t3);line-height:1.4;">'+rule.reason+'</div>';
      h += '</div>';
    });
    h += '</div>';
  }

  h += '</div>';
  return h;
}

function _profStat(label, val, color){
  return '<div style="background:#fff;border:1px solid var(--border);border-radius:6px;padding:5px 8px;">'+
    '<div style="font-size:8px;font-weight:800;letter-spacing:.06em;text-transform:uppercase;color:var(--t4);">'+label+'</div>'+
    '<div style="font-size:12px;font-weight:700;color:'+color+';margin-top:2px;font-family:monospace;">'+val+'</div></div>';
}

function _trunc(s, n){
  if(s===null||s===undefined) return '—';
  const v = String(s); return v.length>n ? v.slice(0,n-1)+'…' : v;
}

/* ── Promote suggested rules to the Data Quality cart ──────────────────── */
async function discProfilePromoteAll(){
  if(!_discProfileCurrent){_discToast('No profile open', 'err');return;}
  const table = _discProfileCurrent.table;
  try{
    const r = await fetch('/api/v1/discovery/profile/'+encodeURIComponent(table)+'/rules');
    const d = await r.json();
    if(!d.success) throw new Error(d.error||'Failed');
    // Persist in browser so DQ cart can pick them up
    const key = 'dq_pending_rules_v1';
    const existing = (()=>{try{return JSON.parse(localStorage.getItem(key)||'[]');}catch(e){return [];}})();
    const merged = existing.concat(d.rules.map(r=>({...r, source:'discovery_profile', added_at:new Date().toISOString()})));
    localStorage.setItem(key, JSON.stringify(merged));
    _discToast('Promoted '+d.rules.length+' rules to Data Quality cart', 'ok');
    // Navigate to DQ cart
    if(typeof switchTab === 'function'){
      setTimeout(()=>switchTab('wf-dq', G('nav-wf-dq')), 300);
    }
  }catch(e){_discToast('Promote failed: '+e.message, 'err');}
}

function discProfileExport(){
  if(!_discProfileCurrent) return;
  const blob = new Blob([JSON.stringify(_discProfileCurrent, null, 2)], {type:'application/json'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'profile_'+_discProfileCurrent.table+'.json';
  a.click(); URL.revokeObjectURL(a.href);
}

function discProfileExportCSV(){
  if(!_discProfileCurrent) return;
  const prof = _discProfileCurrent;
  const esc = v => {
    if(v===null||v===undefined) return '';
    const s = String(v).replace(/"/g,'""');
    return /[",\r\n]/.test(s) ? '"'+s+'"' : s;
  };
  const headers = [
    'table','description','row_count','column_count',
    'column','data_type','category','nullable',
    'null_pct','distinct_count','distinct_pct',
    'min','max','avg','stddev',
    'min_length','max_length','avg_length',
    'top_value','top_value_pct',
    'pii_tags','pii_severity',
    'rule_count','suggested_rules','rule_expressions','rule_severities'
  ];
  const lines = [headers.join(',')];
  const cols = prof.columns || [];
  if(cols.length === 0){
    // Still emit a table-level row
    lines.push([esc(prof.table),esc(prof.description||''),esc(prof.row_count||0),esc(prof.column_count||0)].concat(Array(headers.length-4).fill('')).join(','));
  }
  cols.forEach(c => {
    const stats = c.stats || {};
    const rules = c.suggested_rules || [];
    const ruleNames = rules.map(r => r.name || r.rule || '').join(' | ');
    const ruleExprs = rules.map(r => r.expression || r.expr || '').join(' | ');
    const ruleSevs  = rules.map(r => r.severity || '').join(' | ');
    const piiTags = (c.pii_tags||[]).map(p => (p.tag||p.name||p)).join(' | ');
    const piiSev  = (c.pii_tags||[]).map(p => p.severity||'').filter(Boolean).join(' | ');
    const row = [
      prof.table, prof.description||'', prof.row_count||0, prof.column_count||0,
      c.name, c.data_type||'', c.category||'', c.nullable===false?'NO':'YES',
      stats.null_pct!=null?stats.null_pct:'', stats.distinct_count!=null?stats.distinct_count:'', stats.distinct_pct!=null?stats.distinct_pct:'',
      stats.min!=null?stats.min:'', stats.max!=null?stats.max:'', stats.avg!=null?stats.avg:'', stats.stddev!=null?stats.stddev:'',
      stats.min_length!=null?stats.min_length:'', stats.max_length!=null?stats.max_length:'', stats.avg_length!=null?stats.avg_length:'',
      stats.top_value!=null?stats.top_value:'', stats.top_value_pct!=null?stats.top_value_pct:'',
      piiTags, piiSev,
      rules.length, ruleNames, ruleExprs, ruleSevs
    ];
    lines.push(row.map(esc).join(','));
  });
  const csv = '\ufeff' + lines.join('\r\n'); // BOM for Excel UTF-8
  const blob = new Blob([csv], {type:'text/csv;charset=utf-8;'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'profile_'+prof.table+'_analysis.csv';
  a.click(); URL.revokeObjectURL(a.href);
  _discToast && _discToast('CSV exported', 'ok');
}
