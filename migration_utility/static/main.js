/* ── Global Error Boundary ── */
(function(){
  var _errCount=0, _MAX_ERRS=5;
  function _showErr(msg){
    if(++_errCount>_MAX_ERRS) return;
    var d=document.createElement('div');
    d.setAttribute('role','alert');
    d.style.cssText='position:fixed;top:'+(8+(_errCount-1)*56)+'px;left:50%;transform:translateX(-50%);z-index:99999;background:#fee2e2;color:#b91c1c;border:2px solid #f87171;padding:12px 18px;border-radius:8px;font:13px/1.4 monospace;max-width:80vw;white-space:pre-wrap;box-shadow:0 8px 32px rgba(0,0,0,.2);cursor:pointer;';
    d.textContent=msg;
    d.onclick=function(){d.remove();_errCount--;};
    document.body.appendChild(d);
    setTimeout(function(){if(d.parentNode){d.remove();_errCount--;}},15000);
  }
  window.onerror=function(msg,src,line){_showErr('JS ERROR (line '+line+'): '+msg);return false;};
  window.addEventListener('unhandledrejection',function(e){
    var reason=e.reason;
    var msg=(reason&&reason.message)||String(reason)||'Unknown promise rejection';
    _showErr('Unhandled Promise: '+msg);
  });
})();
const G = id => document.getElementById(id);
let ALL_OBJECTS=[], HELPER_RESULT=null, ACTIVE_FILE=null, UC_TABLE=null;

function toast(msg,type='tinfo',dur=3200){
  const icons={tok:'✓',terr:'✕',tinfo:'ℹ'};
  const el=document.createElement('div');
  el.className='toast '+type;
  let safeMsg=String(msg==null?'':msg);
  if(safeMsg.length>320) safeMsg=safeMsg.slice(0,320)+'… (truncated)';
  if(type==='terr') dur=Math.max(dur,6000);
  el.innerHTML='<span style="font-weight:700;font-size:13px;flex-shrink:0;">'+(icons[type]||'ℹ')+'</span><span class="toast-msg"></span>';
  el.querySelector('.toast-msg').textContent=safeMsg;
  G('toasts').prepend(el);
  setTimeout(()=>{el.classList.add('hiding');setTimeout(()=>el.remove(),220);},dur);
}
function showToast(msg,type='info'){
  const map={success:'tok',error:'terr',warning:'tinfo',info:'tinfo'};
  toast(msg,map[type]||'tinfo');
}

/* ── Themed confirm dialog (replaces native window.confirm, which renders
   as an unstyled OS dialog with poor contrast) ── */
function uiConfirm(message,opts={}){
  return new Promise(resolve=>{
    const danger=!!opts.danger;
    const title=opts.title||(danger?'Confirm deletion':'Confirm action');
    const okLabel=opts.okLabel||(danger?'Delete':'Confirm');
    const overlay=document.createElement('div');
    overlay.className='ui-confirm-overlay';
    overlay.innerHTML=`
      <div class="ui-confirm-card" role="alertdialog" aria-modal="true" aria-labelledby="uiConfirmTitle">
        <div class="ui-confirm-icon ${danger?'danger':''}">
          <svg viewBox="0 0 24 24"><path d="M12 9v4"/><path d="M12 17h.01"/><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/></svg>
        </div>
        <div class="ui-confirm-title" id="uiConfirmTitle">${title}</div>
        <div class="ui-confirm-msg">${String(message).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/\n/g,'<br>')}</div>
        <div class="ui-confirm-actions">
          <button class="btn btn-ghost btn-xs ui-confirm-cancel">Cancel</button>
          <button class="btn btn-xs ${danger?'ui-confirm-danger-btn':'btn-primary'} ui-confirm-ok">${okLabel}</button>
        </div>
      </div>`;
    document.body.appendChild(overlay);
    const cleanup=result=>{overlay.classList.add('closing');setTimeout(()=>overlay.remove(),150);document.removeEventListener('keydown',onKey);resolve(result);};
    const onKey=e=>{if(e.key==='Escape')cleanup(false);if(e.key==='Enter')cleanup(true);};
    document.addEventListener('keydown',onKey);
    overlay.addEventListener('mousedown',e=>{if(e.target===overlay)cleanup(false);});
    overlay.querySelector('.ui-confirm-cancel').onclick=()=>cleanup(false);
    overlay.querySelector('.ui-confirm-ok').onclick=()=>cleanup(true);
    overlay.querySelector('.ui-confirm-ok').focus();
  });
}
window.uiConfirm=uiConfirm;

const TAB_META={
  convert:{title:'Convert SQL Objects to PySpark',sub:'One .py per SP/View · All UDFs bundled into HelperFunction.py',step:1},
  deploy:{title:'Deploy Notebooks',sub:'Connect to Databricks & upload notebooks to your workspace',step:2},
  uc:{title:'Databricks SQL Editor',sub:'Browse catalogs, run SQL queries & preview table data',step:3},
  healer:{title:'System Health Check',sub:'Intelligent failure detection, auto-recovery, and system health monitoring',step:4},
  'wf-dashboard':{title:'Workflow Manager',sub:'Dashboard — metadata-driven pipeline orchestration overview',step:5},
  'wf-metadata':{title:'MetadataFlow',sub:'Configure Databricks connection & provision Delta metadata tables',step:5},
  'wf-pipelines':{title:'Pipeline Studio',sub:'Connect data sources, create & manage medallion pipelines',step:5},
  'wf-jobs':{title:'Job Manager',sub:'Create workflow jobs, monitor runs & track watermarks',step:5},
  'wf-scheduler':{title:'Job Scheduler',sub:'Schedule migration jobs with cron, interval or one-time triggers',step:5},
  'wf-reports':{title:'Reports & Analytics',sub:'Interactive dashboards, charts & exportable reports for migration pipeline',step:5},
  'wf-progress':{title:'Migration Progress Tracker',sub:'Track overall migration completion — tables, stages, blockers & ETA',step:5},
  'wf-audit':{title:'Audit & Compliance Log',sub:'Track every migration action, config change & security event with full compliance scoring',step:5},
  'wf-dq':{title:'Data Quality Dashboard',sub:'Validate completeness, accuracy, consistency & freshness across all migrated tables',step:5},
  'wf-schema':{title:'Schema Comparison',sub:'Compare source SQL Server & target Databricks schemas — column types, nullability & drift detection',step:5},
  'wf-recon':{title:'Reconciliation Report',sub:'Source vs Bronze aggregate reconciliation — row counts, numeric sums & variance analysis',step:5},
  'wf-datamodel':{title:'Data Modeling',sub:'Auto-generate Star & Snowflake schemas with ER diagrams & Databricks DDL',step:5},
  'wf-settings':{title:'Settings',sub:'Configure Azure infrastructure, storage, connectors & Unity Catalog deployment',step:5},
  'wf-admin':{title:'User Management',sub:'Add, edit & remove users — assign role-based access (Admin only)',step:5},
  'wf-discovery':{title:'Discovery',sub:'Scan & analyse SQL objects — complexity scoring, dependency graph & migration readiness',step:5},
  genie:{title:'Genie AI Assistant',sub:'Ask natural language questions about your migration data using Databricks Genie',step:1},
};
const WF_LBL=['Convert SQL Objects','Deploy Notebooks','Databricks SQL Editor','System Health Check','Workflow Manager'];
const TAB_ICONS={
  convert:'<polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/>',
  deploy:'<polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/><path d="M20.39 18.39A5 5 0 0018 9h-1.26A8 8 0 103 16.3"/>',
  uc:'<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>',
  healer:'<path d="M19.69 14a6.9 6.9 0 00.31-2 6.9 6.9 0 00-.31-2l2.15-1.68a.51.51 0 00.12-.64l-2.04-3.53a.51.51 0 00-.61-.22l-2.54 1.02a6.76 6.76 0 00-3.46-2l-.39-2.7a.5.5 0 00-.49-.42h-4.08a.5.5 0 00-.49.42l-.39 2.7a6.76 6.76 0 00-3.46 2L1.73 3.93a.5.5 0 00-.61.22L.09 7.68a.5.5 0 00.12.64L2.36 10a6.9 6.9 0 000 4L.21 15.68a.51.51 0 00-.12.64l2.04 3.53c.12.22.39.3.61.22l2.54-1.02a6.76 6.76 0 003.46 2l.39 2.7c.04.24.25.42.49.42h4.08c.24 0 .45-.18.49-.42l.39-2.7a6.76 6.76 0 003.46-2l2.54 1.02c.22.08.49 0 .61-.22l2.04-3.53a.51.51 0 00-.12-.64L19.69 14zM12 15.5A3.5 3.5 0 1115.5 12 3.5 3.5 0 0112 15.5z"/>',
  'wf-dashboard':'<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>',
  'wf-metadata':'<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>',
  'wf-pipelines':'<path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>',
  'wf-settings':'<path d="M12.22 2h-.44a2 2 0 00-2 2v.18a2 2 0 01-1 1.73l-.43.25a2 2 0 01-2 0l-.15-.08a2 2 0 00-2.73.73l-.22.38a2 2 0 00.73 2.73l.15.1a2 2 0 011 1.72v.51a2 2 0 01-1 1.74l-.15.09a2 2 0 00-.73 2.73l.22.38a2 2 0 002.73.73l.15-.08a2 2 0 012 0l.43.25a2 2 0 011 1.73V20a2 2 0 002 2h.44a2 2 0 002-2v-.18a2 2 0 011-1.73l.43-.25a2 2 0 012 0l.15.08a2 2 0 002.73-.73l.22-.39a2 2 0 00-.73-2.73l-.15-.08a2 2 0 01-1-1.74v-.5a2 2 0 011-1.74l.15-.09a2 2 0 00.73-2.73l-.22-.38a2 2 0 00-2.73-.73l-.15.08a2 2 0 01-2 0l-.43-.25a2 2 0 01-1-1.73V4a2 2 0 00-2-2z"/><circle cx="12" cy="12" r="3"/>',
  'wf-admin':'<path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/>',
  'wf-jobs':'<rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/>',
  'wf-reports':'<line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>',
  'wf-progress':'<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>',
  'wf-audit':'<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',
  'wf-dq':'<path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/>',
  'wf-schema':'<path d="M16 3h5v5"/><path d="M8 3H3v5"/><path d="M12 22v-8.5"/><path d="M20 9.5V12l-8 4.5L4 12V9.5"/><path d="M4 3l8 4.5L20 3"/>',
  'wf-recon':'<path d="M9 5H2v7l6.29 6.29c.94.94 2.48.94 3.42 0l4.58-4.58c.94-.94.94-2.48 0-3.42L9 5z"/><path d="M6 9h.01"/><path d="M22 5l-4.72 4.72"/>',
  'wf-datamodel':'<circle cx="12" cy="5" r="3"/><line x1="12" y1="8" x2="12" y2="14"/><circle cx="6" cy="19" r="3"/><circle cx="18" cy="19" r="3"/><line x1="12" y1="14" x2="6" y2="16"/><line x1="12" y1="14" x2="18" y2="16"/>',
  'wf-discovery':'<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/>',
  genie:'<path d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"/>',
};

function switchTab(id,btn){
  document.querySelectorAll('.tab-pane').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
  G('pane-'+id).classList.add('active');
  G('nav-'+id).classList.add('active');

  // Persist current tab in URL hash for page refresh
  if(id!=='aiworkflow') history.replaceState(null,'','#'+id);

  if(id==='healer'){
    _hlSyncFromConfig();
    hlFetchRecentRuns();
  }
  if(id==='convert') _populateAiModelSelect();
  if(id==='uc' && typeof ucInit==='function') ucInit();
  if(id==='aiworkflow'){
    switchTab('wf-dashboard',G('nav-wf-dashboard'));return;
  }
  const _wfIds=['wf-dashboard','wf-metadata','wf-pipelines','wf-jobs','wf-settings','wf-progress','wf-audit','wf-dq','wf-schema','wf-recon','wf-datamodel','wf-admin','wf-discovery'];
  if(_wfIds.includes(id)){
    // Auto-sync hidden wfDbr* fields from Settings / deployconfig
    _wfSyncHiddenFields();
    if(id==='wf-dashboard'||id==='wf-pipelines') wfRefreshAll();
    if(id==='wf-jobs') wfRefreshJobs(),wfRefreshAuditHistory();
    if(id==='wf-settings'){loadDeployConfig();loadSecretVault();}
    if(id==='wf-admin' && typeof adminRefresh==='function') adminRefresh();
    if(id==='wf-progress' && typeof mptRefresh==='function') mptRefresh();
    if(id==='wf-audit' && typeof auditRefresh==='function') auditRefresh();
    if(id==='wf-dq' && typeof dqRefresh==='function') dqRefresh();
    if(id==='wf-schema' && typeof scRefresh==='function') scRefresh();
    if(id==='wf-recon' && typeof reconRefresh==='function') reconRefresh();
    if(id==='wf-datamodel' && typeof dmInit==='function') dmInit();
    if(id==='wf-discovery' && typeof discInit==='function') discInit();
  }
  const m=TAB_META[id];
  if(m){
    G('topIco').innerHTML=TAB_ICONS[id]||TAB_ICONS.convert;
    G('topTitle').textContent=m.title;
    G('topSub').textContent=m.sub;
    if(G('topBcCur')) G('topBcCur').textContent=m.title;
    for(let i=1;i<=5;i++){const s=G('wf'+i);if(s) s.className='wf-step'+(i<m.step?' done':i===m.step?' active':'');}
    G('wfLbl').textContent='Step '+m.step+' of 5 \u2014 '+WF_LBL[m.step-1];
  }
}

/* Populate the AI model dropdown from the workspace's actual READY serving
   endpoints (same source as Genie's model picker) instead of a static list,
   so every option the user can pick is actually invokable — avoids 403s from
   selecting a model that isn't enabled/entitled in this workspace. Falls
   back silently to the static HTML options (already present) on failure. */
let _aiModelsLoaded=false;
async function _populateAiModelSelect(){
  if(_aiModelsLoaded) return;
  const sel=G('aiModelSelect');
  if(!sel) return;
  try{
    const r=await fetch('/api/v1/genie/fm/endpoints');
    const d=await r.json();
    const eps=(d.endpoints||[]).filter(e=>e.state==='Ready');
    if(eps.length){
      const prev=sel.value;
      sel.innerHTML=eps.map(e=>`<option value="${e.name}">${e.display_name}</option>`).join('');
      if(eps.some(e=>e.name===prev)) sel.value=prev;
      _aiModelsLoaded=true;
    }
  }catch(e){/* keep the static fallback options already in the HTML */}
}
(function(){ if(document.getElementById('pane-convert')) _populateAiModelSelect(); })();

async function loadObjects(){
  try{
    const r=await fetch('/api/v1/all-objects');
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Load failed');
    const MAP={stored_procedure:'SP',view:'VIEW',udf:'UDF'};
    ALL_OBJECTS=[];
    const C={SP:0,VIEW:0,UDF:0};
    for(const[ot,items]of Object.entries(d.grouped||{})){
      items.forEach(o=>{
        const t=MAP[ot]||ot.toUpperCase();
        ALL_OBJECTS.push({key:o.key,name:o.name,description:o.description,object_type:ot,type:t});
        if(C[t]!==undefined)C[t]++;
      });
    }
    G('statSP').textContent=C.SP;G('statVW').textContent=C.VIEW;G('statUDF').textContent=C.UDF;
    renderObjects();
  }catch(e){G('objList').innerHTML='<div class="alert a-err"><span class="a-ico">✕</span>Failed: '+e.message+'</div>';}
}

function renderObjects(){
  const GRPS={SP:[],VIEW:[],UDF:[]};
  ALL_OBJECTS.forEach(o=>{if(GRPS[o.type])GRPS[o.type].push(o);});
  const M={
    SP:{label:'Stored Procedures',dot:'var(--sp-c)',lc:'var(--sp-c)',tc:'sp',desc:'Stored procedure'},
    VIEW:{label:'SQL Views',dot:'var(--vw-c)',lc:'var(--vw-c)',tc:'vw',desc:'SQL View'},
    UDF:{label:'User-Defined Functions',dot:'var(--udf-c)',lc:'var(--udf-c)',tc:'udf',desc:'User-defined function'},
  };
  let html='';
  for(const[t,items]of Object.entries(GRPS)){
    if(!items.length)continue;
    const m=M[t];
    html+=`<div class="grp-hd" onclick="toggleGrp('${t}')">
      <span class="grp-dot" style="background:${m.dot}"></span>
      <span class="grp-lbl" style="color:${m.lc}">${m.label}</span>
      <span class="grp-badge">${items.length}</span>
      <span class="grp-chv" id="gc${t}">▾</span>
    </div><div id="gi${t}" style="padding:4px 0;">`;
    items.forEach(o=>{
      html+=`<div class="obj-item" id="oi${o.key}" onclick="togItem('${o.key}',event)">
        <input type="checkbox" id="ck${o.key}">
        <span class="chk" id="cb${o.key}">✓</span>
        <span class="badge b${m.tc}">${t==='VIEW'?'VIEW':t}</span>
        <div class="obj-info"><div class="obj-name" title="${o.key}">${o.key}</div><div class="obj-desc">${o.description||m.desc}</div></div>
        <button class="sql-btn" onclick="event.stopPropagation();loadSrc('${o.key}')">SQL</button>
      </div>`;
    });
    html+='</div>';
  }
  G('objList').innerHTML=html||'<div class="empty"><div class="empty-s">No objects loaded.</div></div>';
  updSelCnt();
}

function toggleGrp(t){const el=G('gi'+t),chv=G('gc'+t);if(!el)return;const hide=el.style.display!=='none';el.style.display=hide?'none':'';if(chv)chv.textContent=hide?'▸':'▾';}
function togItem(key,e){if(e.target.classList.contains('sql-btn'))return;const c=G('ck'+key);if(c){c.checked=!c.checked;updSel();}}
function updSel(){ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key),item=G('oi'+o.key),cb=G('cb'+o.key);const s=c&&c.checked;if(item)item.classList.toggle('selected',s);if(cb)cb.style.color=s?'#fff':'transparent';});updSelCnt();}
function updSelCnt(){const n=getSel().length;G('selCnt').textContent=n+' / '+ALL_OBJECTS.length;const b=G('btnConvert');if(b)b.disabled=(n===0);updAnalysisSel();}
function getSel(){return ALL_OBJECTS.filter(o=>{const c=G('ck'+o.key);return c&&c.checked;});}

/* ── Source Analysis ── */
function renderAnalysis(objects){
  const card=G('analysisCard');
  if(!objects||!objects.length){card.style.display='none';return;}
  const byType={SP:[],VIEW:[],UDF:[]};
  objects.forEach(o=>{const t=o.type;if(byType[t])byType[t].push(o);else byType[t]=[o];});
  const total=objects.length;
  const cnts={SP:byType.SP.length,VIEW:byType.VIEW.length,UDF:byType.UDF.length};
  // Distribution bar
  const BGMAP={SP:'#FB923C',VIEW:'#38BDF8',UDF:'#A78BFA'};
  G('anDistBar').innerHTML=['SP','VIEW','UDF'].map(t=>{
    const w=total?Math.round(cnts[t]/total*100):0;
    return w?`<div class="an-bar-seg" style="width:${w}%;background:${BGMAP[t]}" title="${cnts[t]} ${t}"></div>`:'';
  }).join('');
  // Type counts
  ['SP','VIEW','UDF'].forEach(t=>{const el=G('anCnt'+t);if(el)el.textContent=cnts[t];});
  // Show first non-empty tab
  window._AN_DATA=byType;
  const first=['SP','VIEW','UDF'].find(t=>cnts[t]>0)||'SP';
  showAnTab(first);
  card.style.display='';
  updAnalysisSel();
}
function showAnTab(type){
  const d=window._AN_DATA;
  if(!d)return;
  ['SP','VIEW','UDF'].forEach(t=>{const btn=G('anTab'+t);if(btn)btn.classList.toggle('active',t===type);});
  const items=d[type]||[];
  const DOTMAP={SP:'#C2410C',VIEW:'#0369A1',UDF:'#6D28D9'};
  if(!items.length){G('anList').innerHTML='<div class="an-empty">No '+type+' objects loaded</div>';return;}
  G('anList').innerHTML=items.map(o=>{
    const len=(o.code||'').length;
    const cx=len===0?null:len<500?'LOW':len<1500?'MED':'HIGH';
    const cxS=cx==='LOW'?'background:#DCFCE7;color:#15803D':cx==='MED'?'background:#FEF9C3;color:#92400E':cx==='HIGH'?'background:#FEE2E2;color:#B91C1C':'';
    return `<div class="an-item" onclick="loadSrc('${o.key}')" title="Click to view SQL">`+
      `<span class="an-item-dot" style="background:${DOTMAP[type]}"></span>`+
      `<span class="an-item-name">${o.name}</span>`+
      (cx?`<span class="an-cx" style="${cxS}">${cx}</span>`:'')+
      `</div>`;
  }).join('');
}
function updAnalysisSel(){
  const sel=typeof getSel==='function'?getSel():[];
  const total=ALL_OBJECTS?ALL_OBJECTS.length:0;
  const el=G('anSelSummary');
  if(el)el.textContent=sel.length+' selected · '+total+' total';
  ['SP','VIEW','UDF'].forEach(t=>{
    const n=ALL_OBJECTS?ALL_OBJECTS.filter(o=>o.type===t&&(()=>{const c=G('ck'+o.key);return c&&c.checked;})()).length:0;
    const badge=G('anSel'+t);
    if(badge){badge.textContent=n?(n+''):'';badge.style.opacity=n?'1':'0';}
  });
}

/* ── Source Connection ── */
const IDD_OPTS = {
  sqlserver: {label:'SQL Server'},
  azuresql: {label:'Azure SQL'},
  synapse:  {label:'Azure Synapse'},
  sqlmi:    {label:'SQL Managed Instance'},
  snowflake:{label:'Snowflake'},
  redshift: {label:'Redshift'},
  sharepoint:{label:'SharePoint'},
  api:      {label:'REST API'}
};
// toggleIDD / pickSrcType no longer needed — native <select> handles it
function toggleIDD(){}
function pickSrcType(){}

/* ── Source-type aware helpers ── */
const _NON_SQL_SRC = v => v==='sharepoint'||v==='api';
function _setLbl(id, txt){
  const el=G(id); if(!el) return;
  el.innerHTML = txt;
}
/* Apply per-type labels to a pane's server/user/pass labels.
   prefix: 'src' | 'cfgSrc' */
function _applySrcLabels(prefix, v){
  let srv='Server / Host', user='Username', pass='Password';
  if(v==='sharepoint'){ srv='SharePoint Site URL'; user='Client ID'; pass='Client Secret'; }
  else if(v==='api'){ srv='API Base URL'; }
  _setLbl(prefix+'ServerLabel', srv+' <span class="cfg-req">*</span>');
  _setLbl(prefix+'UserLabel', user);
  _setLbl(prefix+'PassLabel', pass);
}

/* ── Snowflake-aware field toggling ── */
function onSrcTypeChange(selectEl){
  const v=(selectEl||G('srcType')).value;
  const isSf=(v==='snowflake');
  const nonSql=_NON_SQL_SRC(v);
  // SQL Server fields (SharePoint/API reuse the Server field as Site/Base URL)
  ['srcDbRow'].forEach(id=>{const el=G(id);if(el)el.style.display=(isSf||nonSql)?'none':'';});
  // Snowflake fields
  ['srcAccountRow','srcWarehouseRow','srcRoleRow','srcSnowDbRow'].forEach(id=>{const el=G(id);if(el)el.style.display=isSf?'':'none';});
  _applySrcLabels('src', v);
  // Labels
  const userLabel=G('srcUserLabel');
  if(userLabel) userLabel.textContent=isSf?'Username':'Username';
}
function onWfSrcTypeChange(selectEl){
  const v=(selectEl||G('wfSrcType')).value;
  const isSf=(v==='snowflake');
  ['wfSrcServerRow','wfSrcDbRow'].forEach(id=>{const el=G(id);if(el)el.style.display=isSf?'none':'';});
  ['wfSrcAccountRow','wfSrcWarehouseRow','wfSrcRoleRow','wfSrcSnowDbRow'].forEach(id=>{const el=G(id);if(el)el.style.display=isSf?'':'none';});
}

/* Auto-populate source connection hidden fields from deployconfig.json */
async function _srcSyncFromConfig(){
  const cfg=await _ensureDeployConfig();
  const src=cfg.source||{};
  G('srcType').value=src.source_type||'azuresql';
  G('srcServer').value=src.server||'';
  G('srcDb').value=src.database||'';
  G('srcUser').value=src.username||'';
  G('srcPass').value=src.password||'';
  // Snowflake fields
  if(G('srcAccount'))G('srcAccount').value=src.account||'';
  if(G('srcWarehouse'))G('srcWarehouse').value=src.warehouse||'';
  if(G('srcRole'))G('srcRole').value=src.role||'';
  if(G('srcSnowDb'))G('srcSnowDb').value=src.database||'';
  // SharePoint / REST API fields
  if(G('srcTenantId'))G('srcTenantId').value=src.tenant_id||'';
  if(G('srcApiAuthType'))G('srcApiAuthType').value=src.api_auth_type||'none';
  if(G('srcApiKeyHeader'))G('srcApiKeyHeader').value=src.api_key_header||'';
  // Update info bar
  if(G('srcCfgServer'))G('srcCfgServer').textContent=src.source_type==='snowflake'?(src.account||'—'):(src.server||'—');
  if(G('srcCfgDb'))G('srcCfgDb').textContent=src.database||'—';
  if(G('srcCfgType'))G('srcCfgType').textContent=(IDD_OPTS[src.source_type]||{}).label||src.source_type||'—';
  if(G('srcCfgUser'))G('srcCfgUser').textContent=src.username||'—';
  // Toggle field visibility
  onSrcTypeChange(G('srcType'));
}

async function _dbrSyncFromConfig(){
  const cfg=await _ensureDeployConfig();
  G('dbHost').value=cfg.databricks_host||'';
  G('dbToken').value=cfg.databricks_token||'';
  if(G('dbrCfgHost'))G('dbrCfgHost').textContent=cfg.databricks_host||'—';
  if(G('dbrCfgToken'))G('dbrCfgToken').textContent=cfg.databricks_token?'Configured ✓':'—';
}

function toggleSrcConn(){
  const body=G('srcConnBody'),chv=G('srcConnChv');
  body.classList.toggle('collapsed');
  chv.classList.toggle('open');
}

async function testSourceConn(){
  const btn=G('btnTestSrc');
  const server=G('srcServer').value.trim(),db=G('srcDb').value.trim(),user=G('srcUser').value.trim();
  const srcTypeSel=G('srcType').value;
  const isSf=srcTypeSel==='snowflake';
  const nonSql=_NON_SQL_SRC(srcTypeSel);
  if(isSf){
    const account=(G('srcAccount')||{}).value?.trim()||'';
    if(!account||!user){toast('Account and username are required.','terr');return;}
  }else if(nonSql){
    if(!server){toast((srcTypeSel==='sharepoint'?'SharePoint Site URL':'API Base URL')+' is required.','terr');return;}
    if(srcTypeSel==='sharepoint'&&(!(G('srcTenantId')||{}).value?.trim()||!user)){toast('Tenant ID and Client ID are required for SharePoint.','terr');return;}
  }else if(!server||!db||!user){toast('Server, database and username are required.','terr');return;}
  const origHTML=btn.innerHTML;
  btn.disabled=true;btn.innerHTML='<div class="spin"></div>';
  const msg=G('srcConnMsg');
  msg.style.display='none';
  try{
    const srcType=srcTypeSel;
    const payload={source_type:srcType,server,database:nonSql?'':db,username:user,password:G('srcPass').value};
    if(srcType==='snowflake'){
      payload.account=(G('srcAccount')||{}).value||'';
      payload.warehouse=(G('srcWarehouse')||{}).value||'';
      payload.role=(G('srcRole')||{}).value||'';
      payload.database=(G('srcSnowDb')||{}).value||db;
    }
    if(srcType==='sharepoint'){
      payload.tenant_id=(G('srcTenantId')||{}).value||'';
    }
    if(srcType==='api'){
      payload.api_auth_type=(G('srcApiAuthType')||{}).value||'none';
      payload.api_key_header=(G('srcApiKeyHeader')||{}).value||'';
    }
    const r=await fetch('/api/v1/source/test-connection',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    const d=await r.json();
    msg.style.display='';
    if(d.success){
      msg.style.cssText='display:block;background:var(--green-light);color:var(--green-fg);border:1px solid var(--green-border);font-size:11.5px;padding:8px 10px;border-radius:var(--r);';
      msg.textContent='✓ '+d.server_version;
      G('srcConnDot').className='src-conn-dot ok';
      G('btnLoadSrc').disabled=false;
      toast('Source connected successfully!','tok');
    }else{
      msg.style.cssText='display:block;background:var(--red-light);color:var(--red-fg);border:1px solid var(--red-border);font-size:11.5px;padding:8px 10px;border-radius:var(--r);';
      msg.textContent='✕ '+d.error;
      G('srcConnDot').className='src-conn-dot err';
      G('btnLoadSrc').disabled=true;
      toast(d.error,'terr',5000);
    }
  }catch(e){
    msg.style.cssText='display:block;background:var(--red-light);color:var(--red-fg);border:1px solid var(--red-border);font-size:11.5px;padding:8px 10px;border-radius:var(--r);';
    msg.textContent='✕ '+e.message;
    G('srcConnDot').className='src-conn-dot err';
    toast(e.message,'terr');
  }finally{btn.disabled=false;btn.innerHTML=origHTML;}
}

async function loadFromSource(){
  const btn=G('btnLoadSrc');
  const server=G('srcServer').value.trim(),db=G('srcDb').value.trim(),user=G('srcUser').value.trim();
  const srcType=G('srcType').value;
  const origHTML=btn.innerHTML;
  btn.disabled=true;btn.innerHTML='<div class="spin"></div> Loading…';
  G('objList').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Fetching objects from source…</span></div>';
  try{
    const payload={source_type:srcType,server,database:_NON_SQL_SRC(srcType)?'':db,username:user,password:G('srcPass').value};
    if(srcType==='snowflake'){
      payload.account=(G('srcAccount')||{}).value||'';
      payload.warehouse=(G('srcWarehouse')||{}).value||'';
      payload.role=(G('srcRole')||{}).value||'';
      payload.database=(G('srcSnowDb')||{}).value||db;
    }
    if(srcType==='sharepoint'){
      payload.tenant_id=(G('srcTenantId')||{}).value||'';
    }
    if(srcType==='api'){
      payload.api_auth_type=(G('srcApiAuthType')||{}).value||'none';
      payload.api_key_header=(G('srcApiKeyHeader')||{}).value||'';
    }
    const r=await fetch('/api/v1/source/load-objects',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Load failed');
    const MAP={stored_procedure:'SP',view:'VIEW',udf:'UDF'};
    ALL_OBJECTS=[];
    const C={SP:0,VIEW:0,UDF:0};
    for(const[ot,items]of Object.entries(d.grouped||{})){
      items.forEach(o=>{
        const t=MAP[ot]||ot.toUpperCase();
        ALL_OBJECTS.push({key:o.key,name:o.name,description:o.description||'',object_type:ot,type:t,code:o.code||''});
        if(C[t]!==undefined)C[t]++;
      });
    }
    G('statSP').textContent=C.SP;
    G('statVW').textContent=C.VIEW;
    G('statUDF').textContent=C.UDF;
    renderObjects();
    renderAnalysis(ALL_OBJECTS);
    toast('Loaded '+ALL_OBJECTS.length+' objects from '+db+'.','tok',4000);
    const body=G('srcConnBody'),chv=G('srcConnChv');
    if(!body.classList.contains('collapsed')){body.classList.add('collapsed');chv.classList.remove('open');}
  }catch(e){
    G('objList').innerHTML='<div class="alert a-err"><span class="a-ico">✕</span>'+e.message+'</div>';
    toast(e.message,'terr');
  }finally{btn.disabled=false;btn.innerHTML=origHTML;}
}

function setFilterActive(id){['fbAll','fbNone','fbSP','fbVW','fbUDF'].forEach(i=>{const b=G(i);if(b)b.classList.remove('active');});if(id){const b=G(id);if(b)b.classList.add('active');}}
function selectAll(){
  const allChecked=ALL_OBJECTS.length>0&&ALL_OBJECTS.every(o=>{const c=G('ck'+o.key);return c&&c.checked;});
  ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key);if(c)c.checked=!allChecked;});
  updSel();setFilterActive(allChecked?'fbNone':'fbAll');
}
function deselectAll(){ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key);if(c)c.checked=false;});updSel();setFilterActive('fbNone');}
function selType(t){
  const MAP={SP:'fbSP',VIEW:'fbVW',UDF:'fbUDF'};
  const allOfType=ALL_OBJECTS.filter(o=>o.type===t);
  const alreadyActive=allOfType.length>0&&allOfType.every(o=>{const c=G('ck'+o.key);return c&&c.checked;})&&ALL_OBJECTS.filter(o=>o.type!==t).every(o=>{const c=G('ck'+o.key);return c&&!c.checked;});
  if(alreadyActive){ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key);if(c)c.checked=false;});updSel();setFilterActive('fbNone');}
  else{ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key);if(c)c.checked=(o.type===t);});updSel();setFilterActive(MAP[t]||null);}
}

async function loadSrc(key){
  const local=ALL_OBJECTS.find(o=>o.key===key);
  if(local&&local.code&&local.code.trim()){
    G('srcName').textContent=key+'  ['+(local.object_type||local.type||'').toUpperCase()+']';
    G('srcBody').textContent=local.code.trim();
    G('srcPanel').style.display='';
    return;
  }
  try{
    const r=await fetch('/api/v1/object-code/'+encodeURIComponent(key));
    const d=await r.json();
    if(d.success){G('srcName').textContent=key+'  ['+(d.object_type||'').toUpperCase()+']';G('srcBody').textContent=d.code;G('srcPanel').style.display='';}
    else toast('Source not found: '+(d.error||'?'),'terr');
  }catch(e){toast(e.message,'terr');}
}
async function previewSource(){const sel=getSel();if(!sel.length){toast('Select at least one object first.','tinfo');return;}loadSrc(sel[0].key);}
function closeSrc(){G('srcPanel').style.display='none';}

async function convertSelected(){
  const sel=getSel();
  if(!sel.length){toast('Select at least one object.','tinfo');return;}
  const btn=G('btnConvert');btn.disabled=true;
  G('bci').style.display='none';
  const spinEl=document.createElement('span');spinEl.className='spin';spinEl.style.cssText='border-top-color:#fff;margin-right:0';
  btn.insertBefore(spinEl,btn.querySelector('#bct'));
  G('bct').textContent='Converting…';
  let pct=0;const prog=G('bprog');
  const iv=setInterval(()=>{pct=Math.min(pct+6,82);prog.style.width=pct+'%';},120);
  const cnt=sel.length;
  G('codeOut').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Building notebooks for '+cnt+' object'+(cnt>1?'s':'')+'…</span></div>';
  G('nbTabs').innerHTML='';G('nbBar').style.display='none';
  G('notesCard').style.display='none';G('pyBadge').style.display='none';
  HELPER_RESULT=null;
  try{
    const r=await fetch('/api/v1/convert-separate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
      object_names: sel.map(o=>o.key),
      objects_with_code: Object.fromEntries(sel.filter(o=>o.code).map(o=>[o.key,{type:o.object_type||o.type,code:o.code}]))
    })});
    const d=await r.json();
    clearInterval(iv);prog.style.width='100%';
    setTimeout(()=>prog.style.width='0%',450);
    if(!d.success){G('codeOut').innerHTML='<div class="alert a-err" style="margin:14px;"><span class="a-ico">✕</span>'+(d.error||'Conversion failed')+'</div>';toast(d.error||'Conversion failed','terr');return;}
    HELPER_RESULT=d;renderSeparateFiles(d);updDeployList();
    const msg=(d.sp_view_count||0)+' file'+((d.sp_view_count||0)!==1?'s':'')+(d.udf_count?' + HelperFunction.py ('+d.udf_count+' UDF'+(d.udf_count!==1?'s':'')+')'  :'')+' — '+d.object_count+' objects converted';
    toast(msg,'tok',4500);G('wf1').className='wf-step done';
  }catch(e){
    clearInterval(iv);prog.style.width='0%';
    G('codeOut').innerHTML='<div class="alert a-err" style="margin:14px;"><span class="a-ico">✕</span>'+e.message+'</div>';
    toast('Error: '+e.message,'terr');
  }finally{
    btn.disabled=false;
    if(spinEl.parentNode)spinEl.remove();
    G('bci').style.display='';
    G('bct').textContent='Convert - SQL → PySpark';
    updSelCnt();
  }
}

function renderSeparateFiles(d){
  const udfLbl=d.udf_count?d.udf_count+' UDF'+(d.udf_count!==1?'s':''):'Shared';
  let th=`<button class="nb-tab helper active" id="nbt___helper" onclick="showFile('__helper__')">⚙ HelperFunction.py <span style="opacity:.5;font-size:9px">${udfLbl}</span></button>`;
  (d.files||[]).forEach(f=>{
    const cls=f.object_type==='stored_procedure'?'sp':f.object_type==='view'?'vw':'ud';
    const ico=f.object_type==='stored_procedure'?'▸':f.object_type==='view'?'◉':'ƒ';
    th+=`<button class="nb-tab ${cls}" id="nbt_${f.name}" onclick="showFile('${f.name}')">${ico} ${f.name}.py</button>`;
  });
  G('nbTabs').innerHTML=th;G('nbBar').style.display='';
  ACTIVE_FILE='__helper__';G('codeTitle').textContent='HelperFunction.py';G('pyBadge').style.display='';
  G('codeOut').textContent=d.helper_code;
  const btnDL=G('btnDL'),btnALL=G('btnDLAll');
  if(btnDL)btnDL.disabled=false;if(btnALL)btnALL.disabled=false;G('btnCopy').disabled=false;
  const notes=d.conversion_notes||{};let nh='';
  Object.entries(notes).forEach(([name,ns])=>{
    const o=ALL_OBJECTS.find(x=>x.key===name)||{type:'SP'};const tc=o.type.toLowerCase();
    nh+=`<div class="note-grp"><span class="badge b${tc}">${o.type}</span> ${name}</div><div>`;
    (ns||[]).forEach(n=>{nh+=`<div class="note-item">${n}</div>`;});nh+='</div>';
  });
  if(nh){G('notesList').innerHTML=nh;G('notesCard').style.display='';}
}

function showFile(name){
  if(!HELPER_RESULT)return;
  document.querySelectorAll('.nb-tab').forEach(t=>t.classList.remove('active'));
  ACTIVE_FILE=name;
  if(name==='__helper__'){const t=G('nbt___helper');if(t)t.classList.add('active');G('codeTitle').textContent='HelperFunction.py';G('codeOut').textContent=HELPER_RESULT.helper_code;}
  else{const t=G('nbt_'+name);if(t)t.classList.add('active');const f=(HELPER_RESULT.files||[]).find(x=>x.name===name);G('codeTitle').textContent=name+'.py';G('codeOut').textContent=f?f.code:'# File "'+name+'" not found.';}
  G('btnCopy').disabled=false;const btnDL=G('btnDL');if(btnDL)btnDL.disabled=false;
}

function copyCode(){const c=G('codeOut').textContent;if(!c||c.trim().length<5){toast('Nothing to copy.','tinfo');return;}navigator.clipboard.writeText(c).then(()=>{toast('Copied to clipboard!','tok',2000);const b=G('btnCopy'),orig=b.innerHTML;b.textContent='✓ Copied!';setTimeout(()=>b.innerHTML=orig,1800);});}
function dlCode(){const c=G('codeOut').textContent;if(!c||c.trim().length<5){toast('Nothing to download.','tinfo');return;}const fn=(!ACTIVE_FILE||ACTIVE_FILE==='__helper__')?'HelperFunction.py':(ACTIVE_FILE+'.py');Object.assign(document.createElement('a'),{href:URL.createObjectURL(new Blob([c],{type:'text/plain'})),download:fn}).click();toast('Downloaded '+fn,'tok',2000);}
function dlAllFiles(){
  if(!HELPER_RESULT){toast('Convert first.','tinfo');return;}
  const allFiles=[{filename:'HelperFunction.py',code:HELPER_RESULT.helper_code},...(HELPER_RESULT.files||[]).map(f=>({filename:f.filename,code:f.code}))];
  if(typeof JSZip!=='undefined'){
    var zip=new JSZip();
    allFiles.forEach(function(f){zip.file(f.filename,f.code);});
    zip.generateAsync({type:'blob'}).then(function(blob){
      Object.assign(document.createElement('a'),{href:URL.createObjectURL(blob),download:'pyspark_converted_'+new Date().toISOString().slice(0,10)+'.zip'}).click();
      toast('Downloaded ZIP with '+allFiles.length+' files','tok',2500);
    }).catch(function(err){toast('ZIP failed: '+err.message,'terr',3000);});
  }else{
    allFiles.forEach((f,i)=>{setTimeout(()=>{Object.assign(document.createElement('a'),{href:URL.createObjectURL(new Blob([f.code],{type:'text/plain'})),download:f.filename}).click();},i*350);});
    toast('Downloading '+allFiles.length+' files…','tok',2500);
  }
}

function updDeployList(){
  if(!HELPER_RESULT){G('deployList').innerHTML='<div class="empty" style="padding:12px;"><div class="empty-ico">📓</div><div class="empty-s">Convert objects in Step 1 first.</div></div>';if(G('depCnt'))G('depCnt').textContent='0 files';return;}
  const files=HELPER_RESULT.files||[];
  const total=files.length+(HELPER_RESULT.udf_count>0?1:0);
  if(G('depCnt'))G('depCnt').textContent=total+' file'+(total!==1?'s':'');
  let h='<div style="display:flex;flex-direction:column;gap:6px;">';
  if(HELPER_RESULT.udf_count>0){
    h+=`<div style="display:flex;align-items:center;gap:9px;padding:8px 6px;border-bottom:1px solid var(--border);margin-bottom:2px;">
    <div style="flex:1;"><div style="font-size:12.5px;font-weight:700;color:var(--blue-fg);">⚙ HelperFunction.py</div><div style="font-size:10.5px;color:var(--t3);">${HELPER_RESULT.udf_count||0} UDFs · ${HELPER_RESULT.helper_lines||0} lines</div></div>
  </div>`;
  }
  files.forEach(f=>{const cls=f.object_type==='stored_procedure'?'bsp':f.object_type==='view'?'bvw':'budf';const lbl=f.object_type==='stored_procedure'?'SP':f.object_type==='view'?'VIEW':'UDF';h+=`<div style="display:flex;align-items:center;gap:8px;padding:6px 4px;"><span class="badge ${cls}">${lbl}</span><span style="font-size:12.5px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${f.filename}</span><span style="font-size:10.5px;color:var(--t3);flex-shrink:0;">${f.lines} lines</span></div>`;});
  G('deployList').innerHTML=h+'</div>';
}

async function deployAll(){
  const host=G('dbHost').value.trim(),token=G('dbToken').value.trim(),path=G('depPath').value.trim()||'/Shared/Migrations';
  if(!host||!token){toast('Enter Workspace Host and Access Token.','terr');return;}
  if(!HELPER_RESULT){toast('Convert objects in Step 1 first.','tinfo');return;}
  const btn=G('btnDeployAll'),lbl=G('depBtnTxt'),prog=G('depProg');
  btn.disabled=true;lbl.textContent='Deploying…';
  let pct=0;const iv=setInterval(()=>{pct=Math.min(pct+4,88);prog.style.width=pct+'%';},200);
  G('deployLog').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Uploading notebooks to Databricks…</span></div>';
  const notebooks=[{name:'HelperFunction',code:HELPER_RESULT.helper_code},...(HELPER_RESULT.files||[]).map(f=>({name:f.name,code:f.code}))];
  try{
    const r=await fetch('/api/v1/databricks/upload-multiple',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({host,token,workspace_path:path,notebooks})});
    const d=await r.json();
    clearInterval(iv);prog.style.width='100%';setTimeout(()=>prog.style.width='0%',500);
    if(d.results&&d.results.length){
      let logHtml='';
      d.results.forEach(res=>{const ok=res.success;logHtml+=`<div class="dep-row ${ok?'ok':'err'}"><div class="blt ${ok?'blt-ok':'blt-err'}"></div><span class="dep-name">${res.name}.py</span><span class="dep-path">${res.path||res.error||''}</span></div>`;});
      if(d.uploaded===notebooks.length){logHtml+=`<div class="alert a-ok" style="margin-top:10px;"><span class="a-ico">✓</span> All ${d.uploaded} notebooks deployed to <strong>${path}</strong></div>`;G('wf2').className='wf-step done';lbl.textContent='✓ Deployed!';toast('All '+d.uploaded+' notebooks deployed!','tok');}
      else{logHtml+=`<div class="alert a-warn" style="margin-top:10px;"><span class="a-ico">⚠</span> ${d.uploaded} of ${d.total} notebooks uploaded.</div>`;lbl.textContent='Deploy All to Databricks';toast(d.uploaded+'/'+d.total+' deployed.','tinfo');}
      G('deployLog').innerHTML=logHtml;
    }else{G('deployLog').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error||'Upload failed'}</div>`;lbl.textContent='Deploy All to Databricks';toast('Deploy failed.','terr');}
  }catch(e){
    clearInterval(iv);prog.style.width='0%';
    G('deployLog').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;lbl.textContent='Deploy All to Databricks';toast('Deploy error: '+e.message,'terr');
  }finally{btn.disabled=false;}
}

// ── Push Notebooks to Azure DevOps (reusable for migrations & metadata) ──────
async function nbPushToDevOps(mode){
  // mode: 'migrations' (SP converted) or 'metadata' (MetadataPipeline)
  const org=(G('cfgDevOpsOrg')||{}).value||'';
  const project=(G('cfgDevOpsProject')||{}).value||'';
  const repo=(G('cfgDevOpsRepo')||{}).value||'';
  const branch=(G('cfgDevOpsBranch')||{}).value||'main';
  const reviewers=(G('cfgDevOpsReviewers')||{}).value||'';
  if(!org||!project||!repo){toast('Configure Azure DevOps in Settings first (org/project/repo)','terr');return;}

  let notebooks=[];
  let folderPath='Notebooks/Migrations';
  let defaultMsg='Deploy SP notebooks to DevOps';

  if(mode==='migrations'){
    if(!HELPER_RESULT){toast('Convert objects in Step 1 first.','terr');return;}
    notebooks=[{name:'HelperFunction',code:HELPER_RESULT.helper_code},...(HELPER_RESULT.files||[]).map(f=>({name:f.name,code:f.code}))];
    folderPath='Notebooks/Migrations';
    defaultMsg='Deploy SP migration notebooks';
  } else if(mode==='metadata'){
    // Generate metadata notebooks first
    toast('Generating metadata notebooks…','tinfo');
    try{
      const pipelineMode=(G('wfNbPipelineMode')||{}).value||'standard';
      const c=await _wfDbrCredsWithFallback();
      const gr=await fetch('/api/v1/workflow/notebooks/generate',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({
          catalog:c.catalog||'main',schema:c.schema||'default',
          workspace_path:G('wfNbWsPath').value.trim()||'/Shared/MetadataPipeline',
          landing_path:G('wfNbLandingPath').value.trim()||'/mnt/landing',
          pipeline_mode:pipelineMode,
          cdc_mode:(G('cfgCdcMode')||{}).value||'watermark',
          primary_keys:(G('cfgPrimaryKeys')||{}).value ? G('cfgPrimaryKeys').value.split(',').map(s=>s.trim()).filter(Boolean) : [],
          recon_catalog:G('cfgReconCatalog')?.value?.trim()||'reconciliation',
          recon_schema:G('cfgReconSchema')?.value?.trim()||'hr',
          recon_table:G('cfgReconTable')?.value?.trim()||'ReconcilationDetails',
          log_catalog:G('cfgLogCatalog')?.value?.trim()||'logging',
          log_schema:G('cfgLogSchema')?.value?.trim()||'hr',
          log_table:G('cfgLogTable')?.value?.trim()||'ExecutionLog',
        })
      });
      const gd=await gr.json();
      if(!gd.success){toast(gd.error||'Notebook generation failed','terr');return;}
      notebooks=(gd.notebooks||[]).map(nb=>({name:nb.name,code:nb.code}));
    }catch(e){toast('Error generating notebooks: '+e.message,'terr');return;}
    folderPath='Notebooks/MetadataPipeline';
    defaultMsg='Deploy metadata pipeline notebooks';
  }

  if(!notebooks.length){toast('No notebooks available to push','terr');return;}

  // Show PR modal (same pattern as data modeling)
  const modalHtml=`
    <div id="nbPrModal" style="position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.5);">
      <div style="background:white;border-radius:12px;padding:24px;width:480px;max-width:90vw;box-shadow:0 20px 40px rgba(0,0,0,0.15);">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;">
          <svg viewBox="0 0 24 24" style="width:24px;height:24px;color:#0078D4;"><polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/><path d="M20.39 18.39A5 5 0 0018 9h-1.26A8 8 0 103 16.3"/></svg>
          <span style="font-size:16px;font-weight:700;color:#1E293B;">Push Notebooks to DevOps</span>
        </div>
        <div style="font-size:12px;color:#64748B;margin-bottom:16px;padding:8px 12px;background:#F1F5F9;border-radius:8px;">
          📁 <strong>${folderPath}/</strong> — ${notebooks.length} notebook(s)
        </div>
        <div style="margin-bottom:12px;">
          <label style="font-size:11px;font-weight:600;color:#475569;">Commit Message</label>
          <input id="nbPrCommitMsg" class="inp" value="${defaultMsg}" style="margin-top:4px;">
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px;">
          <div>
            <label style="font-size:11px;font-weight:600;color:#475569;">Push Mode</label>
            <select id="nbPrMode" class="inp" style="margin-top:4px;">
              <option value="pr" selected>Pull Request (recommended)</option>
              <option value="direct">Direct Push (Admin only)</option>
            </select>
          </div>
          <div>
            <label style="font-size:11px;font-weight:600;color:#475569;">Reviewers</label>
            <input id="nbPrReviewers" class="inp" value="${reviewers}" placeholder="email1, email2" style="margin-top:4px;">
          </div>
        </div>
        <div style="margin-bottom:16px;">
          <label style="display:flex;align-items:center;gap:6px;font-size:11px;color:#475569;cursor:pointer;">
            <input type="checkbox" id="nbPrAutoComplete" checked style="accent-color:#0078D4;"> Auto-complete after approval
          </label>
        </div>
        <div style="display:flex;gap:8px;justify-content:flex-end;">
          <button class="btn btn-ghost" onclick="G('nbPrModal').remove()">Cancel</button>
          <button class="btn btn-primary" onclick="nbPrSubmit('${mode}','${folderPath}')" style="background:#0078D4;">
            <svg viewBox="0 0 24 24" style="width:14px;height:14px;"><polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/><path d="M20.39 18.39A5 5 0 0018 9h-1.26A8 8 0 103 16.3"/></svg>
            Push to DevOps
          </button>
        </div>
      </div>
    </div>`;
  document.body.insertAdjacentHTML('beforeend',modalHtml);
  // Store notebooks for the submit function
  window._nbPushData=notebooks;
}

async function nbPrSubmit(mode, folderPath){
  const notebooks=window._nbPushData||[];
  const commitMsg=G('nbPrCommitMsg').value.trim()||'Deploy notebooks';
  const pushMode=G('nbPrMode').value;
  const reviewers=G('nbPrReviewers').value.trim();
  const autoComplete=G('nbPrAutoComplete').checked;
  const modal=G('nbPrModal');
  // Disable button
  modal.querySelectorAll('button').forEach(b=>b.disabled=true);
  toast('Pushing to Azure DevOps…','tinfo');
  try{
    const r=await fetch('/api/v1/notebooks/push-devops',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        notebooks,
        folder_path:folderPath,
        commit_message:commitMsg,
        push_mode:pushMode,
        reviewers:reviewers,
        auto_complete:autoComplete,
      })
    });
    const d=await r.json();
    if(d.success){
      modal.remove();
      if(d.mode==='pr'&&d.pr_url){
        toast('PR created! '+d.files_pushed+' files pushed.','tok');
        // Show success badge
        const badge=document.createElement('div');
        badge.className='alert a-ok';
        badge.style.cssText='position:fixed;top:80px;right:20px;z-index:9999;max-width:400px;box-shadow:0 4px 12px rgba(0,0,0,0.1);';
        badge.innerHTML=`<span class="a-ico">✓</span><div><strong>PR #${d.pr_id} created</strong><br><a href="${d.pr_url}" target="_blank" style="font-size:11px;color:#0078D4;">View Pull Request →</a></div>`;
        document.body.appendChild(badge);
        setTimeout(()=>badge.remove(),8000);
      }else{
        toast('Pushed '+d.files_pushed+' files directly to '+folderPath,'tok');
      }
    }else{
      toast(d.error||'Push failed','terr');
      modal.querySelectorAll('button').forEach(b=>b.disabled=false);
    }
  }catch(e){
    toast('Error: '+e.message,'terr');
    modal.querySelectorAll('button').forEach(b=>b.disabled=false);
  }
  window._nbPushData=null;
}


async function testConn(){
  const host=G('dbHost').value.trim(),token=G('dbToken').value.trim();
  if(!host||!token){toast('Host and token required.','terr');return;}
  G('connStatus').innerHTML='<div class="alert a-info"><span class="spin" style="border-top-color:var(--blue-fg)"></span> Connecting…</div>';
  G('connInfo').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Fetching workspace info…</span></div>';
  try{
    const r=await fetch('/api/v1/databricks/test-connection',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({host,token})});
    const d=await r.json();
    if(d.success){
      G('connStatus').innerHTML='<div class="alert a-ok"><span class="a-ico">✓</span> Connected to Databricks</div>';
      let h=`<div class="alert a-info" style="margin-bottom:12px;"><span class="a-ico">🌐</span><span><strong>Host:</strong> ${host}</span></div>`;
      (d.clusters||[]).forEach(c=>{const sc=c.state==='RUNNING'?'tag-run':c.state==='TERMINATED'?'tag-stop':'tag-pend';h+=`<div class="cl-card"><div class="cl-name">${c.cluster_name} <span class="tag ${sc}">${c.state}</span></div><div class="cl-meta"><span>ID: ${c.cluster_id}</span><span>DBR ${c.spark_version||'N/A'}</span></div></div>`;});
      if(!(d.clusters||[]).length)h+='<div class="alert a-warn"><span class="a-ico">⚠</span> No clusters found.</div>';
      G('connInfo').innerHTML=h;toast('Connected to Databricks!','tok');G('wf2').className='wf-step done';
    }else{G('connStatus').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error}</div>`;G('connInfo').innerHTML=`<pre style="font-size:11px;color:var(--t3);padding:10px;">${JSON.stringify(d,null,2)}</pre>`;toast('Connection failed: '+d.error,'terr');}
  }catch(e){G('connStatus').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;toast('Error: '+e.message,'terr');}
}

// ── UC Config Loader ─────────────────────────────────────────────────────────
let _ucCatalogSchemas = [];

async function ucInit(){
  try{
    const r=await fetch('/api/v1/uc/config');
    const d=await r.json();
    if(d.success){
      G('ucHostDisplay').textContent=d.host||'Not configured';
      G('ucHostDisplay').style.color=d.host?'var(--t1)':'#EF4444';
      G('ucTokenDisplay').innerHTML=d.has_token?'<span style="color:#10B981;">✓ Token configured (hidden)</span>':'<span style="color:#EF4444;">✕ No token in deployconfig.json</span>';
      _ucCatalogSchemas=d.catalog_schemas||[];
      const sel=G('ucCatalog');
      sel.innerHTML='<option value="">— Select catalog —</option>';
      const cats=[...new Set(_ucCatalogSchemas.map(c=>c.catalog))];
      cats.forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c;sel.appendChild(o);});
    }
  }catch(e){console.error('ucInit',e);}
}

function ucOnCatalogChange(){
  const cat=G('ucCatalog').value;
  const sel=G('ucSchema');
  sel.innerHTML='<option value="">— Select schema —</option>';
  if(!cat)return;
  _ucCatalogSchemas.filter(c=>c.catalog===cat).forEach(cs=>{
    const o=document.createElement('option');o.value=cs.schema;o.textContent=cs.schema;sel.appendChild(o);
  });
}

async function loadUCTables(){
  const cat=G('ucCatalog').value,sch=G('ucSchema').value;
  if(!cat||!sch){toast('Select catalog and schema first','terr');return;}
  G('ucResults').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Loading…</span></div>';
  try{
    const r=await fetch('/api/v1/unity-catalog/tables',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:cat,schema:sch})});
    const d=await r.json();
    if(d.success){
      const tbls=d.tables||[];
      G('ucTableList').innerHTML=tbls.length?tbls.map(t=>`<div class="tbl-item" id="utbl${t.table_name}" onclick="selUCTbl('${t.table_name}')"><span style="color:var(--t4);font-size:11px;">⊞</span> ${cat}.${sch}.<strong>${t.table_name}</strong>${t.table_type&&t.table_type!=='N/A'?`<span style="margin-left:auto;font-size:10px;color:var(--t3);">${t.table_type}</span>`:''}</div>`).join(''):'<div class="empty" style="padding:10px;"><div class="empty-s">No tables found.</div></div>';
      G('ucWarehouse').innerHTML=(d.warehouses||[]).map(w=>`<option value="${w.id}">${w.name} (${w.state})</option>`).join('')||'<option value="">No warehouses</option>';
      G('ucResults').innerHTML=`<div class="alert a-ok"><span class="a-ico">✓</span> Loaded ${tbls.length} tables, ${(d.warehouses||[]).length} warehouses.</div>`;
      toast('Loaded '+tbls.length+' tables!','tok');
    }else{G('ucResults').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error}</div>`;}
  }catch(e){G('ucResults').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;}
}

function selUCTbl(t){UC_TABLE=t;G('ucTable').value=t;document.querySelectorAll('.tbl-item').forEach(el=>el.classList.remove('selected'));const el=G('utbl'+t);if(el)el.classList.add('selected');}

function _renderUCResult(d){
  if(!d.success) return `<div class="alert a-err"><span class="a-ico">✕</span>${d.error||d.message||'Unknown error'}</div>`;
  if(d.sql_type==='query'){
    if(!d.columns||d.columns.length===0) return '<div class="alert a-ok"><span class="a-ico">✓</span> Statement executed successfully (no rows returned).</div>';
    const colDefs=d.columns.map(c=>`<th>${c}</th>`).join('');
    const rowDefs=(d.rows||[]).map(r=>`<tr>${r.map(v=>`<td title="${String(v??'').replace(/"/g,'&quot;')}">${v??'<em style="color:var(--t3)">null</em>'}</td>`).join('')}</tr>`).join('');
    const info=`<div style="font-size:11px;color:var(--t3);margin-top:6px;padding:0 2px;">${d.row_count} row${d.row_count!==1?'s':''} · ${d.columns.length} column${d.columns.length!==1?'s':''}</div>`;
    return `<div style="overflow-x:auto;"><table class="uc-tbl"><thead><tr>${colDefs}</tr></thead><tbody>${rowDefs}</tbody></table></div>${info}`;
  }
  if(d.sql_type==='statement') return '<div class="alert a-ok"><span class="a-ico">✓</span> Statement executed successfully.</div>';
  if(d.steps){
    const cfg={PASS:{ico:'✓',col:'var(--green)',bg:'var(--green-light)'},FAIL:{ico:'✕',col:'var(--red)',bg:'var(--red-light)'},WARN:{ico:'⚠',col:'var(--amber)',bg:'var(--amber-light)'},INFO:{ico:'ℹ',col:'var(--blue)',bg:'var(--blue-light)'}};
    const stepsHtml=d.steps.map(s=>{
      const c=cfg[s.status]||{ico:'·',col:'var(--t2)',bg:'var(--surface-2)'};
      let extra='';
      if(s.columns&&s.sample_rows&&s.sample_rows.length>0){
        const h=s.columns.map(c=>`<th>${c}</th>`).join('');
        const b=s.sample_rows.map(r=>`<tr>${r.map(v=>`<td>${v??''}</td>`).join('')}</tr>`).join('');
        extra=`<div style="overflow-x:auto;margin-top:6px;"><table class="uc-tbl"><thead><tr>${h}</tr></thead><tbody>${b}</tbody></table></div>`;
      }
      return `<div class="uc-step"><span class="uc-step-icon" style="color:${c.col};">${c.ico}</span><div style="flex:1;"><div class="uc-step-title">${s.step}</div><div class="uc-step-detail">${s.detail}</div>${extra}</div><span class="uc-step-badge" style="color:${c.col};background:${c.bg};">${s.status}</span></div>`;
    }).join('');
    const meta=`<div style="font-size:10.5px;color:var(--t3);margin-top:8px;padding:0 2px;">Table: <code>${d.table}</code> · ${d.executed_at||''}</div>`;
    return `<div>${stepsHtml}</div>${meta}`;
  }
  if(d.columns&&d.rows){
    const h=d.columns.map(c=>`<th>${c}</th>`).join('');
    const b=(d.rows||[]).map(r=>`<tr>${r.map(v=>`<td>${v??''}</td>`).join('')}</tr>`).join('');
    return `<div style="overflow-x:auto;"><table class="uc-tbl"><thead><tr>${h}</tr></thead><tbody>${b}</tbody></table></div><div style="font-size:11px;color:var(--t3);margin-top:6px;">${d.preview_rows} preview rows · ${d.total_rows} total</div>`;
  }
  return `<pre style="font-size:11px;color:var(--t2);white-space:pre-wrap;">${JSON.stringify(d,null,2)}</pre>`;
}

async function _ucPost(endpoint,extra={}){
  const cat=G('ucCatalog').value,sch=G('ucSchema').value,wh=G('ucWarehouse').value;
  G('ucResults').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Running…</span></div>';
  try{
    const r=await fetch(endpoint,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:cat,schema:sch,warehouse_id:wh,...extra})});
    const d=await r.json();
    G('ucResults').innerHTML=_renderUCResult(d);
    if(d.success!==false)toast('Done!','tok');
  }catch(e){G('ucResults').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;}
}

function previewTable(){const t=UC_TABLE||G('ucTable').value,wh=G('ucWarehouse').value;if(!t||!wh){toast('Select a table and warehouse.','tinfo');return;}_ucPost('/api/v1/unity-catalog/preview',{table_name:t});}
function executeTable(){const t=UC_TABLE||G('ucTable').value,wh=G('ucWarehouse').value;if(!t||!wh){toast('Select a table and warehouse.','tinfo');return;}_ucPost('/api/v1/unity-catalog/execute',{table_name:t});}
function runCustomSQL(){const sql=G('ucSQL').value.trim(),wh=G('ucWarehouse').value;if(!sql||!wh){toast('Enter SQL and select a warehouse.','tinfo');return;}_ucPost('/api/v1/unity-catalog/execute',{table_name:'__custom__',execute_sql:sql});}

// loadObjects() removed — counts stay at 0 until user clicks "Load SQL Objects"

// ═══════════════════════════════════════════════════════
// SYSTEM HEALTH CHECK
// ═══════════════════════════════════════════════════════
let HL_SEV_FILTER=null, HL_MONITOR_IVS={};

async function _hlSyncFromConfig(){
  try{
    const r=await fetch('/api/v1/deploy-config');
    const cfg=await r.json();
    G('hlHost').value=cfg.databricks_host||'';
    G('hlToken').value=cfg.databricks_token||'';
    const src=cfg.source||{};
    G('hlSrcServer').value=src.server||'';
    G('hlSrcDb').value=src.database||'';
    G('hlSrcUser').value=src.username||'';
    G('hlSrcPass').value=src.password||'';
    const lbl=G('hlConnLabel');
    if(lbl) lbl.textContent=(cfg.databricks_host||'').replace('https://','').replace(/\/$/,'') + ' \u2022 ' + (src.server||'').split('.')[0] + '/' + (src.database||'');
  }catch(e){console.warn('hlSyncFromConfig',e);}
}

async function hlFetchRecentRuns(){
  const sel=G('hlRunId');
  const prev=sel.value;
  sel.innerHTML='<option value="">Loading runs\u2026</option>';
  try{
    const r=await fetch('/api/v1/healer/recent-runs');
    const d=await r.json();
    if(!d.success||!d.runs||!d.runs.length){
      sel.innerHTML='<option value="">-- No runs found --</option>';
      return;
    }
    sel.innerHTML='<option value="">-- Select a Run ID --</option>';
    d.runs.forEach(run=>{
      const st=run.start_time?new Date(run.start_time).toLocaleString():'';
      const state=run.result_state||run.life_cycle||'';
      const stateColor=state==='SUCCESS'?'\u2705':state==='FAILED'?'\u274c':state==='RUNNING'?'\u23f3':'\u2022';
      const name=run.run_name||('Job '+run.job_id);
      const opt=document.createElement('option');
      opt.value=run.run_id;
      opt.textContent=`${stateColor} #${run.run_id} \u2014 ${name} (${state}) ${st}`;
      sel.appendChild(opt);
    });
    if(prev) sel.value=prev;
  }catch(e){
    sel.innerHTML='<option value="">-- Error loading runs --</option>';
    console.warn('hlFetchRecentRuns',e);
  }
}

function _hlCreds(){
  return {
    host:        G('hlHost').value.trim(),
    token:       G('hlToken').value.trim(),
    server:      G('hlSrcServer').value.trim(),
    database:    G('hlSrcDb').value.trim(),
    source_type: 'sqlserver',
    username:    G('hlSrcUser').value.trim(),
    password:    G('hlSrcPass').value.trim(),
  };
}

// ── Health Check ───────────────────────────────────
async function hlRunHealthCheck(){
  const btn=G('btnHlCheck');
  btn.disabled=true; btn.textContent='Checking…';
  G('hlChecks').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Running health diagnostics…</span></div>';
  try{
    await _hlSyncFromConfig();
    const c=_hlCreds();
    const r=await fetch('/api/v1/healer/health-check',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(c)});
    const d=await r.json();
    if(!d.success){G('hlChecks').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error||'Health check failed'}</div>`;return;}

    // Update overall status
    const pulse=G('hlPulse');
    const overall=G('hlOverall');
    pulse.className='heal-pulse '+(d.overall||'unknown');
    overall.className='heal-overall '+(d.overall||'unknown');
    overall.textContent=d.overall||'Unknown';

    // Render checks
    const checks=d.checks||[];
    G('hlChecks').innerHTML=checks.map(c=>
      `<div class="hl-check-row">
        <div class="hl-check-dot ${c.status}"></div>
        <div class="hl-check-name">${escHtml(c.name)}</div>
        <div class="hl-check-detail">${escHtml(c.detail)}</div>
      </div>`
    ).join('');

    toast(`Health: ${d.overall} (${checks.length} checks)`,'tok');
    hlRefreshHistory();
  }catch(e){G('hlChecks').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;toast(e.message,'terr');}
  finally{
    btn.disabled=false;
    btn.innerHTML='<svg viewBox="0 0 24 24"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg> Run Health Check';
  }
}

// ── Diagnose ───────────────────────────────────────
async function hlDiagnose(){
  const text=G('hlErrorText').value.trim();
  if(!text){toast('Paste an error message to diagnose.','tinfo');return;}
  const btn=G('btnHlDiagnose');
  btn.disabled=true; btn.textContent='Analyzing…';
  G('hlDiagResult').style.display='none';
  try{
    const r=await fetch('/api/v1/healer/diagnose',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({error_text:text})});
    const d=await r.json();
    if(!d.success){G('hlDiagResult').style.display='';G('hlDiagResult').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error||'Diagnosis failed'}</div>`;return;}

    const sev=d.severity||'info';
    const sevIcons={info:'ℹ',warning:'⚠',error:'✕',critical:'⚡'};
    G('hlDiagResult').style.display='';
    G('hlDiagResult').innerHTML=`
      <div class="hl-diag-card ${sev}">
        <div class="hl-diag-hdr">
          <span class="hl-diag-sev ${sev}">${sevIcons[sev]||'·'} ${sev}</span>
          <span class="hl-diag-cat">${d.category||'UNKNOWN'}</span>
        </div>
        <div class="hl-diag-desc">${escHtml(d.description||'')}</div>
        <div class="hl-diag-rec">
          <strong>🔧 Recommendation:</strong> ${escHtml(d.recommendation||'')}
        </div>
        <div style="margin-top:8px;display:flex;gap:6px;">
          <button class="btn btn-primary btn-xs" onclick="hlExecuteHeal('${d.action||'notify'}','${d.category||''}')">
            ⚡ Auto-Heal: ${d.action||'notify'}
          </button>
          <button class="btn btn-ghost btn-xs" onclick="hlExecuteHeal('skip_table','${d.category||''}')">
            ⏭ Skip & Continue
          </button>
        </div>
      </div>`;
    toast(`Diagnosed: ${d.category} (${sev})`,'tok');
    hlRefreshHistory();
  }catch(e){
    G('hlDiagResult').style.display='';
    G('hlDiagResult').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;
  }finally{
    btn.disabled=false;
    btn.innerHTML='<svg viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg> Diagnose & Recommend';
  }
}

// ── Execute Heal ───────────────────────────────────
async function hlExecuteHeal(action, category){
  const c=_hlCreds();
  toast(`Executing heal: ${action}…`,'tinfo');
  try{
    const r=await fetch('/api/v1/healer/heal',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action, host:c.host, token:c.token, context:{category, job_key:'manual_'+Date.now()}})});
    const d=await r.json();
    if(d.success!==false){
      toast(`✓ Heal "${action}": ${d.message||'done'}`,'tok',4000);
    }else{
      toast(`✕ Heal failed: ${d.message||d.error||'unknown'}`,'terr',4000);
    }
    hlRefreshHistory();
  }catch(e){toast(e.message,'terr');}
}

// ── Restore Points ─────────────────────────────────
async function hlCreateRp(){
  const name=G('hlRpName').value.trim();
  if(!name){toast('Enter a restore point name.','tinfo');return;}
  try{
    const r=await fetch('/api/v1/healer/restore-point',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({key:name, metadata:{created_by:'user',source:'ui'}})});
    const d=await r.json();
    if(d.success){toast(`Restore point "${name}" created!`,'tok');G('hlRpName').value='';hlLoadRps();}
    else toast(d.error||'Failed','terr');
  }catch(e){toast(e.message,'terr');}
}

async function hlLoadRps(){
  try{
    const r=await fetch('/api/v1/healer/restore-points');
    const d=await r.json();
    const rps=d.restore_points||[];
    G('hlRpCount').style.display=rps.length?'':'none';
    G('hlRpCount').textContent=rps.length+' saved';
    if(!rps.length){G('hlRpList').innerHTML='<div class="empty" style="padding:20px;"><div class="empty-ico">📌</div><div class="empty-t">No Restore Points</div><div class="empty-s">Create one before running pipelines.</div></div>';return;}
    G('hlRpList').innerHTML=rps.map(rp=>
      `<div class="hl-rp-row">
        <div style="color:var(--blue);flex-shrink:0;">📌</div>
        <div class="hl-rp-name">${escHtml(rp.key)}</div>
        <div class="hl-rp-time">${new Date(rp.timestamp).toLocaleString()}</div>
        <div class="hl-rp-del" onclick="hlDeleteRp('${rp.key}')">✕</div>
      </div>`
    ).join('');
  }catch(e){toast(e.message,'terr');}
}

async function hlDeleteRp(key){
  try{
    await fetch(`/api/v1/healer/restore-point/${encodeURIComponent(key)}`,{method:'DELETE'});
    toast(`Restore point "${key}" deleted.`,'tok');
    hlLoadRps();
  }catch(e){toast(e.message,'terr');}
}

// ── Monitors ───────────────────────────────────────
async function hlStartMonitor(){
  const runId=G('hlRunId').value.trim();
  if(!runId){toast('Select a Databricks Run to monitor.','tinfo');return;}
  await _hlSyncFromConfig();
  const c=_hlCreds();
  const autoHeal=G('hlAutoHeal').checked;
  try{
    const r=await fetch('/api/v1/healer/monitor/start',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({run_id:parseInt(runId), host:c.host, token:c.token, auto_heal:autoHeal})});
    const d=await r.json();
    if(d.success){
      toast(`Monitor started for run ${runId}`,'tok');
      G('hlRunId').value='';
      const mon=d.monitor;
      // Start polling
      HL_MONITOR_IVS[mon.monitor_id]=setInterval(()=>hlPollMonitor(mon.monitor_id),5000);
      hlRefreshMonitors();
    }else{toast(d.error||'Failed','terr');}
  }catch(e){toast(e.message,'terr');}
}

async function hlPollMonitor(monitorId){
  const c=_hlCreds();
  try{
    const r=await fetch(`/api/v1/healer/monitor/check/${monitorId}`,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({host:c.host,token:c.token})});
    const d=await r.json();
    if(d.success){
      const mon=d.monitor;
      if(mon.status==='completed'||mon.status==='stopped'||mon.status==='failed'){
        clearInterval(HL_MONITOR_IVS[monitorId]);
        delete HL_MONITOR_IVS[monitorId];
        if(mon.status==='completed')toast(`Run ${mon.run_id} completed!`,'tok');
        else if(mon.status==='failed')toast(`Run ${mon.run_id} failed!`,'terr');
      }
      hlRefreshMonitors();
      hlRefreshHistory();
    }
  }catch(e){/* silently retry on next poll */}
}

async function hlRefreshMonitors(){
  try{
    const r=await fetch('/api/v1/healer/monitors');
    const d=await r.json();
    const mons=d.monitors||[];
    if(!mons.length){G('hlMonitors').innerHTML='<div style="font-size:12px;color:var(--t4);text-align:center;padding:10px;">No active monitors</div>';return;}
    G('hlMonitors').innerHTML=mons.map(m=>{
      const heals=m.heals||[];
      const lastEvt=(m.events||[]).slice(-1)[0];
      return `<div class="hl-monitor-row">
        <div class="hl-mon-status ${m.status||'watching'}"></div>
        <div style="flex:1;">
          <div style="font-size:12px;font-weight:600;color:var(--t1);">Run #${m.run_id}</div>
          <div style="font-size:10.5px;color:var(--t3);">${m.status||'watching'}${heals.length?' · '+heals.length+' heal(s)':''}${lastEvt?' · '+lastEvt.msg.substring(0,60):''}</div>
        </div>
        <button class="btn btn-ghost btn-xs" onclick="hlStopMonitor('${m.monitor_id}')">Stop</button>
      </div>`;
    }).join('');
  }catch(e){/* ignore */}
}

async function hlStopMonitor(monitorId){
  try{
    await fetch(`/api/v1/healer/monitor/stop/${monitorId}`,{method:'POST'});
    if(HL_MONITOR_IVS[monitorId]){clearInterval(HL_MONITOR_IVS[monitorId]);delete HL_MONITOR_IVS[monitorId];}
    hlRefreshMonitors();
    toast('Monitor stopped.','tok');
  }catch(e){toast(e.message,'terr');}
}

// ── Healing Rules ──────────────────────────────────
async function hlLoadRules(){
  try{
    const r=await fetch('/api/v1/healer/rules');
    const d=await r.json();
    const rules=d.rules||[];
    G('hlRuleCount').style.display=rules.length?'':'none';
    G('hlRuleCount').textContent=rules.filter(r=>r.enabled).length+' active';
    G('hlRules').innerHTML=rules.map(rule=>
      `<div class="hl-rule-row">
        <div class="hl-rule-toggle ${rule.enabled?'on':''}" onclick="hlToggleRule(${rule.id},${!rule.enabled},this)" title="${rule.enabled?'Disable':'Enable'}"></div>
        <div class="hl-rule-name" title="${escHtml(rule.description||'')}">${escHtml(rule.name)}</div>
        <div class="hl-rule-cat">${rule.category}</div>
      </div>`
    ).join('');
  }catch(e){toast(e.message,'terr');}
}

async function hlToggleRule(id, enabled, el){
  try{
    const r=await fetch('/api/v1/healer/rules/toggle',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rule_id:id, enabled})});
    const d=await r.json();
    if(d.success){
      el.className='hl-rule-toggle '+(enabled?'on':'');
      toast(`Rule ${d.rule.name}: ${enabled?'enabled':'disabled'}`,'tok');
      hlLoadRules();
    }
  }catch(e){toast(e.message,'terr');}
}

// ── Audit History ──────────────────────────────────
function hlFilterSev(sev, btn){
  HL_SEV_FILTER=sev;
  document.querySelectorAll('.hl-sev-btn').forEach(b=>b.classList.remove('active'));
  if(btn)btn.classList.add('active');
  hlRefreshHistory();
}

async function hlRefreshHistory(){
  try{
    const url='/api/v1/healer/history?limit=100'+(HL_SEV_FILTER?'&severity='+HL_SEV_FILTER:'');
    const r=await fetch(url);
    const d=await r.json();
    const items=d.history||[];
    G('hlEventCount').style.display=items.length?'':'none';
    G('hlEventCount').textContent=items.length+' events';
    if(!items.length){G('hlHistory').innerHTML='<div style="color:var(--t4);text-align:center;padding:16px;">No events yet — run a health check to start.</div>';return;}
    G('hlHistory').innerHTML=items.map(h=>{
      const t=new Date(h.timestamp).toLocaleTimeString();
      return `<div class="hl-evt">
        <div class="hl-evt-sev ${h.severity}"></div>
        <div class="hl-evt-time">${t}</div>
        <div class="hl-evt-msg">${escHtml(h.message)}${h.action_taken?' <span style="color:var(--blue);font-weight:600;">→ '+h.action_taken+'</span>':''}${h.success===true?' <span style="color:var(--green-fg);">✓</span>':h.success===false?' <span style="color:var(--red-fg);">✕</span>':''}</div>
      </div>`;
    }).join('');
    G('hlHistory').scrollTop=0;
  }catch(e){/* ignore */}
}

async function hlClearHistory(){
  try{
    await fetch('/api/v1/healer/history/clear',{method:'POST'});
    G('hlHistory').innerHTML='<div style="color:var(--t4);text-align:center;padding:16px;">History cleared.</div>';
    G('hlEventCount').style.display='none';
    toast('History cleared.','tok');
  }catch(e){toast(e.message,'terr');}
}

async function hlRefreshStats(){
  try{
    const r=await fetch('/api/v1/healer/stats');
    const d=await r.json();
    const box=G('hlStatsBox');
    box.style.display='';
    box.innerHTML=`
      <div class="hl-stats-grid">
        <div class="hl-stat-box"><div class="n" style="color:var(--blue-fg);">${d.total_events||0}</div><div class="l">Total Events</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--green-fg);">${d.heals_succeeded||0}</div><div class="l">Heals OK</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--red-fg);">${d.heals_failed||0}</div><div class="l">Heals Failed</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--amber-fg);">${d.active_monitors||0}</div><div class="l">Monitors</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--t1);">${d.restore_points||0}</div><div class="l">Restore Pts</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--accent-primary);">${d.active_rules||0}</div><div class="l">Active Rules</div></div>
      </div>`;
  }catch(e){toast(e.message,'terr');}
}

// Auto-load rules & restore points when healer tab is first opened
(function(){
  const origSwitch=switchTab;
  let healerLoaded=false;
  switchTab=function(id,btn){
    origSwitch(id,btn);
    if(id==='healer'&&!healerLoaded){
      healerLoaded=true;
      hlLoadRules();
      hlLoadRps();
    }
    if(id==='wf-scheduler'){
      if(typeof schLoadJobs==='function') schLoadJobs();
      if(typeof schRefresh==='function') schRefresh();
    }
    if(id==='wf-metadata'){
      // Restore Deploy Notebooks card visibility on page reload / tab switch
      if(typeof wfCheckMetaStatus==='function') wfCheckMetaStatus();
    }
  };
})();

// ═════════════════════════════════════════════════════════════════════════════
// WORKFLOW MANAGER — JAVASCRIPT
// ═════════════════════════════════════════════════════════════════════════════

/* ─── MetadataFlow — Databricks Persistence ─── */
let _wfMetaReady=false;
let _wfSelectedGroups=new Set(); // multi-select for batch Databricks run

let _cachedDeployConfig=null;
async function _ensureDeployConfig(){
  if(_cachedDeployConfig) return _cachedDeployConfig;
  try{
    const r=await fetch('/api/v1/deploy-config');const d=await r.json();
    if(d.success&&d.config) _cachedDeployConfig=d.config;
  }catch(e){}
  return _cachedDeployConfig||{};
}
/* ── Sync hidden wfDbr* fields from Settings / deployconfig and update label ── */
async function _wfSyncHiddenFields(){
  const cfg=await _ensureDeployConfig();
  const h=G('cfgDbrHost')?.value?.trim()||cfg.databricks_host||G('dbHost')?.value?.trim()||'';
  const t=G('cfgDbrToken')?.value?.trim()||cfg.databricks_token||G('dbToken')?.value?.trim()||'';
  const c=G('cfgMetaCatalog')?.value?.trim()||cfg.metadata_catalog||'admin_source';
  const s=G('cfgMetaSchema')?.value?.trim()||cfg.metadata_schema||'configtables';
  if(G('wfDbrHost'))G('wfDbrHost').value=h;
  if(G('wfDbrToken'))G('wfDbrToken').value=t;
  if(G('wfDbrCatalog'))G('wfDbrCatalog').value=c;
  if(G('wfDbrSchema'))G('wfDbrSchema').value=s;
  // Auto-populate Landing Path from volume_path config
  const vPath=G('cfgVolPath')?.value?.trim()||cfg.volume_path||'';
  if(vPath){
    if(G('wfNbLandingPath'))G('wfNbLandingPath').value=vPath;
    if(G('mdlLandingPath'))G('mdlLandingPath').value=vPath;
  }
  // Auto-populate Pipeline Mode from cdc.dlt_mode config
  const dltMode=G('cfgDltMode')?.value?.trim()||(cfg.cdc&&cfg.cdc.dlt_mode)||'standard';
  const pmSel=G('wfNbPipelineMode');
  if(pmSel){
    pmSel.value=dltMode;
    pmSel.dispatchEvent(new Event('change'));
  }
  // Update mode hint text
  const hint=G('wfNbModeHint');
  if(hint) hint.textContent=dltMode==='dlt'?'SDP: 3 notebooks (Extract, SDP Pipeline, SDP Orchestrator) \u2014 Auto Loader + Expectations':'Standard: 4 notebooks (Extract, Bronze, Silver, Orchestrator)';
  _wfUpdateConnLabel();
}
function _wfUpdateConnLabel(){
  const lbl=G('wfMetaConnLabel');if(!lbl)return;
  const c=_wfDbrCreds();
  if(c.host){
    const short=c.host.replace(/^https?:\/\//,'').replace(/\/$/,'');
    lbl.textContent=short+'  •  '+c.catalog+'.'+c.schema;
    lbl.style.color='var(--t2)';
  } else {
    lbl.innerHTML='<span style=\"color:var(--red);\">No connection configured — go to <strong>Settings</strong></span>';
  }
}
function _wfDbrCreds(){
  // Read from Settings fields → hidden fields → deployconfig cache
  const host= G('cfgDbrHost')?.value?.trim() || G('wfDbrHost')?.value?.trim() || '';
  const token= G('cfgDbrToken')?.value?.trim() || G('wfDbrToken')?.value?.trim() || '';
  const catalog= G('cfgMetaCatalog')?.value?.trim() || G('wfDbrCatalog')?.value?.trim() || 'admin_source';
  const schema= G('cfgMetaSchema')?.value?.trim() || G('wfDbrSchema')?.value?.trim() || 'configtables';
  return {host,token,catalog,schema};
}
async function _wfDbrCredsWithFallback(){
  let c=_wfDbrCreds();
  if(!c.host||!c.token){
    const cfg=await _ensureDeployConfig();
    c.host=c.host||cfg.databricks_host||'';
    c.token=c.token||cfg.databricks_token||'';
    c.catalog=c.catalog||cfg.metadata_catalog||'admin_source';
    c.schema=c.schema||cfg.metadata_schema||'configtables';
    // Populate hidden fields + Settings fields so subsequent calls work
    if(c.host){if(G('wfDbrHost'))G('wfDbrHost').value=c.host; if(G('cfgDbrHost')&&!G('cfgDbrHost').value)G('cfgDbrHost').value=c.host;}
    if(c.token){if(G('wfDbrToken'))G('wfDbrToken').value=c.token; if(G('cfgDbrToken')&&!G('cfgDbrToken').value)G('cfgDbrToken').value=c.token;}
    if(c.catalog){if(G('wfDbrCatalog'))G('wfDbrCatalog').value=c.catalog; if(G('cfgMetaCatalog')&&!G('cfgMetaCatalog').value)G('cfgMetaCatalog').value=c.catalog;}
    if(c.schema){if(G('wfDbrSchema'))G('wfDbrSchema').value=c.schema; if(G('cfgMetaSchema')&&!G('cfgMetaSchema').value)G('cfgMetaSchema').value=c.schema;}
    _wfUpdateConnLabel();
  }
  return c;
}
function _wfSourceConfig(){
  const st=G('wfSrcType')?.value||'sqlserver';
  const snowAccount=G('wfSrcAccount')?.value?.trim()||'';
  const cfg={
    source_type: st,
    // Snowflake identifies itself by account, not server -- mirror it into
    // "server" too so nothing downstream (job naming, notebook connection
    // resolution) ever sees an empty connection identifier for this source.
    server:      st==='snowflake'?snowAccount:(G('wfSrcServer')?.value?.trim()||''),
    database:    st==='snowflake'?(G('wfSrcSnowDb')?.value?.trim()||G('wfSrcDb')?.value?.trim()||''):(_NON_SQL_SRC(st)?'':(G('wfSrcDb')?.value?.trim()||'')),
    username:    G('wfSrcUser')?.value?.trim()||'',
  };
  if(st==='snowflake'){
    cfg.account=snowAccount;
    cfg.warehouse=G('wfSrcWarehouse')?.value?.trim()||'';
    cfg.role=G('wfSrcRole')?.value?.trim()||'';
  }
  if(st==='sharepoint'){
    cfg.tenant_id=G('wfSrcTenantId')?.value?.trim()||'';
  }
  if(st==='api'){
    cfg.api_auth_type=(G('wfSrcApiAuthType')||{}).value||'none';
    cfg.api_key_header=G('wfSrcApiKeyHeader')?.value?.trim()||'';
  }
  return cfg;
}
function _wfTargetConfig(){
  const c=_wfDbrCreds();
  // Multi-catalog fields (volumes / bronze / silver)
  const vc=G('wfVolCatalog')?.value?.trim()||'';
  const bc=G('wfBrzCatalog')?.value?.trim()||'';
  const sc=G('wfSlvCatalog')?.value?.trim()||'';
  const ts=G('wfTgtSchema')?.value?.trim()||G('wfQDefaultTarget')?.value?.trim()||'';
  const cfg = {
    host:             c.host,
    metadata_catalog: c.catalog,
    metadata_schema:  c.schema,
    catalog:          bc || c.catalog,
    schema:           ts || c.schema,
    workspace_path:   G('wfNbWsPath')?.value?.trim()||'/Shared/MetadataPipeline',
    landing_path:     G('wfNbLandingPath')?.value?.trim()||'/mnt/landing',
  };
  // Always include multi-catalog keys so notebooks know the real targets
  if(vc) cfg.volumes_catalog=vc;
  if(bc) cfg.bronze_catalog=bc;
  if(sc) cfg.silver_catalog=sc;
  if(ts) cfg.target_schema=ts;
  // Layer mapping from Pipeline Studio
  cfg.layer_mapping=_wfLayerCollectMapping();
  return cfg;
}

async function wfCreateMetadataFlow(){
  const c=_wfDbrCreds();
  if(!c.host||!c.token){toast('Enter Databricks host and token','terr');return;}
  const btn=G('btnWfMeta');btn.disabled=true;btn.textContent='Provisioning tables…';
  const dot=G('wfMetaDot'),lbl=G('wfMetaLabel'),msg=G('wfMetaMsg');
  dot.style.background='#f59e0b';lbl.textContent='Provisioning…';
  msg.innerHTML='<span style="color:var(--amber);">Creating 5 Delta tables in '+c.catalog+'.'+c.schema+'…</span>';
  try{
    const r=await fetch('/api/v1/workflow/metadata/init',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(c)});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    dot.style.background='#10b981';lbl.textContent='Initialized — '+c.catalog+'.'+c.schema;
    msg.innerHTML='<span style="color:var(--green);">✓ '+d.message+'</span>';
    toast(d.message,'tok');
    _wfMetaReady=true;
    G('btnWfMetaLoad').style.display='';
    G('btnWfMetaSync').style.display='';
    G('wfMetaTablesInfo').style.display='block';
    if(G('wfNotebookCard'))G('wfNotebookCard').style.display='';
    wfCheckMetaStatus();
  }catch(e){
    dot.style.background='#ef4444';lbl.textContent='Failed';
    msg.innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
    toast(e.message,'terr');
  }
  btn.disabled=false;
  btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg> Create MetadataFlow';
}

async function wfCheckMetaStatus(){
  try{
    const r=await fetch('/api/v1/workflow/metadata/status');
    const d=await r.json();
    if(!d.success)return;
    const dot=G('wfMetaDot'),lbl=G('wfMetaLabel');
    if(d.initialized){
      dot.style.background='#10b981';lbl.textContent='Active — '+(d.catalog||'main')+'.'+(d.schema||'default');
      _wfMetaReady=true;
      G('btnWfMetaLoad').style.display='';
      G('btnWfMetaSync').style.display='';
      G('wfMetaTablesInfo').style.display='block';
      if(G('wfNotebookCard'))G('wfNotebookCard').style.display='';
      // Update table counts
      const tbls=d.tables||{};
      const m={wf_pipeline_metadata:'wfMetaTblPipelines',wf_job_metadata:'wfMetaTblJobs',wf_job_metadatahis:'wfMetaTblJobHis',wf_run_history:'wfMetaTblRuns',wf_watermark_metadata:'wfMetaTblWm',wf_source_tables:'wfMetaTblSrc',wf_scheduler_config:'wfMetaTblSchCfg',wf_scheduler_history:'wfMetaTblSchHis'};
      for(const[t,info]of Object.entries(tbls)){
        const el=G(m[t]);
        if(el){
          if(info.exists) el.textContent=info.rows;
          else el.textContent='✕';
        }
      }
    }else{
      dot.style.background='#6b7280';lbl.textContent='Not Configured';
    }
  }catch(e){console.error('wfCheckMetaStatus',e);}
}

async function wfLoadMetadata(){
  const btn=G('btnWfMetaLoad');btn.disabled=true;btn.textContent='Loading…';
  try{
    const r=await fetch('/api/v1/workflow/metadata/load',{method:'POST'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    const l=d.loaded;
    toast('Loaded '+l.pipelines+' pipelines, '+l.jobs+' jobs, '+l.watermarks+' watermarks from Databricks','tok');
    G('wfMetaMsg').innerHTML='<span style="color:var(--green);">✓ Loaded: '+l.pipelines+' pipelines, '+l.jobs+' jobs, '+l.watermarks+' watermarks</span>';
    wfRefreshAll();
  }catch(e){
    toast(e.message,'terr');
    G('wfMetaMsg').innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
  }
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg> Load Existing Metadata';
}

async function wfSyncMetadata(){
  const btn=G('btnWfMetaSync');btn.disabled=true;btn.textContent='Syncing…';
  try{
    // Fix 3: dispatch async; poll /sync-status until done.
    const r=await fetch('/api/v1/workflow/metadata/sync',{method:'POST'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    // If backend returned synced counts directly (sync mode fallback), finish.
    if(d.synced){
      const s=d.synced;
      toast('Synced '+s.pipelines+' pipelines, '+s.jobs+' jobs, '+s.runs+' runs to Databricks','tok');
      G('wfMetaMsg').innerHTML='<span style="color:var(--green);">\u2713 Synced: '+s.pipelines+' pipelines, '+s.jobs+' jobs, '+s.runs+' runs</span>';
    }else if(d.task_id){
      const taskId=d.task_id;
      G('wfMetaMsg').innerHTML='<span style="color:var(--muted);">\u23F3 Sync running in background\u2026</span>';
      // Poll every 2s, max 5 minutes
      let task=null;
      for(let i=0;i<150;i++){
        await new Promise(res=>setTimeout(res,2000));
        const pr=await fetch('/api/v1/workflow/metadata/sync-status/'+encodeURIComponent(taskId));
        const pd=await pr.json();
        if(!pd.success){throw new Error(pd.error||'Poll failed');}
        task=pd.task;
        G('wfMetaMsg').innerHTML='<span style="color:var(--muted);">\u23F3 '+(task.progress||task.status)+'\u2026</span>';
        if(task.status==='succeeded'||task.status==='failed')break;
      }
      if(!task||task.status!=='succeeded'){throw new Error((task&&task.error)||'Sync did not complete');}
      const s=task.synced||{};
      toast('Synced '+(s.pipelines||0)+' pipelines, '+(s.jobs||0)+' jobs, '+(s.runs||0)+' runs to Databricks','tok');
      G('wfMetaMsg').innerHTML='<span style="color:var(--green);">\u2713 Synced: '+(s.pipelines||0)+' pipelines, '+(s.jobs||0)+' jobs, '+(s.runs||0)+' runs</span>';
    }
    wfCheckMetaStatus();
  }catch(e){
    toast(e.message,'terr');
    G('wfMetaMsg').innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
  }
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/><path d="M20.39 18.39A5 5 0 0018 9h-1.26A8 8 0 103 16.3"/></svg> Sync to Databricks';
}

/* Save discovered source tables to Databricks when Discover is clicked */
async function _wfSaveSourcesToDatabricks(){
  if(!_wfMetaReady||!WF_SRC_TABLES.length)return;
  try{
    const c=_wfSrcCreds();
    await fetch('/api/v1/workflow/metadata/save-sources',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({tables:WF_SRC_TABLES,source_config:c})
    });
  }catch(e){console.error('_wfSaveSourcesToDatabricks',e);}
}

/* ─── Deploy Metadata Notebooks ─── */
let _wfNbDeployed=false;

// Pipeline mode hint + DQ panel toggle
(function(){
  const sel=document.getElementById('wfNbPipelineMode');
  if(sel)sel.addEventListener('change',function(){
    const h=document.getElementById('wfNbModeHint');
    const dqStd=document.getElementById('wfDqStandard');
    const dqDlt=document.getElementById('wfDqDlt');
    const dqTag=document.getElementById('wfDqModeTag');
    const isDlt=this.value==='dlt';
    if(h)h.textContent=isDlt
      ?'SDP: 3 notebooks (Extract, SDP Pipeline, SDP Orchestrator) — Auto Loader + Expectations'
      :'Standard: 4 notebooks (Extract, Bronze, Silver, Orchestrator)';
    if(dqStd)dqStd.style.display=isDlt?'none':'block';
    if(dqDlt)dqDlt.style.display=isDlt?'block':'none';
    if(dqTag){dqTag.textContent=isDlt?'SDP':'STANDARD';dqTag.style.background=isDlt?'#f59e0b':'var(--blue)';}
    // Update pipeline mode tag in Active Pipeline Groups
    const pTag=document.getElementById('wfPipelineModeTag');
    if(pTag){
      pTag.textContent=isDlt?'⚡ Declarative Pipelines (SDP)':'🔥 Apache Spark';
      pTag.style.background=isDlt?'#f59e0b':'#3b82f6';
    }
    // Update Quick Create pipeline preview & button text
    const preview=document.getElementById('wfQPipelinePreview');
    if(preview){
      if(isDlt){
        preview.innerHTML='<div style="color:var(--t4);font-weight:600;margin-bottom:2px;">Per table creates:</div>'+
          '<div><span style="color:var(--t4);">1.</span> <span style="color:#2563eb;">ExtractTo_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>'+
          '<div><span style="color:var(--t4);">2.</span> <span style="color:#f59e0b;">SDP_BronzeSilver_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>';
      }else{
        preview.innerHTML='<div style="color:var(--t4);font-weight:600;margin-bottom:2px;">Per table creates:</div>'+
          '<div><span style="color:var(--t4);">1.</span> <span style="color:#2563eb;">SqlExtract_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>'+
          '<div><span style="color:var(--t4);">2.</span> <span style="color:#d97706;">LandingToBronze_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>'+
          '<div><span style="color:var(--t4);">3.</span> <span style="color:#059669;">BronzeToSilver_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>';
      }
    }
    const btnLbl=document.getElementById('btnWfQuickLabel');
    if(btnLbl)btnLbl.textContent=isDlt?'Create 2-Stage Declarative Pipeline':'Create 3-Stage Medallion Pipeline';
    // Re-render pipelines to update per-group badges
    if(typeof wfRefreshPipelines==='function')wfRefreshPipelines();
  });
  // Fire once on load to sync initial state
  sel.dispatchEvent(new Event('change'));
})();

async function wfDeployNotebooks(){
  const c=await _wfDbrCredsWithFallback();
  if(!c.host||!c.token){toast('Configure Databricks connection in Settings first','terr');return;}
  const btn=G('btnWfDeployNb');btn.disabled=true;btn.textContent='Deploying…';
  const dot=G('wfNbDot'),lbl=G('wfNbLabel'),msg=G('wfNbMsg');
  const pipelineMode=(G('wfNbPipelineMode')||{}).value||'standard';
  dot.style.background='#f59e0b';lbl.textContent='Deploying…';
  try{
    const r=await fetch('/api/v1/workflow/notebooks/deploy',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        host:c.host,token:c.token,catalog:c.catalog,schema:c.schema,
        workspace_path:G('wfNbWsPath').value.trim()||'/Shared/MetadataPipeline',
        landing_path:G('wfNbLandingPath').value.trim()||'/mnt/landing',
        pipeline_mode:pipelineMode,
        cdc_mode:(G('cfgCdcMode')||{}).value||'watermark',
        primary_keys:(G('cfgPrimaryKeys')||{}).value ? G('cfgPrimaryKeys').value.split(',').map(s=>s.trim()).filter(Boolean) : [],
        recon_catalog:G('cfgReconCatalog')?.value?.trim()||'reconciliation',
        recon_schema:G('cfgReconSchema')?.value?.trim()||'hr',
        recon_table:G('cfgReconTable')?.value?.trim()||'ReconcilationDetails',
        recon_location:G('cfgReconLocation')?.value?.trim()||'',
        log_catalog:G('cfgLogCatalog')?.value?.trim()||'logging',
        log_schema:G('cfgLogSchema')?.value?.trim()||'hr',
        log_table:G('cfgLogTable')?.value?.trim()||'ExecutionLog',
        log_location:G('cfgLogLocation')?.value?.trim()||'',
      })
    });
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Deploy failed');
    dot.style.background='#10b981';lbl.textContent='Deployed — '+d.workspace_path;
    msg.innerHTML='<span style="color:var(--green);">✓ '+d.message+'</span>';
    toast(d.message,'tok');
    _wfNbDeployed=true;
    // Show results grid
    const grid=G('wfNbResultGrid');
    G('wfNbResults').style.display='block';
    grid.innerHTML=(d.results||[]).map(nb=>{
      const ok=nb.success;
      return `<div style="padding:8px;background:${ok?'var(--green)':'var(--red)'}11;border:1px solid ${ok?'var(--green)':'var(--red)'}33;border-radius:var(--r-xs);text-align:center;">
        <div style="font-size:10px;font-weight:600;color:${ok?'var(--green)':'var(--red)'};">${ok?'✅':'❌'} ${nb.name}</div>
        <div style="font-size:9px;color:var(--t4);margin-top:2px;">${nb.layer} · ${nb.lines} lines</div>
        ${nb.path?'<div style="font-size:8px;color:var(--t4);margin-top:1px;">'+nb.path+'</div>':''}
      </div>`;
    }).join('');
  }catch(e){
    dot.style.background='#ef4444';lbl.textContent='Failed';
    msg.innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
    toast(e.message,'terr');
  }
  btn.disabled=false;
  btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/><path d="M20.39 18.39A5 5 0 0018 9h-1.26A8 8 0 103 16.3"/></svg> Deploy Notebooks';
}

async function wfCheckNbStatus(){
  try{
    const r=await fetch('/api/v1/workflow/notebooks/status');const d=await r.json();
    const dot=G('wfNbDot'),lbl=G('wfNbLabel');
    if(d.deployed){
      dot.style.background='#10b981';lbl.textContent='Deployed — '+d.workspace_path;
      _wfNbDeployed=true;
    }else{
      dot.style.background='#6b7280';lbl.textContent='Not Deployed';
    }
  }catch(e){console.error('wfCheckNbStatus',e);}
}

async function wfRunOnDatabricks(groupId, pwd){
  // Auto-detect readiness — if config has host/token, we can proceed
  if(!_wfNbDeployed||!_wfMetaReady){
    try{
      const cfgr=await fetch('/api/v1/deploy-config');const cfgd=await cfgr.json();
      if(cfgd.success&&cfgd.config?.databricks_host&&cfgd.config?.databricks_token){
        _wfMetaReady=true;
        _wfNbDeployed=true;
      }
    }catch(e){}
  }
  if(!_wfMetaReady){
    toast('Configure Databricks host & token in Settings or MetadataFlow first','terr');return false;
  }
  // Get Databricks credentials — try UI fields first, fallback to deployconfig
  let c=_wfDbrCreds();
  if(!c.host||!c.token){
    try{
      const cfgr=await fetch('/api/v1/deploy-config');const cfgd=await cfgr.json();
      if(cfgd.success&&cfgd.config){
        c.host=c.host||cfgd.config.databricks_host||'';
        c.token=c.token||cfgd.config.databricks_token||'';
        const cats=cfgd.config.catalogs||{};
        const firstCat=Object.keys(cats)[0]||'';
        c.catalog=c.catalog||firstCat||'main';
        c.schema=c.schema||(cats[firstCat]?.schemas?.[0])||'default';
      }
    }catch(e){}
  }
  if(!c.host||!c.token){toast('Databricks host & token required — configure in MetadataFlow or Settings','terr');return false;}
  // Get cluster — try UI dropdown first, fallback to auto-detect running cluster
  let clusterId=(G('wfClusterSelect')||{}).value||'';
  if(!clusterId){
    try{
      const clr=await fetch('/api/v1/workflow/clusters?host='+encodeURIComponent(c.host)+'&token='+encodeURIComponent(c.token));
      const cld=await clr.json();
      if(cld.success&&cld.clusters){
        const running=cld.clusters.find(cl=>cl.state==='RUNNING');
        if(running)clusterId=running.cluster_id;
      }
    }catch(e){}
  }
  if(!clusterId){toast('No running cluster found — start a cluster in Pipeline Studio or MetadataFlow','terr');return false;}
  // Get password — try UI field, then deployconfig
  if(pwd===undefined||pwd===''){
    pwd=(G('wfSrcPass')||{}).value||'';
    if(!pwd){
      try{
        const cfgr=await fetch('/api/v1/deploy-config');const cfgd=await cfgr.json();
        if(cfgd.success&&cfgd.config?.source)pwd=cfgd.config.source.password||'';
      }catch(e){}
    }
  }
  toast('Submitting pipeline to Databricks…','tinfo');
  try{
    const r=await fetch('/api/v1/workflow/pipelines/'+groupId+'/run-databricks',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        host:c.host,token:c.token,catalog:c.catalog,schema:c.schema,
        cluster_id:clusterId,
        password:pwd,
        workspace_path:G('wfNbWsPath')?.value?.trim()||'/Shared/MetadataPipeline',
        landing_path:G('wfNbLandingPath')?.value?.trim()||'/mnt/landing',
        recon_catalog:G('cfgReconCatalog')?.value?.trim()||'reconciliation',
        recon_schema:G('cfgReconSchema')?.value?.trim()||'hr',
        recon_table:G('cfgReconTable')?.value?.trim()||'ReconcilationDetails',
        recon_location:G('cfgReconLocation')?.value?.trim()||'',
        log_catalog:G('cfgLogCatalog')?.value?.trim()||'logging',
        log_schema:G('cfgLogSchema')?.value?.trim()||'hr',
        log_table:G('cfgLogTable')?.value?.trim()||'ExecutionLog',
        log_location:G('cfgLogLocation')?.value?.trim()||'',
      })
    });
    const d=await r.json();
    if(!d.success)throw new Error(d.error||d.message||'Failed to submit');
    toast('Pipeline submitted to Databricks! Run ID: '+d.run_id,'tok');
    if(d.run_url){
      const msgEl=G('wfNbMsg');
      if(msgEl)msgEl.innerHTML='<span style="color:var(--green);">✓ Running → <a href="'+d.run_url+'" target="_blank" style="color:var(--blue);">View Run</a></span>';
    }
    // Auto-open pipeline logs panel if on Pipeline Studio
    if(typeof wfShowPipelineLogs==='function'){
      try{wfShowPipelineLogs(groupId, '');}catch(e){}
    }
    setTimeout(()=>wfRefreshAll(),2000);
    return true;
  }catch(e){
    console.error('wfRunOnDatabricks error:',e);
    toast(e.message,'terr');
    if(typeof wfShowPipelineLogs==='function'){
      try{wfShowPipelineLogs(groupId, '');}catch(ex){}
    }
    return false;
  }
}

/* ─── Multi-Select Pipeline Group Helpers ─── */
function wfToggleGroupSelect(groupId, checked){
  if(checked) _wfSelectedGroups.add(groupId);
  else _wfSelectedGroups.delete(groupId);
  const total=document.querySelectorAll('.wfGrpChk').length;
  _wfUpdateGroupToolbar(total);
}
function wfToggleSelectAllGroups(checked){
  document.querySelectorAll('.wfGrpChk').forEach(cb=>{
    cb.checked=checked;
    const gid=cb.dataset.gid;
    if(checked) _wfSelectedGroups.add(gid);
    else _wfSelectedGroups.delete(gid);
  });
  const total=document.querySelectorAll('.wfGrpChk').length;
  _wfUpdateGroupToolbar(total);
}
function _wfUpdateGroupToolbar(total){
  const n=_wfSelectedGroups.size;
  const lbl=G('wfGroupSelCount');
  if(lbl) lbl.textContent=n+' selected';
  const btn=G('btnRunSelectedDbr');
  if(btn) btn.style.display=n>0?'inline-flex':'none';
  const allCb=G('wfGroupSelectAll');
  if(allCb) allCb.checked=(total>0&&n===total);
}
async function wfRunSelectedOnDatabricks(){
  const ids=[..._wfSelectedGroups];
  if(!ids.length){toast('Select at least one pipeline group','terr');return;}
  toast('Submitting '+ids.length+' pipeline(s) to Databricks…','tinfo');
  // Submit ALL pipelines in parallel — don't await one-by-one
  const results=await Promise.allSettled(ids.map(gid=>wfRunOnDatabricks(gid)));
  const ok=results.filter(r=>r.status==='fulfilled'&&r.value===true).length;
  const fail=results.length-ok;
  _wfSelectedGroups.clear();
  wfRefreshPipelines();
  const msg=ok+' submitted'+(fail?' · '+fail+' failed':'');
  toast(msg, fail?'terr':'tok');
}

function switchAiSubTab(tab,btn){
  // Legacy redirect — sub-tabs replaced by separate pages
  if(tab==='jobs') switchTab('wf-jobs',G('nav-wf-jobs'));
  else switchTab('wf-pipelines',G('nav-wf-pipelines'));
}

function wfToggleWatermark(){
  const v=G('wfLoadType').value;
  G('wfWatermarkWrap').style.display=v==='incremental'?'block':'none';
}

/* ─── Data Source Connection & Table Discovery ─── */
let WF_SRC_TABLES=[];
let _wfSelectedQ=null; // legacy compat — multi-select uses _wfQSelected[]
let _wfSelectedJ=null; // selected table for Job Workflow

function _wfSrcCreds(){
  const st=G('wfSrcType').value;
  const creds={
    source_type: st,
    server:      G('wfSrcServer').value.trim(),
    database:    G('wfSrcDb').value.trim(),
    username:    G('wfSrcUser').value.trim(),
    password:    G('wfSrcPass').value,
  };
  if(st==='snowflake'){
    creds.account=(G('wfSrcAccount')||{}).value||'';
    creds.warehouse=(G('wfSrcWarehouse')||{}).value||'';
    creds.role=(G('wfSrcRole')||{}).value||'';
    creds.database=(G('wfSrcSnowDb')||{}).value||creds.database;
  }
  if(st==='sharepoint'){
    creds.tenant_id=(G('wfSrcTenantId')||{}).value||'';
  }
  if(st==='api'){
    creds.api_auth_type=(G('wfSrcApiAuthType')||{}).value||'none';
    creds.api_key_header=(G('wfSrcApiKeyHeader')||{}).value||'';
  }
  return creds;
}

async function _wfSaveSourceToConfig(){
  try{
    // Load existing config, merge source info, save back
    const lr=await fetch('/api/v1/deploy-config');
    const ld=await lr.json();
    const cfg=ld.success?ld.config:{};
    const _st=(G('wfSrcType')||{}).value||'sqlserver';
    cfg.source={
      source_type: _st,
      server:      (G('wfSrcServer')||{}).value?.trim()||'',
      database:    _st==='snowflake'?((G('wfSrcSnowDb')||{}).value?.trim()||''):(_NON_SQL_SRC(_st)?'':((G('wfSrcDb')||{}).value?.trim()||'')),
      username:    (G('wfSrcUser')||{}).value?.trim()||'',
      password:    (G('wfSrcPass')||{}).value||'',
    };
    if(_st==='snowflake'){
      cfg.source.account=(G('wfSrcAccount')||{}).value?.trim()||'';
      cfg.source.warehouse=(G('wfSrcWarehouse')||{}).value?.trim()||'';
      cfg.source.role=(G('wfSrcRole')||{}).value?.trim()||'';
    }
    if(_st==='sharepoint'){
      cfg.source.tenant_id=(G('wfSrcTenantId')||{}).value?.trim()||'';
    }
    if(_st==='api'){
      cfg.source.api_auth_type=(G('wfSrcApiAuthType')||{}).value||'none';
      cfg.source.api_key_header=(G('wfSrcApiKeyHeader')||{}).value?.trim()||'';
    }
    await fetch('/api/v1/deploy-config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
  }catch(e){console.warn('Could not save source to config:',e);}
}

async function wfTestConnection(){
  const c=_wfSrcCreds();
  const _nonSqlTc=_NON_SQL_SRC(c.source_type);
  if(_nonSqlTc){
    if(!c.server){toast('Enter the source URL','terr');return;}
    if(c.source_type==='sharepoint'&&(!c.tenant_id||!c.username)){toast('Enter Tenant ID and Client ID for SharePoint','terr');return;}
  }
  else if(!c.server||!c.database||!c.username){toast('Enter server, database and username','terr');return;}
  const btn=G('btnWfTest');btn.disabled=true;btn.textContent='Testing…';
  const st=G('wfSrcStatus');
  st.innerHTML='<span style="color:var(--amber);">⏳ Testing…</span>';
  try{
    const r=await fetch('/api/v1/workflow/list-tables',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(c)});
    const d=await r.json();
    if(!r.ok||d.error)throw new Error(d.error||'Connection failed');
    st.innerHTML='<span style="color:var(--green);">🟢 Connected — '+d.total+' tables found</span>';
    toast('Connection successful! '+d.total+' tables available.','tok');
    // Auto-populate tables
    WF_SRC_TABLES=d.tables||[];
    G('wfSrcMsg').innerHTML='<span style="color:var(--green);">✓ '+WF_SRC_TABLES.length+' tables loaded</span>';
    // Auto-save source info to deployconfig.json
    _wfSaveSourceToConfig();
  }catch(e){
    st.innerHTML='<span style="color:var(--red);">🔴 Failed</span>';
    G('wfSrcMsg').innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
    toast(e.message,'terr');
  }
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Test Connectivity';
}

async function wfLoadSrcSchemas(){
  const sel=G('wfSrcSchemaSelect');if(!sel)return;
  const c=_wfSrcCreds();
  sel.innerHTML='<option value="">Loading…</option>';
  try{
    const r=await fetch('/api/v1/discovery/schemas',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source_config:c})});
    const d=await r.json();
    if(d.success&&d.schemas){
      sel.innerHTML='<option value="">All Schemas ('+d.schemas.length+')</option>'+d.schemas.map(s=>'<option value="'+s+'">'+s+'</option>').join('');
      toast('Loaded '+d.schemas.length+' schemas','tok');
    }else{sel.innerHTML='<option value="">All Schemas</option>';toast(d.error||'Failed','terr');}
  }catch(e){sel.innerHTML='<option value="">All Schemas</option>';toast(e.message,'terr');}
}
async function wfFetchTables(){
  const c=_wfSrcCreds();
  const _isSf=c.source_type==='snowflake';
  const _isNonSqlFt=_NON_SQL_SRC(c.source_type);
  if(_isSf){if(!c.account||!c.username){toast('Enter account and username','terr');return;}}
  else if(_isNonSqlFt){
    if(!c.server){toast('Enter the source URL','terr');return;}
    if(c.source_type==='sharepoint'&&(!c.tenant_id||!c.username)){toast('Enter Tenant ID and Client ID for SharePoint','terr');return;}
  }
  else{if(!c.server||!c.database||!c.username){toast('Enter server, database and username','terr');return;}}
  const schemaFilter=(G('wfSrcSchemaSelect')||{}).value||'';
  if(schemaFilter) c.schema_filter=schemaFilter;
  const btn=G('btnWfFetch');btn.disabled=true;btn.textContent='Discovering…';
  const st=G('wfSrcStatus');
  try{
    const r=await fetch('/api/v1/workflow/list-tables',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(c)});
    const d=await r.json();
    if(!r.ok||d.error)throw new Error(d.error||'Failed');
    WF_SRC_TABLES=d.tables||[];
    st.innerHTML='<span style="color:var(--green);">🟢 Connected — '+WF_SRC_TABLES.length+' tables</span>';
    G('wfSrcMsg').innerHTML='<span style="color:var(--green);">✓ '+WF_SRC_TABLES.length+' tables ready for pipeline creation</span>';
    toast('Discovered '+WF_SRC_TABLES.length+' tables from source.','tok');
    // Auto-populate inline table picker
    _wfQFiltered=[...WF_SRC_TABLES];
    _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
    _wfPopulateSchemaFilter();
    _wfSaveSourcesToDatabricks();
  }catch(e){
    st.innerHTML='<span style="color:var(--red);">🔴 Failed</span>';
    G('wfSrcMsg').innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
    toast(e.message,'terr');
  }
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg> Discover Tables';
}

/* ─── Quick Create Table Picker (Multi-Table) ─── */
let _wfQSelected = [];  // array of selected table objects

/* ─── Target Schema Mapping Cache ─── */
let _wfCatalogSchemas = [];  // [{catalog:'bronze', schemas:['hr','default']}, ...]
let _wfTargetSchemaList = [];  // unique schema names: ['hr','default','dbo']
let _wfCatSchemaLoaded = false;
async function _wfLoadCatalogSchemas() {
  if (_wfCatSchemaLoaded) return;
  try {
    const r = await fetch('/api/v1/uc/catalog-schemas');
    const d = await r.json();
    if (d.success && d.catalogs) {
      _wfCatalogSchemas = d.catalogs;
      // Collect unique schema names across all catalogs
      const schSet = new Set();
      _wfCatalogSchemas.forEach(c => c.schemas.forEach(s => schSet.add(s)));
      // Always include 'dbo' as a common source schema mapping option
      schSet.add('dbo');
      _wfTargetSchemaList = [...schSet].sort();
      _wfCatSchemaLoaded = true;
      // Populate default target and bulk target dropdowns
      ['wfQDefaultTarget', 'wfQBulkTarget'].forEach(id => {
        const sel = G(id);
        if (sel) {
          const first = id === 'wfQBulkTarget' ? '<option value="">Schema…</option>' : '<option value="">— schema —</option>';
          sel.innerHTML = first;
          _wfTargetSchemaList.forEach(s => {
            sel.innerHTML += `<option value="${s}">${s}</option>`;
          });
        }
      });
    }
  } catch (e) { console.warn('Could not load catalog schemas:', e); }
}
function _wfTargetOptions(selectedVal) {
  let html = '<option value="">— schema —</option>';
  _wfTargetSchemaList.forEach(s => {
    html += `<option value="${s}"${s === selectedVal ? ' selected' : ''}>${s}</option>`;
  });
  return html;
}
function wfQUpdateItemTarget(idx, val) {
  if (_wfQSelected[idx]) _wfQSelected[idx]._target = val;
}
function wfQApplyDefaultTarget(val) {
  // Override ALL selected tables when Default Schema dropdown changes
  _wfQSelected.forEach(t => { if(val) t._target = val; });
  _wfQRenderSelected();
}
function wfQApplyDefaultLayer(val) {
  // Override ALL selected tables when Default Layer dropdown changes
  _wfQSelected.forEach(t => { if(val) t._layer = val; });
  _wfQRenderSelected();
}
window.wfQApplyDefaultLayer=wfQApplyDefaultLayer;
function wfQBulkApplyTarget() {
  const val = G('wfQBulkTarget')?.value || '';

  // If no items checked, apply to ALL selected tables
  if(_wfQChecked.size === 0){
    _wfQSelected.forEach(t => { if(val) t._target = val; });
  } else {
    _wfQChecked.forEach(i => { if (_wfQSelected[i] && val) _wfQSelected[i]._target = val; });
  }
  _wfQRenderSelected();
}

let _wfQPage=0;
const _wfQPageSize=10;
function _wfGetConfiguredTableNames(){
  const names=new Set();
  (_wfPipelineData||[]).forEach(g=>{if(g.full_table)names.add(g.full_table);});
  return names;
}
function _renderTableItems(container, tables, onSelectFn){
  if(!tables.length){container.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;"><div style="font-size:20px;margin-bottom:4px;">📋</div>No tables found</div>';return;}
  const selNames = new Set(_wfQSelected.map(t=>t.full_name));
  const configuredNames = _wfGetConfiguredTableNames();
  const ac=G('wfQAvailCount'); if(ac) ac.textContent=tables.length+' tables';
  const totalPages=Math.ceil(tables.length/_wfQPageSize);
  if(_wfQPage>=totalPages) _wfQPage=Math.max(0,totalPages-1);
  const start=_wfQPage*_wfQPageSize;
  const pageItems=tables.slice(start,start+_wfQPageSize);
  let html=pageItems.map((t,pi)=>{
    const i=start+pi;
    const isSel = selNames.has(t.full_name);
    const isConfigured = configuredNames.has(t.full_name);
    const schema = t.full_name.includes('.') ? t.full_name.split('.')[0]+'.' : '';
    const tName = t.table || t.full_name.split('.').pop();
    return `<div class="wf-tbl-item" style="padding:7px 12px;cursor:pointer;font-size:12px;border-bottom:1px solid var(--border);transition:all .15s;display:flex;align-items:center;gap:8px;${isSel?'background:rgba(37,99,235,.06);':''}" onmouseover="this.style.background='${isSel?'rgba(37,99,235,.1)':'var(--surface-2)'}'" onmouseout="this.style.background='${isSel?'rgba(37,99,235,.06)':''}'" onclick="${onSelectFn}(${i})">
      <div style="width:18px;height:18px;display:flex;align-items:center;justify-content:center;border-radius:4px;border:2px solid ${isSel?'var(--blue)':'var(--border)'};background:${isSel?'var(--blue)':'transparent'};transition:all .15s;flex-shrink:0;">
        ${isSel?'<svg viewBox="0 0 24 24" style="width:12px;height:12px;stroke:#fff;stroke-width:3;fill:none;"><polyline points="20 6 9 17 4 12"/></svg>':''}
      </div>
      <div style="flex:1;min-width:0;">
        <div style="font-weight:600;color:var(--t1);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:11.5px;"><span style="color:var(--t4);font-weight:400;">${schema}</span>${tName}</div>
        <div style="font-size:10px;color:var(--t4);margin-top:1px;">~${Number(t.row_estimate||0).toLocaleString()} rows · ${t.col_count||'?'} cols</div>
      </div>
      ${isConfigured?'<span style="font-size:9px;padding:2px 7px;border-radius:4px;background:#dcfce7;color:#16a34a;font-weight:600;white-space:nowrap;">✓ Configured</span>':''}
      ${isSel&&!isConfigured?'<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:var(--blue);color:#fff;font-weight:600;">✓</span>':''}
    </div>`;
  }).join('');
  // Pagination controls
  if(totalPages>1){
    html+=`<div style="display:flex;align-items:center;justify-content:space-between;padding:6px 12px;border-top:1px solid var(--border);background:var(--surface-2);user-select:none;">
      <button onclick="_wfQPagePrev()" ${_wfQPage===0?'disabled':''} style="font-size:11px;padding:3px 10px;border:1px solid var(--border);border-radius:4px;background:${_wfQPage===0?'var(--surface)':'#fff'};color:${_wfQPage===0?'var(--t4)':'var(--t1)'};cursor:${_wfQPage===0?'default':'pointer'};font-weight:500;">← Prev</button>
      <span style="font-size:10px;color:var(--t3);font-weight:500;">Page ${_wfQPage+1} of ${totalPages}</span>
      <button onclick="_wfQPageNext()" ${_wfQPage>=totalPages-1?'disabled':''} style="font-size:11px;padding:3px 10px;border:1px solid var(--border);border-radius:4px;background:${_wfQPage>=totalPages-1?'var(--surface)':'#fff'};color:${_wfQPage>=totalPages-1?'var(--t4)':'var(--t1)'};cursor:${_wfQPage>=totalPages-1?'default':'pointer'};font-weight:500;">Next →</button>
    </div>`;
  }
  container.innerHTML=html;
}
function _wfQPagePrev(){if(_wfQPage>0){_wfQPage--;_renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');}}
function _wfQPageNext(){const tp=Math.ceil(_wfQFiltered.length/_wfQPageSize);if(_wfQPage<tp-1){_wfQPage++;_renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');}}

let _wfQChecked=new Set();
function _wfQRenderSelected(){
  const el=G('wfQSelectedList');
  const badge=G('wfQSelCount');
  const summary=G('wfQSelSummary');
  const selAllChk=G('wfQSelAllChk');
  // Clean up checked set — remove indices that no longer exist
  _wfQChecked=new Set([..._wfQChecked].filter(i=>i<_wfQSelected.length));
  _wfQUpdateBulkBar();
  if(!_wfQSelected.length){
    el.innerHTML='<div style="padding:30px 16px;text-align:center;color:var(--t4);font-size:11px;"><div style="font-size:24px;margin-bottom:6px;">📂</div>No tables selected<br><span style="font-size:10px;">Click tables on the left to add them</span></div>';
    badge.style.display='none';
    if(summary) summary.textContent='';
    if(selAllChk){selAllChk.checked=false;selAllChk.indeterminate=false;}
    return;
  }
  badge.style.display='';
  badge.textContent=_wfQSelected.length+' selected';
  if(summary) summary.textContent=_wfQSelected.length+' table'+(_wfQSelected.length>1?'s':'');
  // Update Select All checkbox state
  if(selAllChk){
    selAllChk.checked=_wfQChecked.size===_wfQSelected.length&&_wfQSelected.length>0;
    selAllChk.indeterminate=_wfQChecked.size>0&&_wfQChecked.size<_wfQSelected.length;
  }
  el.innerHTML=_wfQSelected.map((t,i)=>{
    const tbl=t.table||t.full_name.split('.').pop();
    const schema = t.full_name.includes('.') ? t.full_name.split('.')[0] : '';
    const chk=_wfQChecked.has(i);
    const tgtVal = t._target || '';
    const tgtLabel = tgtVal ? tgtVal.split('.').map(p=>'<span style="color:#7c3aed;font-weight:600;">'+p+'</span>').join('<span style="color:var(--t4);">.</span>') : '';
    return `<div data-tbl-name="${t.full_name}" style="display:flex;align-items:flex-start;gap:6px;padding:7px 10px;border-bottom:1px solid var(--border);font-size:11px;transition:background .12s;${chk?'background:#eff6ff;':''}" onmouseover="if(!${chk})this.style.background='var(--surface-2)'" onmouseout="if(!${chk})this.style.background=''">
      <input type="checkbox" class="wfQRowChk" data-idx="${i}" ${chk?'checked':''} onchange="wfQToggleRowChk(${i},this.checked)" style="width:14px;height:14px;accent-color:#3b82f6;cursor:pointer;flex-shrink:0;margin-top:3px;">
      <div style="width:22px;height:22px;border-radius:5px;background:linear-gradient(135deg,#3b82f6,#2563eb);display:flex;align-items:center;justify-content:center;color:#fff;font-size:9px;font-weight:700;flex-shrink:0;">${i+1}</div>
      <div style="flex:1;min-width:0;">
        <div style="display:flex;align-items:center;gap:6px;">
          <span style="font-weight:600;color:var(--t1);font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:120px;" title="${t.full_name}">${tbl}</span>
          <span style="font-size:9px;color:var(--t4);">${schema?schema+' · ':''}~${Number(t.row_estimate||0).toLocaleString()} rows</span>
        </div>
        <div style="display:flex;align-items:center;gap:4px;margin-top:3px;">
          <span style="font-size:8px;color:var(--t4);white-space:nowrap;">→</span>
          <select class="inp wfQItemTarget" data-idx="${i}" style="font-size:9px;padding:2px 4px;max-width:100px;border-radius:4px;color:#7c3aed;font-weight:600;border-color:#c4b5fd;" onchange="wfQUpdateItemTarget(${i},this.value)" title="Target schema on Databricks">
            ${_wfTargetOptions(tgtVal)}
          </select>

        </div>
      </div>
      <select class="inp wfQItemLoad" data-idx="${i}" style="font-size:9px;padding:2px 4px;max-width:70px;border-radius:4px;" onchange="wfQUpdateItemLoad(${i},this.value)">
        <option value="full"${(t._loadType||'full')==='full'?' selected':''}>Full</option>
        <option value="incremental"${t._loadType==='incremental'?' selected':''}>Incr</option>
      </select>
      <input class="inp wfQItemWm" data-idx="${i}" placeholder="WM col" style="font-size:9px;padding:2px 4px;width:60px;border-radius:4px;display:${t._loadType==='incremental'?'block':'none'};" value="${t._wmCol||''}" oninput="wfQUpdateItemWm(${i},this.value)">
      <button onclick="wfQRemoveItem(${i})" style="background:none;border:none;color:var(--t4);cursor:pointer;font-size:14px;padding:0 2px;flex-shrink:0;transition:color .15s;" onmouseover="this.style.color='var(--red)'" onmouseout="this.style.color='var(--t4)'" title="Remove">×</button>
    </div>`;
  }).join('');
}
function wfQToggleRowChk(idx,checked){
  if(checked) _wfQChecked.add(idx); else _wfQChecked.delete(idx);
  const selAllChk=G('wfQSelAllChk');
  if(selAllChk){
    selAllChk.checked=_wfQChecked.size===_wfQSelected.length&&_wfQSelected.length>0;
    selAllChk.indeterminate=_wfQChecked.size>0&&_wfQChecked.size<_wfQSelected.length;
  }
  _wfQUpdateBulkBar();
  // Update row highlight without full re-render
  const row=document.querySelectorAll('.wfQRowChk[data-idx="'+idx+'"]')[0];
  if(row&&row.closest('div[style]'))row.closest('div[style*="border-bottom"]').style.background=checked?'#eff6ff':'';
}
function wfQToggleSelectAll(checked){
  _wfQChecked.clear();
  if(checked) _wfQSelected.forEach((_,i)=>_wfQChecked.add(i));
  _wfQRenderSelected();
}
function _wfQUpdateBulkBar(){
  const bar=G('wfQBulkBar');
  const cnt=G('wfQBulkCount');
  if(!bar)return;
  if(_wfQChecked.size>0){
    bar.style.display='flex';
    cnt.textContent=_wfQChecked.size+' checked';
  } else {
    bar.style.display='none';
  }
  // Update Create button label with checked count
  const btnLbl=G('btnWfQuickLabel');
  if(btnLbl){
    const isDlt=((G('wfNbPipelineMode')||{}).value||'standard')==='dlt';
    const n=_wfQChecked.size;
    if(n>0) btnLbl.textContent=(isDlt?'Create 2-Stage Declarative Pipeline':'Create 3-Stage Medallion Pipeline')+' ('+n+' table'+(n>1?'s':'')+')';
    else btnLbl.textContent=isDlt?'Create 2-Stage Declarative Pipeline':'Create 3-Stage Medallion Pipeline';
  }
}
function wfQBulkRemove(){
  const indices=[..._wfQChecked].sort((a,b)=>b-a);
  indices.forEach(i=>_wfQSelected.splice(i,1));
  _wfQChecked.clear();
  _wfQRenderSelected();
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}
function wfQBulkApplyLoad(){
  const loadType=G('wfQBulkLoadType')?.value||'full';
  _wfQChecked.forEach(i=>{
    if(_wfQSelected[i]) _wfQSelected[i]._loadType=loadType;
  });
  _wfQRenderSelected();
}

function wfQUpdateItemLoad(idx,val){
  if(_wfQSelected[idx]){
    _wfQSelected[idx]._loadType=val;
    _wfQRenderSelected();
  }
}
function wfQUpdateItemWm(idx,val){
  if(_wfQSelected[idx]) _wfQSelected[idx]._wmCol=val;
}
function wfQRemoveItem(idx){
  _wfQSelected.splice(idx,1);
  _wfQRenderSelected();
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}
function wfQSelectAll(){
  if(!WF_SRC_TABLES.length){toast('Discover tables first','terr');return;}
  const selNames=new Set(_wfQSelected.map(t=>t.full_name));
  const defaultLoad=G('wfQLoadType').value;
  WF_SRC_TABLES.forEach(t=>{
    if(!selNames.has(t.full_name)){
      _wfQSelected.push({...t, _loadType:defaultLoad, _wmCol:''});
      _wfQChecked.add(_wfQSelected.length-1);  // auto-check on add
    }
  });
  _wfQRenderSelected();
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}
function wfQClearAll(){
  _wfQSelected=[];
  _wfQChecked.clear();
  _wfQRenderSelected();
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}

let _wfQFiltered=[];
function wfShowQDropdown(){
  if(!WF_SRC_TABLES.length) return;
  if(!_wfQFiltered.length) _wfQFiltered=[...WF_SRC_TABLES];
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}
function wfFilterQTables(val){
  if(!WF_SRC_TABLES.length){
    G('wfQTableDropdown').innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;"><div style="font-size:20px;margin-bottom:4px;">⚠</div>Click "Discover Tables" above first</div>';
    return;
  }
  const q=(val||'').toLowerCase();
  const schemaFilter=(G('wfQSchemaFilter')||{}).value||'';
  _wfQFiltered=WF_SRC_TABLES.filter(t=>{
    if(q && !t.full_name.toLowerCase().includes(q)) return false;
    if(schemaFilter && (t.schema||t.full_name.split('.')[0])!==schemaFilter) return false;
    return true;
  });
  _wfQPage=0;  // reset to first page on filter change
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
  const info=G('wfQFilteredInfo');
  if(info) info.textContent=(q||schemaFilter)?_wfQFiltered.length+' of '+WF_SRC_TABLES.length:'';
}
/* Populate schema dropdown after table discovery */
function _wfPopulateSchemaFilter(){
  const sel=G('wfQSchemaFilter');
  if(!sel) return;
  const schemas=new Set(WF_SRC_TABLES.map(t=>t.schema||t.full_name.split('.')[0]).filter(Boolean));
  sel.innerHTML='<option value="">All Schemas ('+schemas.size+')</option>'+
    [...schemas].sort().map(s=>'<option value="'+s+'">'+s+'</option>').join('');
}
/* Filter selected table list */
function wfFilterSelectedTables(val){
  const q=(val||'').toLowerCase();
  const rows=G('wfQSelectedList')?.querySelectorAll('[data-tbl-name]');
  if(!rows) return;
  rows.forEach(r=>{
    const name=(r.dataset.tblName||'').toLowerCase();
    r.style.display=(!q||name.includes(q))?'':'none';
  });
}
function wfSelectQTable(idx){
  const t=_wfQFiltered[idx];if(!t)return;
  // Toggle selection
  const existIdx=_wfQSelected.findIndex(s=>s.full_name===t.full_name);
  if(existIdx>=0){
    _wfQChecked.delete(existIdx);
    _wfQSelected.splice(existIdx,1);
    // Re-index checked set after splice
    const updated=new Set();
    _wfQChecked.forEach(i=>{if(i>existIdx)updated.add(i-1);else updated.add(i);});
    _wfQChecked=updated;
  }else{
    const defaultLoad=G('wfQLoadType').value;
    _wfQSelected.push({...t, _loadType:defaultLoad, _wmCol:''});
    _wfQChecked.add(_wfQSelected.length-1);  // auto-check on add
  }
  _wfQRenderSelected();
  // Re-render available list to update checkmarks
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
  // Keep dropdown open for multi-select
}
function wfClearQSelection(){
  wfQClearAll();
}

/* ─── Job Workflow Table Picker (Sub-tab 2) ─── */
let _wfJFiltered=[];
function wfShowJobDropdown(){
  if(!WF_SRC_TABLES.length){
    const dd=G('wfTableDropdown');
    dd.innerHTML='<div style="padding:12px;text-align:center;color:var(--t4);font-size:11px;">⚠ Connect data source in Medallion Architecture tab first</div>';
    dd.style.display='block';
    return;
  }
  _wfJFiltered=[...WF_SRC_TABLES];
  const dd=G('wfTableDropdown');
  _renderTableItems(dd,_wfJFiltered,'wfSelectJobTable');
  dd.style.display='block';
}
function wfFilterJobTables(val){
  if(!WF_SRC_TABLES.length)return;
  const q=val.toLowerCase();
  _wfJFiltered=q?WF_SRC_TABLES.filter(t=>t.full_name.toLowerCase().includes(q)):WF_SRC_TABLES;
  const dd=G('wfTableDropdown');
  _renderTableItems(dd,_wfJFiltered,'wfSelectJobTable');
  dd.style.display='block';
}
function wfSelectJobTable(idx){
  const t=_wfJFiltered[idx];if(!t)return;
  _wfSelectedJ=t;
  G('wfTableSearch').value='';
  G('wfTableDropdown').style.display='none';
  G('wfSelectedTable').style.display='block';
  G('wfSelName').textContent=t.full_name;
  G('wfSelMeta').textContent='~'+Number(t.row_estimate||0).toLocaleString()+' rows · '+(t.col_count||'?')+' cols';
  // Update preview
  const n=t.table||t.full_name.split('.').pop();
  const prev=document.getElementById('wfPreviewName');
  if(prev)prev.textContent=n;
  document.querySelectorAll('.wfPrevTbl').forEach(e=>e.textContent=n);
}
function wfClearJobSelection(){
  _wfSelectedJ=null;
  G('wfSelectedTable').style.display='none';
  G('wfTableSearch').value='';
  const prev=document.getElementById('wfPreviewName');
  if(prev)prev.textContent='TableName';
  document.querySelectorAll('.wfPrevTbl').forEach(e=>e.textContent='TableName');
}

// Close dropdowns when clicking outside (job table picker only — Quick Create stays open)
document.addEventListener('click',function(e){
  if(!e.target.closest('#wfTableSearch')&&!e.target.closest('#wfTableDropdown')){
    const dd=G('wfTableDropdown');if(dd)dd.style.display='none';
  }
});

async function wfCreatePipeline(){
  if(!_wfSelectedJ){toast('Select a table from the dropdown — connect data source first','terr');return;}
  /* ── Cluster gate ── */
  const _sel=G('wfClusterSelect');
  if(!_sel||!_sel.value){toast('Please select a Databricks cluster first','terr');return;}
  const _cOpt=_sel.options[_sel.selectedIndex];
  if(_cOpt&&_cOpt.dataset.state!=='RUNNING'){
    toast('Cluster is '+(_cOpt.dataset.state||'not running')+' — please start the cluster before creating a pipeline','terr');return;
  }
  const schema=_wfSelectedJ.schema||'dbo';
  const table=_wfSelectedJ.table||_wfSelectedJ.full_name.split('.').pop();
  const loadType=G('wfLoadType').value;
  const wmCol=loadType==='incremental'?G('wfWatermarkCol').value.trim():'';
  if(loadType==='incremental'&&!wmCol){toast('Enter watermark column for incremental load','terr');return;}
  const btn=G('btnWfCreate');btn.disabled=true;btn.textContent='Creating…';
  try{
    const r=await fetch('/api/v1/workflow/create-pipeline',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({table_schema:schema,table_name:table,load_type:loadType,watermark_column:wmCol,source_config:_wfSourceConfig(),target_config:_wfTargetConfig(),pipeline_mode:(G('wfNbPipelineMode')||{}).value||'standard',cdc_mode:(G('cfgCdcMode')||{}).value||'watermark',primary_keys:(G('cfgPrimaryKeys')||{}).value?G('cfgPrimaryKeys').value.split(',').map(s=>s.trim()).filter(Boolean):[]})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast('Pipeline created: '+d.jobs.length+' jobs for '+table,'tok');
    wfClearJobSelection();
    G('wfLoadType').value='full';wfToggleWatermark();
    wfRefreshAll();
  }catch(e){toast(e.message,'terr');}
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg> Create 3-Stage Pipeline';
}

/* ─── Quick Create from Medallion Tab ─── */
function wfUpdateQuickPreview(){
  // no-op — multi-table mode handles preview via _wfQRenderSelected()
}
function wfQToggleWm(){
  // no-op — per-table load type is now inline in the selected list
}
async function wfQuickCreate(){
  if(!_wfQSelected.length){toast('Select one or more tables from the dropdown','terr');return;}
  // Only migrate tables whose checkboxes are checked
  const checkedTables=_wfQChecked.size>0
    ? [..._wfQChecked].sort((a,b)=>a-b).map(i=>_wfQSelected[i]).filter(Boolean)
    : [];
  if(!checkedTables.length){toast('Check the tables you want to migrate using the checkboxes','terr');return;}
  /* ── Cluster gate ── */
  const _sel=G('wfClusterSelect');
  if(!_sel||!_sel.value){toast('Please select a Databricks cluster first','terr');return;}
  const _cOpt=_sel.options[_sel.selectedIndex];
  if(_cOpt&&_cOpt.dataset.state!=='RUNNING'){
    toast('Cluster is '+(_cOpt.dataset.state||'not running')+' — please start the cluster before creating a pipeline','terr');return;
  }
  // Validate incremental tables have watermark columns (skip for Change Tracking CDC — uses SYS_CHANGE_VERSION)
  const _cdcMode=(G('cfgCdcMode')||{}).value||'watermark';
  for(const t of checkedTables){
    if(t._loadType==='incremental'&&_cdcMode!=='change_tracking'&&!(t._wmCol||'').trim()){
      toast('Enter watermark column for '+t.full_name+' (incremental)','terr');return;
    }
  }
  const btn=G('btnWfQuick');btn.disabled=true;btn.textContent='Creating '+checkedTables.length+' pipeline(s)…';
  try{
    const tables=checkedTables.map(t=>{
      const obj = {
        schema: t.schema||'dbo',
        table: t.table||t.full_name.split('.').pop(),
        load_type: t._loadType||G('wfQLoadType').value,
        watermark_column: t._wmCol||'',
      };
      // Per-table target schema mapping — use per-table _target first,
      // then fall back to Default Schema dropdown value
      const _defaultTgt=G('wfQDefaultTarget')?.value?.trim()||'';
      if(t._target){
        obj.target_schema=t._target;
      } else if(_defaultTgt){
        obj.target_schema=_defaultTgt;
      }
      // Per-table layer mapping — for ExistingSetting config routing
      const _defaultLayer=G('wfQDefaultLayer')?.value?.trim()||'';
      if(t._layer){
        obj.target_layer=t._layer;
      } else if(_defaultLayer){
        obj.target_layer=_defaultLayer;
      }
      return obj;
    });
    const r=await fetch('/api/v1/workflow/create-pipelines-bulk',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({tables:tables,source_config:_wfSourceConfig(),target_config:_wfTargetConfig(),pipeline_mode:(G('wfNbPipelineMode')||{}).value||'standard',cdc_mode:(G('cfgCdcMode')||{}).value||'watermark',primary_keys:(G('cfgPrimaryKeys')||{}).value?G('cfgPrimaryKeys').value.split(',').map(s=>s.trim()).filter(Boolean):[],use_layer_mapping:!!(G('wfQUseLayerMapping')&&G('wfQUseLayerMapping').checked)})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    const archCount=(d.groups||[]).reduce((s,g)=>(s+(g.archived_jobs||[]).length),0);
    let msg='Created '+d.created+' pipeline(s) with '+d.total_jobs+' jobs';
    if(archCount>0) msg+=' ('+archCount+' old job(s) archived to history)';
    toast(msg,'tok');
    wfQClearAll();
    G('wfQLoadType').value='full';
    wfRefreshAll();
  }catch(e){toast(e.message,'terr');}
  btn.disabled=false;const _isDlt=((G('wfNbPipelineMode')||{}).value||'standard')==='dlt';btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg> <span id="btnWfQuickLabel">'+(_isDlt?'Create 2-Stage Declarative Pipeline':'Create 3-Stage Medallion Pipeline')+'</span>';
}

/* ─── Layer Detail Panel ─── */
let _openLayer=null;
function wfToggleLayerDetail(layer){
  const panel=G('mdlDetailPanel');
  if(_openLayer===layer){wfCloseLayerDetail();return;}
  _openLayer=layer;
  const titles={source:'SQL Source — Extract Jobs',landing:'Landing Zone — Ingested Data',bronze:'Bronze Layer — Raw Delta Tables',silver:'Silver Layer — Cleansed Tables'};
  const stages={source:'extract',landing:'landing_to_bronze',bronze:'landing_to_bronze',silver:'bronze_to_silver'};
  G('mdlDetailTitle').textContent=titles[layer]||'Layer Details';
  // Highlight selected layer
  document.querySelectorAll('.mdl-layer').forEach(el=>el.style.outline='none');
  const layerEl=G('mdlLayer'+layer.charAt(0).toUpperCase()+layer.slice(1));
  if(layerEl)layerEl.style.outline='2px solid rgba(255,255,255,.5)';
  // Load jobs for this stage
  wfLoadLayerJobs(layer,stages[layer]);
  panel.classList.add('open');
}
function wfCloseLayerDetail(){
  _openLayer=null;
  G('mdlDetailPanel').classList.remove('open');
  document.querySelectorAll('.mdl-layer').forEach(el=>el.style.outline='none');
}
async function wfLoadLayerJobs(layer,stage){
  const content=G('mdlDetailContent');
  content.innerHTML='<div style="text-align:center;color:var(--t4);padding:12px;">Loading jobs…</div>';
  try{
    const r=await fetch('/api/v1/workflow/jobs?stage='+encodeURIComponent(stage));
    const d=await r.json();
    if(!d.success||!d.jobs.length){
      content.innerHTML='<div style="text-align:center;color:var(--t4);padding:12px;">No jobs at this stage yet. Create a pipeline using Quick Create.</div>';
      return;
    }
    const jc={created:'#94a3b8',running:'#3b82f6',success:'#10b981',failed:'#ef4444'};
    const ji={created:'⏸',running:'🔄',success:'✅',failed:'❌'};
    content.innerHTML=d.jobs.map(j=>
      `<div class="mdl-detail-job">
        <div style="display:flex;align-items:center;gap:8px;">
          <span style="font-size:12px;">${ji[j.status]||'⏸'}</span>
          <div>
            <div style="font-weight:600;color:var(--t1);">${j.job_name}</div>
            <div style="font-size:10px;color:var(--t4);">${j.load_type.toUpperCase()} · ${j.run_count||0} runs</div>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:6px;">
          <span style="font-size:10px;font-weight:600;color:${jc[j.status]||'#94a3b8'};">${j.status.toUpperCase()}</span>
          <button class="btn btn-primary btn-xs" onclick="wfRunSingleJob('${j.job_id}')" title="Run">▶</button>
        </div>
      </div>`
    ).join('');
  }catch(e){
    content.innerHTML='<div style="text-align:center;color:var(--red);padding:12px;">Error: '+e.message+'</div>';
  }
}

async function wfRefreshAll(){
  wfRefreshStats();
  wfRefreshPipelines();
  wfRefreshJobs();
  wfRefreshHistory();
  wfRefreshAuditHistory();
  wfRefreshWatermarks();
  if(!_wfClustersLoaded)wfFetchClusters();
  if(!_wfCatSchemaLoaded) _wfLoadCatalogSchemas();
  // Re-render open layer detail if any
  if(_openLayer){
    const stages={source:'extract',landing:'landing_to_bronze',bronze:'landing_to_bronze',silver:'bronze_to_silver'};
    wfLoadLayerJobs(_openLayer,stages[_openLayer]);
  }
}

async function wfRefreshStats(){
  try{
    const r=await fetch('/api/v1/workflow/stats');const d=await r.json();
    let s=d.success?d.stats:null;

    // If local stats are all zeros, fallback to Databricks metadata
    if(!s || (s.total_jobs===0 && s.total_pipelines===0)){
      try{
        const dbxR=await fetch('/api/v1/reports/jobs').then(r=>r.json());
        if(dbxR.success && dbxR.jobs && dbxR.jobs.length){
          const jobs=dbxR.jobs;
          const stages=new Set(jobs.map(j=>j.stage).filter(Boolean));
          const successJobs=jobs.filter(j=>(j.status||'').toLowerCase()==='success').length;
          const failedJobs=jobs.filter(j=>(j.status||'').toLowerCase()==='failed').length;
          const runningJobs=jobs.filter(j=>(j.status||'').toLowerCase()==='running').length;
          const totalRows=jobs.reduce((sum,j)=>sum+(parseInt(j.rows_processed)||0),0);
          const tablesMigrated=new Set(jobs.filter(j=>(j.status||'').toLowerCase()==='success'&&(j.stage||'').includes('silver')).map(j=>j.full_table||j.table_name)).size;
          s={
            total_pipelines: stages.size || Math.ceil(jobs.length/2),
            total_jobs: jobs.length,
            success_jobs: successJobs,
            failed_jobs: failedJobs,
            running_jobs: runningJobs,
            total_rows_processed: totalRows,
            tables_migrated: tablesMigrated,
            extract_jobs: jobs.filter(j=>(j.stage||'').includes('extract')).length,
            ingest_jobs: jobs.filter(j=>(j.stage||'').includes('dlt')||((j.stage||'').includes('bronze'))).length,
            cleanse_jobs: jobs.filter(j=>(j.stage||'').includes('silver')).length
          };
        }
      }catch(dbxErr){console.warn('Dashboard Databricks fallback failed',dbxErr);}
    }
    if(!s) return;

    G('wfStatPipelines').textContent=s.total_pipelines;
    G('wfStatJobs').textContent=s.total_jobs;
    G('wfStatSuccess').textContent=s.success_jobs;
    G('wfStatFailed').textContent=s.failed_jobs;
    const rowsEl=G('wfStatRows');
    if(rowsEl)rowsEl.textContent=(s.tables_migrated!=null?s.tables_migrated:s.total_rows_processed)||0;
    const rowsSubEl=G('wfStatRowsSub');
    if(rowsSubEl)rowsSubEl.textContent=s.tables_migrated!=null?'Fully reached Silver':'Total data volume';
    const b=G('navBadgeWf');if(b)b.textContent=s.total_jobs;
    // Update medallion layer counts
    const sc=G('mdlSrcCount');if(sc)sc.textContent=s.total_pipelines+' tables';
    const lc=G('mdlLandingCount');if(lc)lc.textContent=s.total_pipelines+' tables';
    const bc=G('mdlBronzeCount');if(bc)bc.textContent=s.total_pipelines+' tables';
    const slc=G('mdlSilverCount');if(slc)slc.textContent=s.total_pipelines+' tables';
    // Toggle has-data class on layers
    ['Source','Landing','Bronze','Silver'].forEach(layer=>{
      const el=G('mdlLayer'+layer);
      if(el){ if(s.total_pipelines>0) el.classList.add('has-data'); else el.classList.remove('has-data'); }
    });
    // Update arrow job counts
    const a1=G('mdlArrow1Count');if(a1)a1.textContent=(s.extract_jobs||s.total_pipelines)+' jobs';
    const a2=G('mdlArrow2Count');if(a2)a2.textContent=(s.ingest_jobs||s.total_pipelines)+' jobs';
    const a3=G('mdlArrow3Count');if(a3)a3.textContent=(s.cleanse_jobs||s.total_pipelines)+' jobs';
    // Animate arrows when there are running jobs
    ['mdlArrow1','mdlArrow2','mdlArrow3','mdlArrow4'].forEach(id=>{
      const el=G(id);
      if(el){ if(s.running_jobs>0) el.classList.add('active'); else el.classList.remove('active'); }
    });
  }catch(e){console.error('wfRefreshStats',e);}
}

/* ─── Fetch Databricks Clusters ─── */
let _wfClustersLoaded=false;
async function wfFetchClusters(){
  const c=await _wfDbrCredsWithFallback();
  if(!c.host||!c.token) return;  // silently skip — no credentials configured yet
  const sel=G('wfClusterSelect'), stat=G('wfClusterStatus'), info=G('wfClusterInfo');
  stat.textContent='Loading…';stat.style.color='var(--t4)';
  try{
    const r=await fetch(`/api/v1/workflow/clusters?host=${encodeURIComponent(c.host)}&token=${encodeURIComponent(c.token)}`);
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed to list clusters');
    const clusters=d.clusters||[];
    // Keep current selection if possible
    const prev=sel.value;
    sel.innerHTML='<option value="">— Select a cluster —</option>';
    let runCount=0;
    clusters.forEach(cl=>{
      const st=cl.state||'UNKNOWN';
      const isRun=st==='RUNNING';
      if(isRun)runCount++;
      const icon=isRun?'🟢':st==='TERMINATED'?'🔴':st==='PENDING'?'🟡':'⚪';
      const opt=document.createElement('option');
      opt.value=cl.cluster_id;
      opt.textContent=`${icon} ${cl.cluster_name}  (${st} · DBR ${cl.spark_version||'N/A'})`;
      opt.dataset.state=st;
      sel.appendChild(opt);
    });
    // Restore previous selection or auto-select if only one cluster
    if(prev){sel.value=prev;}
    else if(clusters.length===1){sel.value=clusters[0].cluster_id;}
    stat.textContent=`${clusters.length} cluster${clusters.length!==1?'s':''} found (${runCount} running)`;
    stat.style.color='var(--green)';
    _wfClustersLoaded=true;
    _updateClusterInfo();
  }catch(e){
    stat.textContent=e.message;stat.style.color='var(--red)';
    console.error('wfFetchClusters',e);
  }
}
function _updateClusterInfo(){
  const sel=G('wfClusterSelect'),info=G('wfClusterInfo');
  const opt=sel.options[sel.selectedIndex];
  if(opt&&opt.value){
    info.style.display='block';
    info.innerHTML=`<span style="font-weight:600;">ID:</span> <code style="font-size:9px;">${opt.value}</code>`;
  }else{info.style.display='none';}
  // Start button: never hide, only disable when running
  const btn=G('btnStartCluster');
  if(btn){
    if(opt&&opt.value&&opt.dataset.state==='RUNNING'){
      btn.disabled=true;btn.style.opacity='0.4';
      btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;fill:currentColor;"><polygon points="5 3 19 12 5 21 5 3"/></svg> Running';
    }else if(opt&&opt.value){
      btn.disabled=false;btn.style.opacity='1';
      btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;fill:currentColor;"><polygon points="5 3 19 12 5 21 5 3"/></svg> Start';
    }else{
      btn.disabled=true;btn.style.opacity='0.4';
      btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;fill:currentColor;"><polygon points="5 3 19 12 5 21 5 3"/></svg> Start';
    }
  }
}

async function wfStartCluster(){
  const sel=G('wfClusterSelect');
  if(!sel||!sel.value){toast('Select a cluster first','terr');return;}
  const opt=sel.options[sel.selectedIndex];
  if(opt&&opt.dataset.state==='RUNNING'){toast('Cluster is already running','tinfo');return;}
  const c=await _wfDbrCredsWithFallback();
  if(!c.host||!c.token){toast('Configure Databricks connection in Settings first','terr');return;}
  const btn=G('btnStartCluster');
  btn.disabled=true;btn.textContent='Starting…';
  try{
    const r=await fetch('/api/v1/workflow/clusters/start',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({host:c.host,token:c.token,cluster_id:sel.value})
    });
    const d=await r.json();
    if(!d.success)throw new Error(d.error||d.message||'Failed');
    toast('Cluster start initiated — may take 2–5 minutes','tok');
    // Poll for cluster status update
    let polls=0;
    const poller=setInterval(async()=>{
      polls++;
      await wfFetchClusters();
      const updated=sel.options[sel.selectedIndex];
      if(updated&&updated.dataset.state==='RUNNING'){
        clearInterval(poller);
        toast('Cluster is now RUNNING ✔','tok');
        btn.style.display='none';
      }
      if(polls>=40)clearInterval(poller); // stop after ~5 min
    },8000);
  }catch(e){toast(e.message,'terr');}
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;fill:currentColor;"><polygon points="5 3 19 12 5 21 5 3"/></svg> Start Cluster';
}
(function(){
  const sel=document.getElementById('wfClusterSelect');
  if(sel)sel.addEventListener('change',_updateClusterInfo);
})();

let _wfPipelineData=[];  // cached for filtering
async function wfRefreshPipelines(){
  try{
    const r=await fetch('/api/v1/workflow/pipelines');const d=await r.json();
    if(!d.success)return;
    _wfPipelineData=d.groups||[];
    _wfRenderFilteredPipelines();
  }catch(e){console.error('wfRefreshPipelines',e);}
}
function wfFilterPipelineGroups(val){
  _wfRenderFilteredPipelines();
}
function _wfRenderFilteredPipelines(){
    const el=G('wfPipelineList');
    const infoBar=G('wfPipelineListInfo');
    let groups=_wfPipelineData;
    // Apply text filter
    const q=((G('wfPipelineFilter')||{}).value||'').toLowerCase();
    const sf=((G('wfPipelineStatusFilter')||{}).value||'');
    if(q) groups=groups.filter(g=>(g.full_table||'').toLowerCase().includes(q)||(g.table_name||'').toLowerCase().includes(q));
    if(sf) groups=groups.filter(g=>g.status===sf);

    if(!groups.length){
      el.innerHTML='<div class="empty" style="padding:24px;"><div class="empty-ico">🔗</div><div class="empty-t">'+(q||sf?'No pipelines match filter':'No pipelines yet — use Quick Create to get started')+'</div></div>';
      if(infoBar)infoBar.style.display='none';
      const _toolbar=G('wfGroupToolbar');if(_toolbar)_toolbar.style.display='none';
      return;
    }
    // Show / hide toolbar depending on group count
    const _toolbar=G('wfGroupToolbar');
    if(_toolbar)_toolbar.style.display=groups.length?'flex':'none';
    // Prune stale selections
    const _gids=new Set(groups.map(g=>g.group_id));
    _wfSelectedGroups.forEach(id=>{if(!_gids.has(id))_wfSelectedGroups.delete(id);});
    _wfUpdateGroupToolbar(groups.length);

    /* Helper: format datetime for display */
    function _fmtDt(ts){
      if(!ts)return '';
      try{
        const d=new Date(ts);
        if(isNaN(d.getTime()))return '';
        const pad=n=>String(n).padStart(2,'0');
        return pad(d.getDate())+'/'+pad(d.getMonth()+1)+'/'+d.getFullYear()+' '+pad(d.getHours())+':'+pad(d.getMinutes());
      }catch(e){return '';}
    }

    el.innerHTML=groups.map(g=>{
      const _pm=g.pipeline_mode||(G('wfNbPipelineMode')||{}).value||'standard';
      const _pmIsDlt=_pm==='dlt';
      const _lastAct=_fmtDt(g.last_activity);
      const _pmBadge=_pmIsDlt
        ?'<span style="font-size:8px;padding:1px 5px;border-radius:6px;background:#f59e0b;color:#fff;font-weight:700;">⚡SDP</span>'
        :'<span style="font-size:8px;padding:1px 5px;border-radius:6px;background:#3b82f6;color:#fff;font-weight:700;">Spark</span>';
      const _chk=_wfSelectedGroups.has(g.group_id)?'checked':'';
      const _SL={extract:'Extract',landing_to_bronze:'→Bronze',bronze_to_silver:'→Silver',dlt_bronze_silver:'⚡SDP'};
      const _statusBg={created:'#f1f5f9',running:'#dbeafe',success:'#d1fae5',failed:'#fee2e2'};
      const _statusFg={created:'#64748b',running:'#2563eb',success:'#059669',failed:'#dc2626'};
      const _statusIco={created:'⏸',running:'🔄',success:'✅',failed:'❌'};
      /* Compact inline stage indicators */
      const stageIndicators=(g.jobs||[]).sort((a,b)=>a.order-b.order).map((j,i,arr)=>{
        const jc={created:'#94a3b8',running:'#3b82f6',success:'#10b981',failed:'#ef4444'};
        const c=jc[j.status]||'#94a3b8';
        const sl=_SL[j.stage]||j.stage;
        const sep=i<arr.length-1?' <span style="color:var(--t4);font-size:10px;">→</span> ':'';
        const _jt=_fmtDt(j.last_run_at);
        return '<span style="display:inline-flex;align-items:center;gap:3px;padding:1px 6px;border-radius:4px;background:'+c+'14;border:1px solid '+c+'33;font-size:9px;font-weight:600;color:'+c+';" title="'+j.job_name+(_jt?' | '+_jt:'')+'">'
          +(_statusIco[j.status]||'⏸')+' '+sl
          +(j.status==='failed'?' <button onclick="event.stopPropagation();wfRunSingleJob(\''+j.job_id+'\')" style="padding:0 3px;font-size:8px;background:none;border:none;color:'+c+';cursor:pointer;font-weight:700;" title="Rerun">↻</button>':'')
          +'</span>'+sep;
      }).join('');
      return `<div style="padding:8px 12px;border:1px solid var(--border);border-radius:var(--r-sm);margin-bottom:4px;background:var(--surface);transition:box-shadow .15s;" onmouseover="this.style.boxShadow='0 2px 8px rgba(0,0,0,.06)'" onmouseout="this.style.boxShadow=''">
        <div style="display:flex;align-items:center;gap:6px;">
          <input type="checkbox" class="wfGrpChk" data-gid="${g.group_id}" ${_chk} onchange="wfToggleGroupSelect('${g.group_id}',this.checked)" style="accent-color:var(--blue);width:14px;height:14px;cursor:pointer;flex-shrink:0;">
          <div style="flex:1;min-width:0;">
            <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
              <span style="font-weight:700;font-size:12px;color:var(--t1);">${g.full_table}</span>
              ${_pmBadge}
              <span style="display:inline-flex;align-items:center;gap:3px;padding:1px 6px;border-radius:99px;font-size:9px;font-weight:600;background:${_statusBg[g.status]||'#f1f5f9'};color:${_statusFg[g.status]||'#64748b'};">${_statusIco[g.status]||'⏸'} ${g.status.toUpperCase()}</span>
              ${_lastAct?'<span style="font-size:9px;color:var(--t4);">🕐 '+_lastAct+'</span>':''}
            </div>
            <div style="display:flex;align-items:center;gap:4px;margin-top:4px;flex-wrap:wrap;">
              <span style="font-size:9px;color:var(--t3);">${g.load_type.toUpperCase()} · ${g.job_ids.length} jobs${g.watermark_column?' · WM:'+g.watermark_column:''}</span>
              <span style="font-size:9px;color:var(--t4);">|</span>
              ${stageIndicators}
            </div>
          </div>
          <div style="display:flex;align-items:center;gap:4px;flex-shrink:0;">
            <button class="btn btn-primary btn-xs" onclick="wfRunOnDatabricks('${g.group_id}')" title="Run on Databricks" style="background:var(--accent-gradient);padding:3px 8px;font-size:10px;">⚡ Run</button>
            <button class="btn btn-ghost btn-xs" onclick="wfRerunPipeline('${g.group_id}')" title="Rerun from failure" style="padding:3px 6px;font-size:10px;">🔄</button>
            <button class="btn btn-ghost btn-xs" onclick="wfShowPipelineLogs('${g.group_id}','${g.full_table}')" title="Show logs" style="padding:3px 6px;font-size:10px;">📋</button>
            <button class="btn btn-ghost btn-xs" onclick="wfDeletePipeline('${g.group_id}')" title="Delete" style="color:var(--red);padding:3px 6px;font-size:10px;">✕</button>
          </div>
        </div>
      </div>`;
    }).join('');

    // Info bar
    if(infoBar){
      if(q||sf){
        infoBar.style.display='block';
        infoBar.textContent='Showing '+groups.length+' of '+_wfPipelineData.length+' pipeline(s)';
      }else{
        infoBar.style.display='block';
        infoBar.textContent=groups.length+' pipeline group(s)';
      }
    }
}

const WF_STAGE_LABELS={extract:'Extract',landing_to_bronze:'Landing→Bronze',bronze_to_silver:'Bronze→Silver',dlt_bronze_silver:'⚡ SDP Bronze+Silver'};
const WF_STATUS_BADGES={
  created:'<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:#f1f5f9;color:#64748b;">CREATED</span>',
  running:'<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:#dbeafe;color:#2563eb;">RUNNING</span>',
  success:'<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:#d1fae5;color:#059669;">SUCCESS</span>',
  failed:'<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:#fee2e2;color:#dc2626;">FAILED</span>',
};

/* ─── Job Multi-Select State ─── */
let _wfSelectedJobs=new Set();
let _wfAllJobs=[];

function wfToggleJobSelect(jobId, checked){
  if(checked) _wfSelectedJobs.add(jobId);
  else _wfSelectedJobs.delete(jobId);
  _wfUpdateJobToolbar();
}
function wfToggleAllJobs(checked){
  document.querySelectorAll('.wfJobChk').forEach(cb=>{
    cb.checked=checked;
    if(checked) _wfSelectedJobs.add(cb.dataset.jid);
    else _wfSelectedJobs.delete(cb.dataset.jid);
  });
  const allHead=G('wfJobSelectAllHead');
  if(allHead)allHead.checked=checked;
  const allTb=G('wfJobSelectAll');
  if(allTb)allTb.checked=checked;
  _wfUpdateJobToolbar();
}
function _wfUpdateJobToolbar(){
  const n=_wfSelectedJobs.size;
  const tb=G('wfJobToolbar');
  if(tb)tb.style.display=n>0?'flex':'none';
  const lbl=G('wfJobSelCount');
  if(lbl)lbl.textContent=n+' selected';
  const runBtn=G('btnWfRunSelectedJobs');
  if(runBtn)runBtn.style.display=n>0?'inline-flex':'none';
  // Show "Rerun Failed" only if any selected job has failed status
  const rerunBtn=G('btnWfRerunFailedJobs');
  if(rerunBtn){
    const hasFailed=_wfAllJobs.some(j=>_wfSelectedJobs.has(j.job_id)&&j.status==='failed');
    rerunBtn.style.display=hasFailed?'inline-flex':'none';
  }
}

async function wfRunSelectedJobs(){
  const ids=[..._wfSelectedJobs];
  if(!ids.length){toast('Select at least one job','terr');return;}
  // Find unique pipeline groups for selected jobs and submit to Databricks
  const groupIds=new Set();
  for(const jid of ids){
    const job=_wfAllJobs.find(j=>j.job_id===jid);
    if(job&&job.group_id)groupIds.add(job.group_id);
  }
  if(!groupIds.size){toast('No pipeline groups found for selected jobs','terr');return;}
  toast('Submitting '+groupIds.size+' pipeline(s) to Databricks…','tinfo');
  let ok=0,fail=0;
  for(const gid of groupIds){
    const success=await wfRunOnDatabricks(gid);
    if(success)ok++;else fail++;
  }
  _wfSelectedJobs.clear();
  _wfUpdateJobToolbar();
  const msg=ok+' submitted'+(fail?' · '+fail+' failed':'');
  toast(msg,fail?'terr':'tok');
  setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},2000);
}

async function wfRerunFailedJobs(){
  const ids=[..._wfSelectedJobs];
  const failedIds=ids.filter(jid=>_wfAllJobs.some(j=>j.job_id===jid&&j.status==='failed'));
  if(!failedIds.length){toast('No failed jobs selected','terr');return;}
  // Find unique pipeline groups for failed jobs and resubmit to Databricks
  const groupIds=new Set();
  for(const jid of failedIds){
    const job=_wfAllJobs.find(j=>j.job_id===jid);
    if(job&&job.group_id)groupIds.add(job.group_id);
  }
  if(!groupIds.size){toast('No pipeline groups found','terr');return;}
  toast('Rerunning '+groupIds.size+' pipeline(s) on Databricks…','tinfo');
  let ok=0,fail=0;
  for(const gid of groupIds){
    const success=await wfRunOnDatabricks(gid);
    if(success)ok++;else fail++;
  }
  _wfSelectedJobs.clear();
  _wfUpdateJobToolbar();
  toast(ok+' resubmitted'+(fail?' · '+fail+' failed':''),'tok');
  setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},2000);
}

async function wfRefreshJobs(){
  try{
    const stage=G('wfFilterStage')?G('wfFilterStage').value:'';
    const status=G('wfFilterStatus')?G('wfFilterStatus').value:'';
    let url='/api/v1/workflow/jobs?';
    if(stage)url+='stage='+stage+'&';
    if(status)url+='status='+status+'&';
    const r=await fetch(url);const d=await r.json();
    if(!d.success)return;
    _wfAllJobs=d.jobs||[];
    const tb=G('wfJobTbody');
    if(!_wfAllJobs.length){
      tb.innerHTML='<tr><td colspan="9" style="padding:32px;text-align:center;color:var(--t4);">No jobs found</td></tr>';
      G('wfJobToolbar').style.display='none';
      return;
    }
    tb.innerHTML=_wfAllJobs.map(j=>{
      const isSel=_wfSelectedJobs.has(j.job_id);
      const lastRun=j.last_run_at?new Date(j.last_run_at).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      return `<tr style="border-bottom:1px solid var(--border);transition:background .15s;${isSel?'background:var(--surface-2);':''}" onmouseover="this.style.background='var(--surface-2)'" onmouseout="if(!${isSel})this.style.background=''">
      <td style="padding:8px 6px;text-align:center;"><input type="checkbox" class="wfJobChk" data-jid="${j.job_id}" ${isSel?'checked':''} onchange="wfToggleJobSelect('${j.job_id}',this.checked)"></td>
      <td style="padding:8px 10px;font-weight:600;font-family:var(--font-mono);font-size:11px;">${j.job_name}</td>
      <td style="padding:8px 10px;">${WF_STAGE_LABELS[j.stage]||j.stage}</td>
      <td style="padding:8px 10px;font-size:11px;">${j.full_table}</td>
      <td style="padding:8px 10px;"><span style="font-size:10px;font-weight:600;text-transform:uppercase;color:${j.load_type==='incremental'?'var(--amber)':'var(--blue)'};">${j.load_type}</span></td>
      <td style="padding:8px 10px;text-align:center;">${WF_STATUS_BADGES[j.status]||j.status}</td>
      <td style="padding:8px 10px;text-align:center;font-size:10px;color:var(--t3);">${lastRun}</td>
      <td style="padding:8px 10px;text-align:center;font-size:11px;">${j.run_count}${j.fail_count?' <span style="color:var(--red);">('+j.fail_count+' fail)</span>':''}</td>
      <td style="padding:8px 10px;text-align:center;white-space:nowrap;">
        <button class="btn btn-primary btn-xs" onclick="wfRunSingleJob('${j.job_id}')" title="Run" style="padding:3px 8px;">▶</button>
        ${j.status==='failed'?'<button class="btn btn-xs" onclick="wfRunSingleJob(\''+j.job_id+'\')" title="Rerun failed" style="padding:3px 8px;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;">↺</button>':''}
        <button class="btn btn-ghost btn-xs" onclick="wfViewJobLogs('${j.job_id}')" title="Logs" style="padding:3px 8px;">📋</button>
        <button class="btn btn-ghost btn-xs" onclick="wfDeleteJob('${j.job_id}')" title="Delete" style="padding:3px 8px;color:var(--red);">✕</button>
      </td>
    </tr>`;
    }).join('');
    _wfUpdateJobToolbar();
    // Auto-poll while any jobs are running
    _wfScheduleAutoRefresh();
  }catch(e){console.error('wfRefreshJobs',e);}
}

let _wfAutoRefreshTimer=null;
function _wfScheduleAutoRefresh(){
  clearTimeout(_wfAutoRefreshTimer);
  const hasRunning=_wfAllJobs.some(j=>j.status==='running');
  if(hasRunning){
    _wfAutoRefreshTimer=setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},10000);
  }
}

/* ─── Execution History ─── */
async function wfRefreshHistory(){
  try{
    const status=G('wfHistFilterStatus')?G('wfHistFilterStatus').value:'';
    let url='/api/v1/workflow/runs?limit=50';
    if(status)url+='&status='+status;
    const r=await fetch(url);const d=await r.json();
    if(!d.success)return;
    const tb=G('wfHistoryTbody');
    const runs=d.runs||[];
    if(!runs.length){
      tb.innerHTML='<tr><td colspan="9" style="padding:24px;text-align:center;color:var(--t4);">No execution history yet</td></tr>';
      return;
    }
    tb.innerHTML=runs.map(run=>{
      const dur=run.duration_sec!=null?run.duration_sec+'s':'—';
      const rows=run.rows_processed!=null&&run.rows_processed>0?Number(run.rows_processed).toLocaleString():'—';
      const started=run.started_at?new Date(run.started_at).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'}):'—';
      const runIdShort=(run.run_id||'').substring(0,8)+'…';
      return `<tr style="border-bottom:1px solid var(--border);transition:background .15s;" onmouseover="this.style.background='var(--surface-2)'" onmouseout="this.style.background=''">
        <td style="padding:6px 10px;font-family:var(--font-mono);font-size:10px;color:var(--t3);" title="${run.run_id}">${runIdShort}</td>
        <td style="padding:6px 10px;font-weight:600;font-size:11px;">${run.job_name||'—'}</td>
        <td style="padding:6px 10px;font-size:11px;">${WF_STAGE_LABELS[run.stage]||run.stage||'—'}</td>
        <td style="padding:6px 10px;font-size:11px;">${run.full_table||'—'}</td>
        <td style="padding:6px 10px;text-align:center;">${WF_STATUS_BADGES[run.status]||run.status}</td>
        <td style="padding:6px 10px;text-align:center;font-size:11px;">${dur}</td>
        <td style="padding:6px 10px;text-align:center;font-size:11px;">${rows}</td>
        <td style="padding:6px 10px;font-size:10px;color:var(--t3);">${started}</td>
        <td style="padding:6px 10px;text-align:center;white-space:nowrap;">
          <button class="btn btn-ghost btn-xs" onclick="wfViewRunLog('${run.run_id}')" title="View logs" style="padding:3px 8px;">📋</button>
          ${run.status==='failed'?'<button class="btn btn-xs" onclick="wfRerunFromHistory(\''+run.job_id+'\')" title="Rerun" style="padding:3px 8px;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;">↺ Rerun</button>':''}
        </td>
      </tr>`;
    }).join('');
  }catch(e){console.error('wfRefreshHistory',e);}
}

async function wfRefreshAuditHistory(){
  try{
    const tbl=(G('wfAuditFilterTable')||{}).value||'';
    let url='/api/v1/workflow/jobs/history';
    if(tbl)url+='?table_name='+encodeURIComponent(tbl);
    const r=await fetch(url);const d=await r.json();
    if(!d.success){console.error('audit history',d.error);return;}
    const tb=G('wfAuditTbody');
    const rows=d.history||[];
    if(!rows.length){
      tb.innerHTML='<tr><td colspan="9" style="padding:24px;text-align:center;color:var(--t4);">No archived jobs yet</td></tr>';
      return;
    }
    tb.innerHTML=rows.map(h=>{
      const created=h.created_at?new Date(h.created_at).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      const archived=h.archived_at?new Date(h.archived_at).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      const lt=h.load_type==='incremental'?'<span style="background:#dbeafe;color:#1d4ed8;padding:2px 6px;border-radius:4px;font-size:10px;font-weight:600;">INCREMENTAL</span>':'<span style="background:#e0e7ff;color:#4338ca;padding:2px 6px;border-radius:4px;font-size:10px;font-weight:600;">FULL</span>';
      const st=WF_STATUS_BADGES[h.status]||h.status||'—';
      return `<tr style="border-bottom:1px solid var(--border);transition:background .15s;" onmouseover="this.style.background='var(--surface-2)'" onmouseout="this.style.background=''">
        <td style="padding:6px 10px;font-weight:600;font-size:11px;">${h.job_name||'—'}</td>
        <td style="padding:6px 10px;font-size:11px;">${h.full_table||h.table_name||'—'}</td>
        <td style="padding:6px 10px;font-size:11px;">${WF_STAGE_LABELS[h.stage]||h.stage||'—'}</td>
        <td style="padding:6px 10px;text-align:center;">${lt}</td>
        <td style="padding:6px 10px;text-align:center;">${st}</td>
        <td style="padding:6px 10px;text-align:center;font-size:11px;">${h.run_count||0}</td>
        <td style="padding:6px 10px;font-size:10px;color:var(--t3);">${created}</td>
        <td style="padding:6px 10px;font-size:10px;color:var(--t3);">${archived}</td>
        <td style="padding:6px 10px;font-size:10px;color:var(--amber);">${h.archive_reason||'—'}</td>
      </tr>`;
    }).join('');
  }catch(e){console.error('wfRefreshAuditHistory',e);}
}

async function wfViewRunLog(runId){
  try{
    const r=await fetch('/api/v1/workflow/runs/'+runId);const d=await r.json();
    if(!d.success)return;
    const run=d.run;
    const logEl=G('wfRunLogs');
    const sc={success:'#a6e3a1',failed:'#f38ba8',running:'#89b4fa'};
    let html='<div style="color:'+(sc[run.status]||'#cdd6f4')+';font-weight:600;margin-bottom:6px;">'+run.job_name+' — '+run.status.toUpperCase()+'</div>';
    html+='<div style="color:#6c7086;margin-bottom:8px;">Run: '+run.run_id+'  ·  Started: '+(run.started_at||'—')+'</div>';
    (run.logs||[]).forEach(l=>html+='<div>'+l+'</div>');
    if(run.error)html+='<div style="color:#f38ba8;margin-top:6px;">ERROR: '+run.error+'</div>';
    logEl.innerHTML=html;
    logEl.scrollTop=logEl.scrollHeight;
  }catch(e){console.error('wfViewRunLog',e);}
}

async function wfRerunFromHistory(jobId){
  if(!jobId){toast('No job ID to rerun','terr');return;}
  // Find the group_id for this job and submit to Databricks
  try{
    const jr=await fetch('/api/v1/workflow/jobs/'+jobId);const jd=await jr.json();
    if(!jd.success){toast('Job not found','terr');return;}
    const groupId=jd.job?.group_id;
    if(!groupId){toast('No pipeline group for this job','terr');return;}
    toast('Resubmitting pipeline to Databricks…','tinfo');
    await wfRunOnDatabricks(groupId);
    setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},2000);
  }catch(e){toast(e.message,'terr');}
}

async function wfRunSingleJob(jobId){
  // Find the group_id for this job and submit to Databricks
  try{
    const job=_wfAllJobs.find(j=>j.job_id===jobId);
    const groupId=job?.group_id;
    if(!groupId){
      // Fallback: fetch job info from API
      const jr=await fetch('/api/v1/workflow/jobs/'+jobId);const jd=await jr.json();
      if(!jd.success){toast('Job not found','terr');return;}
      const gid=jd.job?.group_id;
      if(!gid){toast('No pipeline group for this job','terr');return;}
      toast('Submitting pipeline to Databricks…','tinfo');
      const pwd=(G('wfSrcPass')||{}).value||'';
      await wfRunOnDatabricks(gid, pwd);
    }else{
      toast('Submitting pipeline to Databricks…','tinfo');
      const pwd=(G('wfSrcPass')||{}).value||'';
      await wfRunOnDatabricks(groupId, pwd);
    }
    setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},2000);
  }catch(e){toast(e.message,'terr');}
}

async function wfRunPipelineGroup(groupId){
  try{
    toast('Starting pipeline…','tinfo');
    const r=await fetch('/api/v1/workflow/pipelines/'+groupId+'/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast('Pipeline started: '+d.total_jobs+' jobs queued','tok');
    (d.runs||[]).forEach(run=>{if(run.run_id)wfPollRun(run.run_id);});
    // Auto-open logs for this pipeline
    wfShowPipelineLogs(groupId, d.runs?.[0]?.full_table||groupId);
    setTimeout(()=>{wfRefreshAll();},1000);
  }catch(e){toast(e.message,'terr');
}
}

async function wfRerunPipeline(groupId){
  try{
    toast('Rerunning from first failed stage…','tinfo');
    const r=await fetch('/api/v1/workflow/pipelines/'+groupId+'/rerun',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'No failed jobs found');
    const _SL={1:'Extract',2:'Landing→Bronze',3:'Bronze→Silver'};
    toast('Rerun started from '+(_SL[d.rerun_from]||'stage '+d.rerun_from)+': '+d.total_reran+' jobs','tok');
    // Auto-open logs for this pipeline
    wfShowPipelineLogs(groupId, '');
    setTimeout(()=>wfRefreshAll(),1000);
  }catch(e){toast(e.message,'terr');}
}

async function wfDeleteJob(jobId){
  if(!(await uiConfirm('Delete this job?',{danger:true})))return;
  try{
    const r=await fetch('/api/v1/workflow/jobs/'+jobId,{method:'DELETE'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast('Deleted: '+d.job_name,'tok');
    wfRefreshAll();
  }catch(e){toast(e.message,'terr');}
}

async function wfDeletePipeline(groupId){
  if(!(await uiConfirm('Delete this entire pipeline group and all its jobs?',{danger:true})))return;
  try{
    const r=await fetch('/api/v1/workflow/pipelines/'+groupId,{method:'DELETE'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast('Pipeline deleted ('+d.deleted_jobs.length+' jobs)','tok');
    wfClosePipelineLogs();
    wfRefreshAll();
  }catch(e){toast(e.message,'terr');}
}

/* ─── Pipeline Studio: Show Logs ─── */
let _wfLogGroupId='';
let _wfLogPollTimer=null;

async function wfShowPipelineLogs(groupId, tableName){
  _wfLogGroupId=groupId;
  const card=G('wfPipelineLogCard');
  const nameEl=G('wfPipelineLogName');
  card.style.display='';
  nameEl.textContent=tableName||groupId;
  card.scrollIntoView({behavior:'smooth',block:'nearest'});
  await _wfFetchPipelineLogs(groupId);
  // Auto-poll if any run is still running
  _wfStartLogPoll();
}

function wfClosePipelineLogs(){
  _wfLogGroupId='';
  G('wfPipelineLogCard').style.display='none';
  G('wfPipelineLogs').innerHTML='<div style="color:#6c7086;">// Select a pipeline to view execution logs…</div>';
  _wfStopLogPoll();
}

function wfRefreshPipelineLogs(){
  if(_wfLogGroupId) _wfFetchPipelineLogs(_wfLogGroupId);
}

async function _wfFetchPipelineLogs(groupId){
  try{
    const r=await fetch('/api/v1/workflow/runs?group_id='+encodeURIComponent(groupId)+'&limit=30');
    const d=await r.json();
    if(!d.success)return;
    const logEl=G('wfPipelineLogs');
    const runs=d.runs||[];
    if(!runs.length){
      logEl.innerHTML='<div style="color:#6c7086;">// No runs recorded yet for this pipeline — click ⚡ Databricks to start</div>';
      return;
    }
    let html='<div style="color:#89dceb;margin-bottom:10px;font-weight:600;">// Pipeline Execution Logs — '+runs.length+' run'+(runs.length>1?'s':'')+'</div>';
    const _SL={extract:'Extract',landing_to_bronze:'Landing→Bronze',bronze_to_silver:'Bronze→Silver',dlt_bronze_silver:'⚡ SDP Bronze+Silver'};
    let hasRunning=false;
    runs.forEach(run=>{
      const sc={success:'#a6e3a1',failed:'#f38ba8',running:'#89b4fa',created:'#6c7086'};
      const icon={success:'✅',failed:'❌',running:'🔄',created:'⏸'};
      if(run.status==='running') hasRunning=true;
      const stageLabel=run.stage?(' <span style="font-size:9px;padding:1px 5px;border-radius:4px;background:#45475a;color:#89dceb;margin-left:6px;">'+(_SL[run.stage]||run.stage)+'</span>'):'';
      html+='<div style="margin-bottom:12px;padding:8px 10px;border:1px solid #313244;border-radius:6px;background:#181825;">';
      html+='<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">';
      html+='<span style="color:'+(sc[run.status]||'#cdd6f4')+';font-weight:700;">'+(icon[run.status]||'•')+' '+run.job_name+stageLabel+' — '+run.status.toUpperCase()+'</span>';
      html+='<span style="color:#585b70;font-size:10px;">'+run.run_id+'</span>';
      html+='</div>';
      // Timing info
      if(run.started_at){
        html+='<div style="color:#585b70;font-size:10px;margin-bottom:4px;">⏱ Started: '+run.started_at;
        if(run.duration_sec!=null) html+=' · Duration: '+run.duration_sec+'s';
        if(run.rows_processed) html+=' · Rows: '+Number(run.rows_processed).toLocaleString();
        html+='</div>';
      }
      // Logs
      const logs=run.logs||[];
      if(logs.length){
        logs.forEach(l=>{
          let color='#cdd6f4';
          if(l.includes('✅')||l.includes('complete')||l.includes('SUCCESS')) color='#a6e3a1';
          else if(l.includes('❌')||l.includes('FAIL')||l.includes('ERROR')) color='#f38ba8';
          else if(l.includes('🔄')||l.includes('Running')||l.includes('🚀')) color='#89b4fa';
          else if(l.includes('⚠️')) color='#fab387';
          html+='<div style="color:'+color+';line-height:1.6;">'+l+'</div>';
        });
      }
      // Error message
      if(run.error){
        html+='<div style="color:#f38ba8;margin-top:4px;padding:4px 8px;background:#f38ba822;border-radius:4px;font-size:10px;">ERROR: '+run.error+'</div>';
      }
      // Rerun button for failed jobs
      if(run.status==='failed'&&run.job_id){
        html+='<button onclick="wfRunSingleJob(\''+run.job_id+'\')" style="margin-top:6px;margin-right:6px;padding:2px 10px;font-size:10px;background:#f38ba822;color:#f38ba8;border:1px solid #f38ba8;border-radius:4px;cursor:pointer;font-weight:600;" title="Rerun this failed job">🔄 Rerun Job</button>';
      }
      // Fetch Databricks Output button (for terminal runs with dbr_run_id)
      if(run.dbr_run_id && (run.status==='success'||run.status==='failed')){
        html+='<button onclick="wfFetchDbrOutput(\''+run.run_id+'\')" style="margin-top:6px;padding:2px 10px;font-size:10px;background:#45475a;color:#cdd6f4;border:1px solid #585b70;border-radius:4px;cursor:pointer;" title="Fetch notebook output from Databricks">📋 Fetch Databricks Output</button>';
      }
      html+='</div>';
    });
    logEl.innerHTML=html;
    logEl.scrollTop=logEl.scrollHeight;
    // Store running state for poll decision
    logEl.dataset.hasRunning=hasRunning?'1':'0';
  }catch(e){console.error('_wfFetchPipelineLogs',e);}
}

function _wfStartLogPoll(){
  _wfStopLogPoll();
  _wfLogPollTimer=setInterval(()=>{
    if(!_wfLogGroupId){_wfStopLogPoll();return;}
    const logEl=G('wfPipelineLogs');
    if(logEl&&logEl.dataset.hasRunning==='1'){
      _wfFetchPipelineLogs(_wfLogGroupId);
      wfRefreshPipelines();
    }else{
      _wfStopLogPoll();
    }
  },2000);
}
function _wfStopLogPoll(){
  if(_wfLogPollTimer){clearInterval(_wfLogPollTimer);_wfLogPollTimer=null;}
}

/* ── Auto-refresh pipeline status when any pipeline is running ── */
let _wfPipelineAutoPoll=null;
function _wfStartPipelineAutoPoll(){
  if(_wfPipelineAutoPoll) return;
  _wfPipelineAutoPoll=setInterval(()=>{
    const hasRunning=_wfPipelineData.some(g=>g.status==='running'||(g.jobs||[]).some(j=>j.status==='running'));
    if(hasRunning){
      wfRefreshPipelines();
    } else {
      clearInterval(_wfPipelineAutoPoll);
      _wfPipelineAutoPoll=null;
    }
  },5000);
}
// Start auto-poll after any pipeline refresh if running pipelines exist
const _patchedRefreshPipelines=wfRefreshPipelines;
wfRefreshPipelines=async function(){
  await _patchedRefreshPipelines();
  const hasRunning=_wfPipelineData.some(g=>g.status==='running'||(g.jobs||[]).some(j=>j.status==='running'));
  if(hasRunning) _wfStartPipelineAutoPoll();
};

async function wfFetchDbrOutput(runId){
  const host=(G('wfDbrHost')||{}).value||'';
  const token=(G('wfDbrToken')||{}).value||'';
  if(!host||!token){_toast('Enter Databricks host & token first','warn');return;}
  try{
    const r=await fetch('/api/v1/workflow/runs/'+runId+'/databricks-output',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({host,token})});
    const d=await r.json();
    if(!d.success){_toast(d.message||'Failed to fetch output','error');return;}
    let msg='';
    if(d.notebook_result) msg+='📄 Result: '+d.notebook_result+'\n';
    if(d.error) msg+='🔴 Error: '+d.error+'\n';
    if(d.error_trace) msg+='📋 Trace:\n'+d.error_trace+'\n';
    if(d.tasks&&d.tasks.length){
      msg+='\n📌 Tasks:\n';
      d.tasks.forEach(t=>{msg+='  '+t.task_key+': '+(t.result_state||t.life_cycle)+(t.state_message?' — '+t.state_message:'')+'\n';});
    }
    if(!msg) msg='No output available for this run.';
    alert(msg);
  }catch(e){_toast('Error fetching output: '+e.message,'error');}
}
/* ─── / Pipeline Studio Logs ─── */

async function wfViewJobLogs(jobId){
  try{
    const r=await fetch('/api/v1/workflow/jobs/'+jobId);const d=await r.json();
    if(!d.success)return;
    const logEl=G('wfRunLogs');
    const job=d.job;
    const runs=d.runs||[];
    let html='<div style="color:#a6e3a1;margin-bottom:8px;">// === '+job.job_name+' — Run History ===</div>';
    if(!runs.length)html+='<div style="color:#6c7086;">No runs yet</div>';
    runs.forEach(run=>{
      const sc={success:'#a6e3a1',failed:'#f38ba8',running:'#89b4fa'};
      html+='<div style="margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #313244;">';
      html+='<div style="color:'+(sc[run.status]||'#cdd6f4')+';font-weight:600;">Run '+run.run_id+' — '+run.status.toUpperCase()+'</div>';
      (run.logs||[]).forEach(l=>html+='<div>'+l+'</div>');
      if(run.error)html+='<div style="color:#f38ba8;">ERROR: '+run.error+'</div>';
      html+='</div>';
    });
    logEl.innerHTML=html;
    // Auto-scroll sub-tab to jobs and switch
    switchAiSubTab('jobs',G('aiSubJobs'));
  }catch(e){console.error('wfViewJobLogs',e);}
}

function wfPollRun(runId){
  const poll=async()=>{
    try{
      const r=await fetch('/api/v1/workflow/runs/'+runId);const d=await r.json();
      if(!d.success)return;
      const run=d.run;
      // Update log panel live
      const logEl=G('wfRunLogs');
      let html='<div style="color:#89b4fa;font-weight:600;">▶ '+run.job_name+' — LIVE</div>';
      (run.logs||[]).forEach(l=>html+='<div>'+l+'</div>');
      logEl.innerHTML=html;
      logEl.scrollTop=logEl.scrollHeight;
      if(run.status==='running'){
        setTimeout(poll,1500);
      }else{
        wfRefreshAll();
      }
    }catch(e){console.error('wfPollRun',e);}
  };
  setTimeout(poll,800);
}

async function wfRefreshRuns(){
  try{
    const r=await fetch('/api/v1/workflow/runs?limit=20');const d=await r.json();
    if(!d.success)return;
    const logEl=G('wfRunLogs');
    if(!d.runs.length){logEl.innerHTML='<div style="color:#6c7086;">// No runs yet</div>';return;}
    let html='';
    d.runs.forEach(run=>{
      const sc={success:'#a6e3a1',failed:'#f38ba8',running:'#89b4fa'};
      html+='<div style="margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid #313244;">';
      html+='<div style="color:'+(sc[run.status]||'#cdd6f4')+';font-weight:600;">'+run.job_name+' — '+run.status.toUpperCase()+'</div>';
      const lastLog=(run.logs||[]).slice(-3);
      lastLog.forEach(l=>html+='<div style="font-size:10px;">'+l+'</div>');
      html+='</div>';
    });
    logEl.innerHTML=html;
  }catch(e){console.error('wfRefreshRuns',e);}
}

async function wfRefreshWatermarks(){
  try{
    const r=await fetch('/api/v1/workflow/watermarks');const d=await r.json();
    if(!d.success)return;
    const el=G('wfWatermarks');
    const wms=d.watermarks;
    const keys=Object.keys(wms);
    if(!keys.length){el.innerHTML='<div style="color:var(--t4);text-align:center;padding:12px;">No watermarks — use incremental loads to track</div>';return;}
    el.innerHTML=keys.map(k=>{
      const w=wms[k];
      return `<div style="display:flex;justify-content:space-between;align-items:center;padding:8px;border:1px solid var(--border);border-radius:var(--r-sm);margin-bottom:6px;background:var(--surface-2);">
        <div>
          <div style="font-weight:600;font-size:12px;color:var(--t1);">${k}</div>
          <div style="font-size:10px;color:var(--t3);">${w.column} = ${w.last_value||'<em>not set</em>'}</div>
        </div>
        <button class="btn btn-ghost btn-xs" onclick="wfResetWatermark('${k}')" title="Reset watermark" style="color:var(--amber);">↺ Reset</button>
      </div>`;
    }).join('');
  }catch(e){console.error('wfRefreshWatermarks',e);}
}

async function wfResetWatermark(table){
  if(!(await uiConfirm('Reset watermark for '+table+'? Next run will do a full load.')))return;
  try{
    const r=await fetch('/api/v1/workflow/watermarks/reset',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({table:table})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast(d.message,'tok');
    wfRefreshWatermarks();
  }catch(e){toast(e.message,'terr');}
}

// ═══════════ SETTINGS / DEPLOY CONFIG ═══════════

/* Settings page: toggle source fields per selected source type */
function cfgOnSrcTypeChange(selectEl){
  const v=(selectEl||G('cfgSrcType')).value;
  const isSf=(v==='snowflake');
  const nonSql=_NON_SQL_SRC(v);
  const isSp=(v==='sharepoint');
  const isApi=(v==='api');
  // SQL Server fields — SharePoint/API keep the Server field (Site/Base URL), hide Database
  const srvRow=G('cfgSrcServerRow');if(srvRow)srvRow.style.display=isSf?'none':'';
  const dbRow=G('cfgSrcDbRow');if(dbRow)dbRow.style.display=(isSf||nonSql)?'none':'';
  // Snowflake fields
  const acctRow=G('cfgSrcAccountRow');if(acctRow)acctRow.style.display=isSf?'':'none';
  const snowDbRow=G('cfgSrcSnowDbRow');if(snowDbRow)snowDbRow.style.display=isSf?'':'none';
  const extraRow=G('cfgSrcSnowExtraRow');if(extraRow)extraRow.style.display=isSf?'grid':'none';
  // SharePoint fields
  const spTenantRow=G('cfgSrcSpTenantRow');if(spTenantRow)spTenantRow.style.display=isSp?'':'none';
  // REST API fields
  const apiAuthRow=G('cfgSrcApiAuthRow');if(apiAuthRow)apiAuthRow.style.display=isApi?'':'none';
  if(typeof cfgOnApiAuthChange==='function')cfgOnApiAuthChange();
  // Dynamic labels
  _applySrcLabels('cfgSrc', v);
}
window.cfgOnSrcTypeChange=cfgOnSrcTypeChange;

/* Settings page: show Key Header Name only for api_key auth */
function cfgOnApiAuthChange(){
  const sel=G('cfgSrcApiAuthType');if(!sel)return;
  const row=G('cfgSrcApiKeyHeaderRow');
  if(row)row.style.display=sel.value==='api_key'?'':'none';
}

// ═══════════════════════════════════════════════════════════════════════════════
//  EXISTING SETTING — Medallion Layer Mapping (Cross-Account Migration)
// ═══════════════════════════════════════════════════════════════════════════════

let _cfgSelectedMode = 'new';
let _cfgExCatalogs = [];
let _cfgExSchemaMap = {}; // catalog -> [schemas], shared shape with Pipeline Studio
const _cfgExLayers = ['landing','bronze','silver','reconciliation','loggingdetails'];
let _cfgExLayerValidation = {};
_cfgExLayers.forEach(l => _cfgExLayerValidation[l] = false);

/* Switch between New / Existing tabs */
function cfgSwitchMode(mode){
  _cfgSelectedMode = mode;
  const newPane=G('cfgNewSettingPane'), exPane=G('cfgExistingSettingPane');
  const tabNew=G('cfgTabNewSetting'), tabEx=G('cfgTabExistingSetting');
  const lbl=G('cfgActiveModeLabel');
  if(mode==='existing'){
    if(newPane) newPane.style.display='none';
    if(exPane) exPane.style.display='';
    if(tabNew){tabNew.classList.remove('active');tabNew.style.borderBottom='';}
    if(tabEx){tabEx.classList.add('active');tabEx.style.borderBottom='3px solid #6366f1';}
    if(lbl) lbl.textContent='Existing Setting';
    if(!_cfgExCatalogs.length) cfgExLoadCatalogs();
    _cfgExPreFillFromConfig();
    _cfgExUpdateSharedSummary();
  } else {
    if(newPane) newPane.style.display='';
    if(exPane) exPane.style.display='none';
    if(tabNew){tabNew.classList.add('active');tabNew.style.borderBottom='3px solid #6366f1';}
    if(tabEx){tabEx.classList.remove('active');tabEx.style.borderBottom='';}
    if(lbl) lbl.textContent='New Setting';
  }
}
window.cfgSwitchMode=cfgSwitchMode;

/* Pre-fill storage/container/paths from config when switching to Existing */
function _cfgExPreFillFromConfig(){
  const cc=_cachedDeployConfig||{};
  const sa=(G('cfgStorageAcct')?.value)||cc.storage_account||'';
  const cn=(G('cfgContainer')?.value)||cc.container||'';
  const defaults={landing:'dev/landing',bronze:'dev/uc-managed/bronze',silver:'dev/uc-managed/silver',reconciliation:'dev/uc-managed/reconciliation',loggingdetails:'dev/uc-managed/loggingdetails'};
  _cfgExLayers.forEach(layer=>{
    const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
    const sEl=G('cfgEx'+cap+'Storage');
    const cEl=G('cfgEx'+cap+'Container');
    const pEl=G('cfgEx'+cap+'Path');
    if(sEl&&!sEl.value&&sa) sEl.value=sa;
    if(cEl&&!cEl.value&&cn) cEl.value=cn;
    if(pEl&&!pEl.value) pEl.value=defaults[layer]||'';
  });
}

/* Shared config summary (from New Setting or cached config) */
function _cfgExUpdateSharedSummary(){
  const cc=_cachedDeployConfig||{};
  const host=(G('cfgDbrHost')?.value)||cc.databricks_host||'';
  const src=(G('cfgSrcServer')?.value)||(cc.source||{}).server||'';
  const srcDb=(G('cfgSrcDb')?.value)||(cc.source||{}).database||'';
  const metaCat=(G('cfgMetaCatalog')?.value)||cc.metadata_catalog||'';
  const metaSch=(G('cfgMetaSchema')?.value)||cc.metadata_schema||'';
  const cdc=(G('cfgCdcMode')?.value)||(cc.cdc||{}).cdc_mode||'';
  const dlt=(G('cfgDltMode')?.value)||(cc.cdc||{}).dlt_mode||'';
  const reconCat=(G('cfgReconCatalog')?.value)||(cc.reconciliation||{}).catalog||'';
  const reconSch=(G('cfgReconSchema')?.value)||(cc.reconciliation||{}).schema||'';
  if(G('cfgExSharedHost')) G('cfgExSharedHost').textContent=host||'Not configured';
  if(G('cfgExSharedSource')) G('cfgExSharedSource').textContent=(src||'\u2014')+' / '+(srcDb||'\u2014');
  if(G('cfgExSharedMeta')) G('cfgExSharedMeta').textContent=(metaCat||'\u2014')+'.'+(metaSch||'\u2014');
  if(G('cfgExSharedCdc')) G('cfgExSharedCdc').textContent=cdc||'\u2014';
  if(G('cfgExSharedDlt')) G('cfgExSharedDlt').textContent=dlt||'\u2014';
  if(G('cfgExSharedRecon')) G('cfgExSharedRecon').textContent=(reconCat||'\u2014')+'.'+(reconSch||'\u2014');
}

/* Load ALL catalogs from UC into dropdowns */
async function cfgExLoadCatalogs(){
  try{
    // Same source as Data Modeling and Pipeline Studio: every catalog plus its
    // schemas in one call, so all three mapping UIs stay identical.
    const r=await fetch('/api/v1/datamodel/catalogs-schemas');
    const d=await r.json();
    const pairs=(d&&d.success&&Array.isArray(d.catalog_schemas))?d.catalog_schemas:[];
    _cfgExSchemaMap={};
    pairs.forEach(p=>{
      const c=p&&p.catalog, s=p&&p.schema;
      if(!c) return;
      if(!_cfgExSchemaMap[c]) _cfgExSchemaMap[c]=[];
      if(s&&_cfgExSchemaMap[c].indexOf(s)<0) _cfgExSchemaMap[c].push(s);
    });
    _cfgExCatalogs=Object.keys(_cfgExSchemaMap).sort();
    if(_cfgExCatalogs.length){
      _cfgExLayers.forEach(layer=>{
        const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
        const sel=G('cfgEx'+cap+'Catalog');
        if(!sel) return;
        const saved=sel.getAttribute('data-saved')||'';
        sel.innerHTML='<option value="">\u2014 select catalog \u2014</option>';
        _cfgExCatalogs.forEach(c=>{
          const o=document.createElement('option');
          o.value=c; o.textContent=c;
          if(c===saved) o.selected=true;
          sel.appendChild(o);
        });
        cfgExLoadSchemas(layer);
      });
    } else {
      console.warn('No catalogs returned:', (d&&d.error)||'empty');
    }
  }catch(e){console.error('cfgExLoadCatalogs error:',e);}
}
window.cfgExLoadCatalogs=cfgExLoadCatalogs;

/* Load schemas for a specific layer's selected catalog */
function cfgExLoadSchemas(layer){
  const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
  const catSel=G('cfgEx'+cap+'Catalog');
  const schSel=G('cfgEx'+cap+'Schema');
  if(!catSel||!schSel) return;
  const catalog=catSel.value;
  if(!catalog){schSel.innerHTML='<option>\u2014 select catalog first \u2014</option>';return;}
  const saved=schSel.getAttribute('data-saved')||'';
  schSel.innerHTML='<option value="">\u2014 select schema \u2014</option>';
  (_cfgExSchemaMap[catalog]||[]).slice().sort().forEach(s=>{
    const o=document.createElement('option');
    o.value=s; o.textContent=s;
    if(s===saved) o.selected=true;
    schSel.appendChild(o);
  });
}
window.cfgExLoadSchemas=cfgExLoadSchemas;

/* Collect layer mapping from form */
function _cfgExCollectLayerMapping(){
  const mapping={};
  _cfgExLayers.forEach(layer=>{
    const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
    mapping[layer]={
      catalog: G('cfgEx'+cap+'Catalog')?.value||'',
      schema: G('cfgEx'+cap+'Schema')?.value||'',
      storage_account: G('cfgEx'+cap+'Storage')?.value||'',
      container: G('cfgEx'+cap+'Container')?.value||'',
      base_path: G('cfgEx'+cap+'Path')?.value||''
    };
  });
  return mapping;
}

/* Populate form from saved mapping */
function _cfgExPopulateLayerMapping(mapping){
  if(!mapping) return;
  _cfgExLayers.forEach(layer=>{
    const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
    const lm=mapping[layer]||{};
    const catEl=G('cfgEx'+cap+'Catalog');
    const schEl=G('cfgEx'+cap+'Schema');
    if(catEl){ catEl.setAttribute('data-saved', lm.catalog||''); catEl.value=lm.catalog||''; }
    if(schEl){ schEl.setAttribute('data-saved', lm.schema||''); schEl.value=lm.schema||''; }
    const sEl=G('cfgEx'+cap+'Storage');
    const cEl=G('cfgEx'+cap+'Container');
    const pEl=G('cfgEx'+cap+'Path');
    if(sEl&&lm.storage_account) sEl.value=lm.storage_account;
    if(cEl&&lm.container) cEl.value=lm.container;
    if(pEl&&lm.base_path) pEl.value=lm.base_path;
  });
}

/* Test access for a layer */
async function cfgExTestAccess(layer){
  const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
  const statusEl=G('cfg'+cap+'Status');
  if(!statusEl) return;
  statusEl.innerHTML='<span style="font-size:10px;padding:3px 8px;border-radius:10px;background:#fef3c7;color:#d97706;font-weight:600;">Validating...</span>';
  const catalog=G('cfgEx'+cap+'Catalog')?.value||'';
  const schema=G('cfgEx'+cap+'Schema')?.value||'';
  if(!catalog){statusEl.innerHTML='<span style="font-size:10px;padding:3px 8px;border-radius:10px;background:#fee2e2;color:#dc2626;font-weight:600;">Select catalog</span>';return;}
  try{
    const r=await fetch('/api/v1/settings/validate-access',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({catalog,schema,layer})});
    const d=await r.json();
    if(d.success){
      statusEl.innerHTML='<span style="font-size:10px;padding:3px 8px;border-radius:10px;background:#d1fae5;color:#059669;font-weight:600;">\u2713 Validated</span>';
      _cfgExLayerValidation[layer]=true;
    } else {
      statusEl.innerHTML='<span style="font-size:10px;padding:3px 8px;border-radius:10px;background:#fee2e2;color:#dc2626;font-weight:600;">'+(d.error||'Failed')+'</span>';
    }
  }catch(e){
    statusEl.innerHTML='<span style="font-size:10px;padding:3px 8px;border-radius:10px;background:#fee2e2;color:#dc2626;font-weight:600;">Error</span>';
  }
}
window.cfgExTestAccess=cfgExTestAccess;


/* ═══════════════════════════════════════════════════════
   Pipeline Studio — Layer → Catalog.Schema Mapping
   ═══════════════════════════════════════════════════════ */
const _wfLayerNames = ['landing','bronze','silver','reconciliation','loggingdetails'];
let _wfLayerCatalogs = []; // cached catalog list
let _wfLayerSchemaMap = {}; // catalog -> [schemas], same source as Data Modeling

/* Load all catalogs into every layer's catalog dropdown */
async function wfLayerLoadCatalogs(){
  const statusEl=G('wfLayerMappingStatus');
  if(statusEl) statusEl.textContent='Loading catalogs...';
  try{
    // Data Modeling's endpoint: enumerates every UC catalog AND its schemas in
    // one call, so both pages always offer an identical catalog/schema list.
    const r=await fetch('/api/v1/datamodel/catalogs-schemas');
    const d=await r.json();
    const pairs=(d&&d.success&&Array.isArray(d.catalog_schemas))?d.catalog_schemas:[];
    _wfLayerSchemaMap={};
    pairs.forEach(p=>{
      const c=p&&p.catalog, s=p&&p.schema;
      if(!c) return;
      if(!_wfLayerSchemaMap[c]) _wfLayerSchemaMap[c]=[];
      if(s&&_wfLayerSchemaMap[c].indexOf(s)<0) _wfLayerSchemaMap[c].push(s);
    });
    _wfLayerCatalogs=Object.keys(_wfLayerSchemaMap).sort();
    if(!_wfLayerCatalogs.length){
      if(statusEl) statusEl.textContent='⚠️ No catalogs found';
      return;
    }
    _wfLayerNames.forEach(layer=>{
      const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
      const sel=G('wfLayer'+cap+'Cat');
      if(!sel) return;
      const saved=sel.getAttribute('data-saved')||sel.value||'';
      sel.innerHTML='<option value="">— catalog —</option>';
      _wfLayerCatalogs.forEach(c=>{
        const o=document.createElement('option');
        o.value=c; o.textContent=c;
        if(c===saved) o.selected=true;
        sel.appendChild(o);
      });
      // Schemas come from the same payload, so fill them regardless of selection
      wfLayerLoadSchemas(layer);
    });
    if(statusEl) statusEl.textContent='✓ '+_wfLayerCatalogs.length+' catalogs loaded';
    setTimeout(()=>{if(statusEl) statusEl.textContent='';},3000);
  }catch(e){
    if(statusEl) statusEl.textContent='⚠️ Error: '+e.message;
  }
}
window.wfLayerLoadCatalogs=wfLayerLoadCatalogs;

/* Fill a layer's schema dropdown from the cached catalog->schemas map */
function wfLayerLoadSchemas(layer){
  const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
  const catSel=G('wfLayer'+cap+'Cat');
  const schSel=G('wfLayer'+cap+'Sch');
  if(!catSel||!schSel) return;
  const catalog=catSel.value;
  const saved=schSel.getAttribute('data-saved')||'';
  schSel.innerHTML='<option value="">— schema —</option>';
  if(!catalog) return;
  (_wfLayerSchemaMap[catalog]||[]).slice().sort().forEach(s=>{
    const o=document.createElement('option');
    o.value=s; o.textContent=s;
    if(s===saved) o.selected=true;
    schSel.appendChild(o);
  });
}
window.wfLayerLoadSchemas=wfLayerLoadSchemas;

/* Collect the current layer mapping from dropdowns */
function _wfLayerCollectMapping(){
  const mapping={};
  _wfLayerNames.forEach(layer=>{
    const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
    mapping[layer]={
      catalog: G('wfLayer'+cap+'Cat')?.value||'',
      schema: G('wfLayer'+cap+'Sch')?.value||''
    };
  });
  return mapping;
}

/* Populate dropdowns from saved mapping */
function _wfLayerPopulateMapping(mapping){
  if(!mapping) return;
  _wfLayerNames.forEach(layer=>{
    const cap=layer.charAt(0).toUpperCase()+layer.slice(1);
    const lm=mapping[layer]||{};
    const catEl=G('wfLayer'+cap+'Cat');
    const schEl=G('wfLayer'+cap+'Sch');
    if(catEl&&lm.catalog){ catEl.setAttribute('data-saved',lm.catalog); catEl.value=lm.catalog; }
    if(schEl&&lm.schema){ schEl.setAttribute('data-saved',lm.schema); schEl.value=lm.schema; }
  });
}

/* Save layer mapping to config (persist for migration) */
async function wfLayerSaveMapping(){
  const mapping=_wfLayerCollectMapping();
  const statusEl=G('wfLayerMappingStatus');
  // Validate at least landing+bronze+silver have catalogs
  const missing=[];
  ['landing','bronze','silver'].forEach(l=>{if(!mapping[l].catalog) missing.push(l);});
  if(missing.length){
    if(statusEl) statusEl.innerHTML='<span style="color:#dc2626;">⚠️ Select catalog for: '+missing.join(', ')+'</span>';
    return;
  }
  if(statusEl) statusEl.textContent='Saving...';
  try{
    // Get current config, merge layer mapping, save back
    const gr=await fetch('/api/v1/deploy-config');
    const gd=await gr.json();
    const cfg=gd.success&&gd.config?gd.config:{};
    cfg.selected_setting='ExistingSetting';
    cfg.existing_setting=cfg.existing_setting||{};
    cfg.existing_setting.medallion_layer_mapping=mapping;
    const sr=await fetch('/api/v1/deploy-config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
    const sd=await sr.json();
    if(sd.success){
      _cachedDeployConfig=cfg;
      // NOTE: this used to also blast an UPDATE across every row of
      // wf_job_metadata (ALL pipelines, for every table ever created),
      // regardless of what's selected in Quick Create — one Save Mapping
      // click would silently repoint every existing job's target catalog.
      // Saving here now only persists the mapping for future use: check
      // "Use Layer Mapping" in Quick Create Pipeline to apply it to the
      // tables you've actually selected there.
      if(statusEl) statusEl.innerHTML='<span style="color:#059669;">✓ Mapping saved — check "Use Layer Mapping" in Quick Create to apply it to selected tables</span>';
    } else {
      if(statusEl) statusEl.innerHTML='<span style="color:#dc2626;">⚠️ '+(sd.error||'Save failed')+'</span>';
    }
    setTimeout(()=>{if(statusEl) statusEl.textContent='';},5000);
  }catch(e){
    if(statusEl) statusEl.innerHTML='<span style="color:#dc2626;">⚠️ '+e.message+'</span>';
  }
}
window.wfLayerSaveMapping=wfLayerSaveMapping;

/* Layer defaults derived from the catalogs this app provisions */
function _wfLayerDefaultMapping(cfg){
  cfg=cfg||{};
  const cats=cfg.catalogs||{};
  const schemasOf=n=>{const c=cats[n];const s=(c&&c.schemas)||[];return s[0]||'';};
  const byKeyword=kw=>Object.keys(cats).find(n=>n.toLowerCase().includes(kw))||'';
  const bronze=byKeyword('bronze'), silver=byKeyword('silver');
  const recon=cfg.reconciliation||{}, log=cfg.logging||{};
  return {
    landing:        {catalog: cfg.volume_catalog||'', schema: cfg.volume_schema||''},
    bronze:         {catalog: bronze, schema: schemasOf(bronze)},
    silver:         {catalog: silver, schema: schemasOf(silver)},
    reconciliation: {catalog: recon.catalog||'', schema: recon.schema||''},
    loggingdetails: {catalog: log.catalog||'', schema: log.schema||''},
  };
}

/* Auto-load layer mapping on Pipeline Studio init */
function _wfLayerAutoInit(){
  const cc=_cachedDeployConfig||{};
  const ex=cc.existing_setting||{};
  const saved=ex.medallion_layer_mapping||{};
  const defaults=_wfLayerDefaultMapping(cc);
  const mapping={};
  _wfLayerNames.forEach(layer=>{
    const s=saved[layer]||{}, d=defaults[layer]||{};
    mapping[layer]={catalog: s.catalog||d.catalog||'', schema: s.schema||d.schema||''};
  });
  _wfLayerPopulateMapping(mapping);
  // Load catalogs (will also restore schema selections)
  wfLayerLoadCatalogs();
}

function _collectConfig(){
  const extLocs={};
  document.querySelectorAll('#cfgExtLocList [data-extloc]').forEach(row=>{
    const n=row.querySelector('.cfg-extloc-name').value.trim();
    const u=row.querySelector('.cfg-extloc-url').value.trim();
    if(n&&u) extLocs[n]=u;
  });
  const catalogs={};
  document.querySelectorAll('#cfgCatalogList [data-catalog]').forEach(row=>{
    const n=row.querySelector('.cfg-cat-name').value.trim();
    const l=row.querySelector('.cfg-cat-loc').value.trim();
    const s=(row.querySelector('.cfg-cat-schemas')?row.querySelector('.cfg-cat-schemas').value.trim():'default')||'default';
    if(n&&l) catalogs[n]={location:l, schemas:s.split(',').map(x=>x.trim()).filter(Boolean)};
  });
  return {
    keyvault_name: (_cachedDeployConfig||{}).keyvault_name||'',
    subscription_id: G('cfgSubId').value.trim(),
    resource_group:  G('cfgResourceGroup').value.trim(),
    region:          G('cfgRegion').value,
    databricks_host: G('cfgDbrHost').value.trim(),
    databricks_token: G('cfgDbrToken').value.trim(),
    storage_account: G('cfgStorageAcct').value.trim(),
    container:       G('cfgContainer').value.trim(),
    folders:         [],
    access_connector:G('cfgAccessConnector').value.trim(),
    storage_credential_name:G('cfgStorageCredName')?.value?.trim()||'',
    storage_credential_test_auth_mode: (G('cfgStorageTestAuthMode')?.value||'pat'),
    role_assignment: 'Storage Blob Data Owner',
    external_locations: extLocs,
    catalogs:        catalogs,
    volume_name:     G('cfgVolName').value.trim(),
    volume_catalog:  G('cfgVolCatalog').value.trim(),
    volume_schema:   G('cfgVolSchema').value.trim()||'default',
    volume_path:     G('cfgVolPath').value.trim(),
    reconciliation: {
      catalog:  G('cfgReconCatalog').value.trim()||'reconciliation',
      schema:   G('cfgReconSchema').value.trim()||'hr',
      table:    G('cfgReconTable').value.trim()||'ReconcilationDetails',
      location: G('cfgReconLocation').value.trim(),
    },
    logging: {
      catalog:  G('cfgLogCatalog').value.trim()||'logging',
      schema:   G('cfgLogSchema').value.trim()||'hr',
      table:    G('cfgLogTable').value.trim()||'ExecutionLog',
      location: G('cfgLogLocation').value.trim(),
    },
    cdc: {
      cdc_mode: (G('cfgCdcMode')||{}).value||'watermark',
      dlt_mode: (G('cfgDltMode')||{}).value||'standard',
      primary_keys: (G('cfgPrimaryKeys')||{}).value ? G('cfgPrimaryKeys').value.split(',').map(s=>s.trim()).filter(Boolean) : [],
    },
    source: (function(){
      const _st=(G('cfgSrcType')||{}).value||(G('wfSrcType')||{}).value||'sqlserver';
      const _nonSql=_NON_SQL_SRC(_st);
      const s={
        source_type: _st,
        server:      G('cfgSrcServer')?.value?.trim()||(G('wfSrcServer')||{}).value?.trim()||'',
        database:    _st==='snowflake'?(G('cfgSrcSnowDb')?.value?.trim()||(G('wfSrcSnowDb')||{}).value?.trim()||''):(_nonSql?'':(G('cfgSrcDb')?.value?.trim()||(G('wfSrcDb')||{}).value?.trim()||'')),
        username:    G('cfgSrcUser')?.value?.trim()||(G('wfSrcUser')||{}).value?.trim()||'',
        password:    G('cfgSrcPass')?.value||(G('wfSrcPass')||{}).value||'',
      };
      if(_st==='snowflake'){
        s.account=G('cfgSrcAccount')?.value?.trim()||(G('wfSrcAccount')||{}).value?.trim()||'';
        s.warehouse=G('cfgSrcWarehouse')?.value?.trim()||(G('wfSrcWarehouse')||{}).value?.trim()||'';
        s.role=G('cfgSrcRole')?.value?.trim()||(G('wfSrcRole')||{}).value?.trim()||'';
      }
      if(_st==='sharepoint'){
        s.tenant_id=G('cfgSrcSpTenantId')?.value?.trim()||(G('wfSrcTenantId')||{}).value?.trim()||'';
      }
      if(_st==='api'){
        s.api_auth_type=(G('cfgSrcApiAuthType')||{}).value||(G('wfSrcApiAuthType')||{}).value||'none';
        s.api_key_header=G('cfgSrcApiKeyHeader')?.value?.trim()||(G('wfSrcApiKeyHeader')||{}).value?.trim()||'';
      }
      return s;
    })(),
    metadata_catalog: G('cfgMetaCatalog')?.value?.trim()||'',
    metadata_schema:  G('cfgMetaSchema')?.value?.trim()||'',
    infra_mode:       G('cfgInfraMode')?.value||'existing',
    azure_tenant_id:  G('cfgTenantId')?.value?.trim()||'',
    azure_client_id:  G('cfgClientId')?.value?.trim()||'',
    azure_client_secret: G('cfgClientSecret')?.value||'',
    devops_org:     G('cfgDevOpsOrg')?.value?.trim()||'',
    devops_project: G('cfgDevOpsProject')?.value?.trim()||'',
    devops_repo:    G('cfgDevOpsRepo')?.value?.trim()||'',
    devops_branch:  G('cfgDevOpsBranch')?.value?.trim()||'main',
    devops_reviewers: G('cfgDevOpsReviewers')?.value?.trim()||'',
    devops_pat:     'xxxxxxxxxxxxxxxxx',
  };
}

function _populateConfig(c){
  if(!c) return;
  G('cfgSubId').value=c.subscription_id||'';
  G('cfgResourceGroup').value=c.resource_group||'';
  G('cfgRegion').value=c.region||'centralindia';
  G('cfgDbrHost').value=c.databricks_host||'';
  G('cfgDbrToken').value=c.databricks_token||'';
  G('cfgStorageAcct').value=c.storage_account||'';
  G('cfgContainer').value=c.container||'';
  G('cfgAccessConnector').value=c.access_connector||'';
  if(G('cfgStorageCredName')) G('cfgStorageCredName').value=c.storage_credential_name||'';
  if(G('cfgStorageTestAuthMode')) G('cfgStorageTestAuthMode').value=c.storage_credential_test_auth_mode||'pat';
  if(typeof cfgStorageTestAuthModeChanged==='function') cfgStorageTestAuthModeChanged();
  if(G('cfgInfraMode')) G('cfgInfraMode').value=c.infra_mode||'existing';
  if(typeof cfgInfraModeChanged==='function') cfgInfraModeChanged();
  G('cfgTenantId').value=c.azure_tenant_id||'';
  G('cfgClientId').value=c.azure_client_id||'';
  G('cfgClientSecret').value=c.azure_client_secret||'';
  if(c.azure_tenant_id&&c.azure_client_id&&c.azure_client_secret){
    const spSt=G('cfgSpStatus');if(spSt){spSt.innerHTML='<span style="color:#16a34a;">✓ Service Principal configured</span>';}
  }
  // DevOps
  if(G('cfgDevOpsOrg'))    G('cfgDevOpsOrg').value=c.devops_org||'';
  if(G('cfgDevOpsProject'))G('cfgDevOpsProject').value=c.devops_project||'';
  if(G('cfgDevOpsRepo'))   G('cfgDevOpsRepo').value=c.devops_repo||'';
  if(G('cfgDevOpsBranch')) G('cfgDevOpsBranch').value=c.devops_branch||'main';
  if(G('cfgDevOpsReviewers')) G('cfgDevOpsReviewers').value=c.devops_reviewers||'';
  if(G('cfgDevOpsPat'))    G('cfgDevOpsPat').value='••••••••••••••••••••••• (Secret: devops-pat)';
  // External locations
  const elList=G('cfgExtLocList'); elList.innerHTML='';
  const elEntries=Object.entries(c.external_locations||{});
  (elEntries.length?elEntries:[['','']]).forEach(([n,u])=>{
    _addExtLocRow(n,u);
  });
  // Catalogs
  const catList=G('cfgCatalogList'); catList.innerHTML='';
  const catEntries=Object.entries(c.catalogs||{});
  (catEntries.length?catEntries:[['',{location:'',schemas:['default']}]]).forEach(([n,v])=>{
    if(typeof v==='string') _addCatalogRow(n,v,'default');
    else _addCatalogRow(n,v.location||'',(v.schemas||['default']).join(','));
  });
  G('cfgVolName').value=c.volume_name||'';
  G('cfgVolCatalog').value=c.volume_catalog||'';
  G('cfgVolSchema').value=c.volume_schema||'default';
  G('cfgVolPath').value=c.volume_path||'';
  // Reconciliation
  const rc=c.reconciliation||{};
  G('cfgReconCatalog').value=rc.catalog||'reconciliation';
  G('cfgReconSchema').value=rc.schema||'hr';
  G('cfgReconTable').value=rc.table||'ReconcilationDetails';
  G('cfgReconLocation').value=rc.location||'';
  // Logging
  const lc=c.logging||{};
  G('cfgLogCatalog').value=lc.catalog||'logging';
  G('cfgLogSchema').value=lc.schema||'hr';
  G('cfgLogTable').value=lc.table||'ExecutionLog';
  G('cfgLogLocation').value=lc.location||'';
  // CDC / DLT
  const cc=c.cdc||{};
  if(G('cfgCdcMode')) G('cfgCdcMode').value=cc.cdc_mode||'watermark';
  if(G('cfgDltMode')) G('cfgDltMode').value=cc.dlt_mode||'standard';
  if(G('cfgPrimaryKeys')) G('cfgPrimaryKeys').value=(cc.primary_keys||[]).join(', ');
  if(typeof cfgCdcModeChange==='function') cfgCdcModeChange();
  if(typeof cfgDltModeChange==='function') cfgDltModeChange();
  // Source connection — populate BOTH Settings fields AND Pipeline Studio fields
  const src=c.source||{};
  // Settings page source fields
  if(G('cfgSrcType')&&src.source_type) G('cfgSrcType').value=src.source_type;
  if(G('cfgSrcServer')) G('cfgSrcServer').value=src.server||'';
  if(G('cfgSrcDb'))     G('cfgSrcDb').value=src.database||'';
  if(G('cfgSrcUser'))   G('cfgSrcUser').value=src.username||'';
  if(G('cfgSrcPass'))   G('cfgSrcPass').value=src.password||'';
  // Snowflake fields
  if(G('cfgSrcAccount'))   G('cfgSrcAccount').value=src.account||'';
  if(G('cfgSrcWarehouse')) G('cfgSrcWarehouse').value=src.warehouse||'';
  if(G('cfgSrcRole'))      G('cfgSrcRole').value=src.role||'';
  if(G('cfgSrcSnowDb'))    G('cfgSrcSnowDb').value=src.database||'';
  // SharePoint / REST API fields
  if(G('cfgSrcSpTenantId'))    G('cfgSrcSpTenantId').value=src.tenant_id||'';
  if(G('cfgSrcApiAuthType'))   G('cfgSrcApiAuthType').value=src.api_auth_type||'none';
  if(G('cfgSrcApiKeyHeader'))  G('cfgSrcApiKeyHeader').value=src.api_key_header||'';
  // Toggle visibility based on source type
  if(typeof cfgOnSrcTypeChange==='function') cfgOnSrcTypeChange(G('cfgSrcType'));
  // Programmatic .value assignments above don't fire change/input events —
  // explicitly refresh the Discovery source badge so it doesn't stay stale.
  if(typeof _discUpdateSourceBadge==='function') _discUpdateSourceBadge();
  // Pipeline Studio source fields (hidden)
  if(G('wfSrcType')&&src.source_type) G('wfSrcType').value=src.source_type;
  if(G('wfSrcServer')) G('wfSrcServer').value=src.server||'';
  if(G('wfSrcDb'))     G('wfSrcDb').value=src.database||'';
  if(G('wfSrcUser'))   G('wfSrcUser').value=src.username||'';
  if(G('wfSrcPass'))   G('wfSrcPass').value=src.password||'';
  // Snowflake Pipeline Studio fields
  if(G('wfSrcAccount'))   G('wfSrcAccount').value=src.account||'';
  if(G('wfSrcWarehouse')) G('wfSrcWarehouse').value=src.warehouse||'';
  if(G('wfSrcRole'))      G('wfSrcRole').value=src.role||'';
  if(G('wfSrcSnowDb'))    G('wfSrcSnowDb').value=src.database||'';
  // SharePoint / REST API Pipeline Studio fields
  if(G('wfSrcTenantId'))     G('wfSrcTenantId').value=src.tenant_id||'';
  if(G('wfSrcApiAuthType'))  G('wfSrcApiAuthType').value=src.api_auth_type||'none';
  if(G('wfSrcApiKeyHeader')) G('wfSrcApiKeyHeader').value=src.api_key_header||'';
  if(typeof onWfSrcTypeChange==='function') onWfSrcTypeChange(G('wfSrcType'));
  // Show source info in Pipeline Studio compact card
  const _srcInfoLabel=src.source_type==='snowflake'?(src.account||''):(src.server||'');
  if(G('wfSrcConnInfo')&&_srcInfoLabel) G('wfSrcConnInfo').textContent='🟢 '+_srcInfoLabel+' / '+(src.database||'');
  // Metadata catalog/schema — Settings fields AND MetadataFlow fields
  const metaCat=c.metadata_catalog||'';
  const metaSch=c.metadata_schema||'';
  if(G('cfgMetaCatalog')) G('cfgMetaCatalog').value=metaCat;
  if(G('cfgMetaSchema'))  G('cfgMetaSchema').value=metaSch;
  if(G('wfDbrCatalog')&&metaCat) G('wfDbrCatalog').value=metaCat;
  if(G('wfDbrSchema')&&metaSch)  G('wfDbrSchema').value=metaSch;
  // MetadataFlow Databricks host/token fields (populated from Settings config)
  if(G('wfDbrHost')&&c.databricks_host)  G('wfDbrHost').value=c.databricks_host;
  if(G('wfDbrToken')&&c.databricks_token) G('wfDbrToken').value=c.databricks_token;
  // ExistingSetting — populate mode and medallion mapping (AFTER all fields set)
  if(c.existing_setting && c.existing_setting.medallion_layer_mapping){
    _cfgExPopulateLayerMapping(c.existing_setting.medallion_layer_mapping);
  }
  if(c.selected_setting){
    _cfgSelectedMode = c.selected_setting==='ExistingSetting' ? 'existing' : 'new';
    cfgSwitchMode(_cfgSelectedMode);
  }
  // Schema Comparison — auto-populate source/target from config
  const scSrc=G('scSourceSchema'), scTgt=G('scTargetSchema');
  if(scSrc&&!scSrc.value&&src.database) scSrc.value=src.database+'.dbo';
  if(scTgt&&!scTgt.value){
    const cats=Object.keys(c.catalogs||{});
    const bronzeCat=cats.find(k=>k.toLowerCase().includes('bronze'))||cats[0]||'';
    const bronzeSchema=bronzeCat&&c.catalogs[bronzeCat]?(c.catalogs[bronzeCat].schemas||[])[0]||'default':'default';
    if(bronzeCat) scTgt.value=bronzeCat+'.'+bronzeSchema;
  }
  /* refresh interactive status after populate */
  if(typeof cfgUpdateStatus==='function') cfgUpdateStatus();
  if(typeof cfgDeriveAbfss==='function') cfgDeriveAbfss();
}

/* ── Settings Accordion & Interactive Helpers ── */
window.cfgToggleAccordion=function(id){
  const el=G(id); if(!el)return;
  el.classList.toggle('open');
};
window.cfgUpdateStatus=function(){
  const filled=id=>{const e=G(id);return e&&e.value.trim().length>0;};
  const dot=(pillId,ok)=>{const p=G(pillId);if(!p)return;const d=p.querySelector('.cfg-dot');if(d){d.className='cfg-dot'+(ok?' ok':'');}};
  dot('cfgStatAzure',filled('cfgSubId')&&filled('cfgDbrHost')&&filled('cfgDbrToken'));
  const _stCfg=(G('cfgSrcType')||{}).value||'sqlserver';
  let _srcOk;
  if(_stCfg==='snowflake')      _srcOk=filled('cfgSrcAccount');
  else if(_stCfg==='sharepoint')_srcOk=filled('cfgSrcServer')&&filled('cfgSrcSpTenantId')&&filled('cfgSrcUser');
  else if(_stCfg==='api')       _srcOk=filled('cfgSrcServer');
  else                          _srcOk=filled('cfgSrcServer')&&filled('cfgSrcDb');
  dot('cfgStatSrc',_srcOk);
  dot('cfgStatStorage',filled('cfgStorageAcct')&&filled('cfgContainer'));
  dot('cfgStatUC',G('cfgCatalogList')&&G('cfgCatalogList').children.length>0);
  dot('cfgStatCDC',filled('cfgCdcMode'));
  dot('cfgStatDevOps',filled('cfgDevOpsOrg')&&filled('cfgDevOpsProject')&&filled('cfgDevOpsRepo'));
  const h=G('cfgHintAzure');if(h){const host=G('cfgDbrHost');h.textContent=host&&host.value?host.value.replace(/^https?:\/\//,'').slice(0,30):'';};
  const hs=G('cfgHintSrc');if(hs){const _sfHint=(G('cfgSrcType')||{}).value==='snowflake';const sv=_sfHint?G('cfgSrcAccount'):G('cfgSrcServer');hs.textContent=sv&&sv.value?sv.value.slice(0,30):'';};
};
window.cfgDeriveAbfss=function(){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  const preview=G('cfgAbfssPreview');
  const base=G('cfgAbfssBase');
  if(acct&&cont){
    const url='abfss://'+cont+'@'+acct+'.dfs.core.windows.net';
    if(base) base.textContent=url;
    if(preview) preview.style.display='block';
  } else {
    if(preview) preview.style.display='none';
  }
  cfgAutoFillDependents();
};

/* Fill every field that derives from Storage Account/Container, but only
   when the user hasn't typed a value of their own — keeps Storage &
   Unity Catalog sections effectively "zero manual input" beyond account
   name + container. */
window.cfgAutoFillDependents=function(){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  if(!acct||!cont) return;
  const base='abfss://'+cont+'@'+acct+'.dfs.core.windows.net';
  const fillIfEmpty=(id,val)=>{const f=G(id);if(f&&!f.value.trim())f.value=val;};
  fillIfEmpty('cfgAccessConnector', acct+'_access');
  fillIfEmpty('cfgStorageCredName', acct+'_cred');
  fillIfEmpty('cfgVolName', 'landing');
  fillIfEmpty('cfgVolCatalog', (Object.keys((_cachedDeployConfig||{}).catalogs||{})[0])||'dev_volumes');
  fillIfEmpty('cfgVolSchema', 'default');
  fillIfEmpty('cfgVolPath', base+'/dev/landing');
  fillIfEmpty('cfgReconLocation', base+'/dev/uc-managed');
  fillIfEmpty('cfgLogLocation', base+'/dev/uc-managed');
  document.querySelectorAll('[data-extloc] .cfg-extloc-url').forEach(f=>{if(!f.value.trim())f.value=base;});
  document.querySelectorAll('[data-catalog]').forEach(row=>{
    const nameInp=row.querySelector('.cfg-cat-name');
    const locInp=row.querySelector('.cfg-cat-loc');
    const catName=(nameInp&&nameInp.value.trim())||'';
    if(locInp&&!locInp.value.trim()) locInp.value=base+'/dev/uc-managed'+(catName?'/'+catName:'');
  });
};
window.cfgAutoFillVolPath=function(){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  if(!acct||!cont){toast('Please fill Storage Account and Container first.','terr');return;}
  const f=G('cfgVolPath');if(f)f.value='abfss://'+cont+'@'+acct+'.dfs.core.windows.net/dev/landing';
};
window.cfgAutoFillLoc=function(fieldId){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  if(!acct||!cont){toast('Please fill Storage Account and Container first.','terr');return;}
  const f=G(fieldId);if(f)f.value='abfss://'+cont+'@'+acct+'.dfs.core.windows.net/dev/uc-managed';
};
window.cfgAutoFillInput=function(inputEl){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  if(!acct||!cont){toast('Please fill Storage Account and Container first.','terr');return;}
  if(inputEl)inputEl.value='abfss://'+cont+'@'+acct+'.dfs.core.windows.net';
};
window.cfgAutoFillCatLoc=function(row){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  if(!acct||!cont){toast('Please fill Storage Account and Container first.','terr');return;}
  const nameInp=row.querySelector('.cfg-cat-name');
  const catName=(nameInp&&nameInp.value.trim())||'';
  const locInp=row.querySelector('.cfg-cat-loc');
  if(locInp) locInp.value='abfss://'+cont+'@'+acct+'.dfs.core.windows.net/dev/uc-managed'+(catName?'/'+catName:'');
};
window.cfgSwitchSubTab=function(tab){
  const recon=G('cfgSubReconPanel'),log=G('cfgSubLogPanel');
  const tRecon=G('cfgSubRecon'),tLog=G('cfgSubLog');
  if(tab==='recon'){
    if(recon)recon.style.display='';if(log)log.style.display='none';
    if(tRecon)tRecon.classList.add('active');if(tLog)tLog.classList.remove('active');
  } else {
    if(recon)recon.style.display='none';if(log)log.style.display='';
    if(tRecon)tRecon.classList.remove('active');if(tLog)tLog.classList.add('active');
  }
};
window.cfgTogglePw=function(fieldId,btn){
  const f=G(fieldId);if(!f)return;
  const isP=f.type==='password';f.type=isP?'text':'password';
  if(btn)btn.title=isP?'Hide':'Show';
};
window.cfgInfraModeChanged=function(){
  const mode=(G('cfgInfraMode')||{}).value||'existing';
  const btn=G('btnDeployInfra');
  if(btn) btn.title = mode==='create'
    ? 'Creates Storage Account, Access Connector + RBAC, then configures Unity Catalog. Requires an Azure Service Principal.'
    : 'Skips Azure resource creation — only configures Unity Catalog against existing resources. Works with just a Databricks PAT.';
};
window.cfgStorageTestAuthModeChanged=function(){
  const mode=(G('cfgStorageTestAuthMode')||{}).value||'pat';
  const hint=G('cfgStorageTestModeHint');
  if(!hint)return;
  if(mode==='service_principal'){
    hint.textContent='Uses Databricks Host + Azure Service Principal (Tenant ID, Client ID, Client Secret) from the section above.';
  }else{
    hint.textContent='Uses Databricks Host + PAT token. PAT can be left blank to use saved secret token.';
  }
};

function _addExtLocRow(name,url){
  const d=document.createElement('div');
  d.className='cfg-grid'; d.style.marginBottom='8px'; d.setAttribute('data-extloc','');
  d.innerHTML='<div><label class="lbl">Location Name</label><input class="inp cfg-extloc-name" placeholder="e.g. landing_loc_mig" value="'+(name||'')+'"></div>'+
    '<div style="display:flex;gap:6px;align-items:flex-end;"><div style="flex:1;"><label class="lbl">ABFSS URL <button type=button class=cfg-auto-btn onclick="cfgAutoFillInput(this.closest(\'[data-extloc]\').querySelector(\'.cfg-extloc-url\'))">Auto-fill</button></label><input class="inp cfg-extloc-url" placeholder="abfss://..." value="'+(url||'')+'"></div>'+
    '<button class="btn btn-ghost btn-xs" onclick="this.closest(\'.cfg-grid\').remove()" style="margin-bottom:2px;color:var(--red);" title="Remove">&times;</button></div>';
  G('cfgExtLocList').appendChild(d);
  if(typeof cfgAutoFillDependents==='function') cfgAutoFillDependents();
}
function cfgAddExtLoc(){ _addExtLocRow('',''); }

function _addCatalogRow(name,loc,schemas){
  const d=document.createElement('div');
  d.className='cfg-grid-3'; d.style.marginBottom='8px'; d.setAttribute('data-catalog','');
  d.innerHTML='<div><label class="lbl">Catalog Name</label><input class="inp cfg-cat-name" placeholder="e.g. bronze" value="'+(name||'')+'"></div>'+
    '<div><label class="lbl">Managed Location (ABFSS) <button type=button class=cfg-auto-btn onclick="cfgAutoFillCatLoc(this.closest(\'[data-catalog]\'))">Auto-fill</button></label><input class="inp cfg-cat-loc" placeholder="abfss://..." value="'+(loc||'')+'"></div>'+
    '<div style="display:flex;gap:6px;align-items:flex-end;"><div style="flex:1;"><label class="lbl">Schemas</label><input class="inp cfg-cat-schemas" placeholder="default,hr,raw" value="'+(schemas||'default')+'"></div>'+
    '<button class="btn btn-ghost btn-xs" onclick="this.closest(\'.cfg-grid-3\').remove()" style="margin-bottom:2px;color:var(--red);" title="Remove">&times;</button></div>';
  G('cfgCatalogList').appendChild(d);
  if(typeof cfgAutoFillDependents==='function') cfgAutoFillDependents();
}
function cfgAddCatalog(){ _addCatalogRow('','','default'); }

function cfgCdcModeChange(){
  const mode=(G('cfgCdcMode')||{}).value||'watermark';
  const ctCfg=G('cfgCtConfig');
  if(ctCfg) ctCfg.style.display=(mode==='change_tracking'?'block':'none');
  const badge=G('cfgCdcBadge');
  if(badge) badge.textContent=(mode==='change_tracking'?'CT':'CDC');
  // Update layer cards based on current DLT mode too
  cfgDltModeChange();
}

function cfgDltModeChange(){
  const mode=(G('cfgDltMode')||{}).value||'standard';
  const cdcMode=(G('cfgCdcMode')||{}).value||'watermark';
  const l1=G('cfgLayer1Label'),d1=G('cfgLayer1Desc'),s1=G('cfgLayer1Sub');
  const l2=G('cfgLayer2Label'),d2=G('cfgLayer2Desc'),s2=G('cfgLayer2Sub');
  const l3=G('cfgLayer3Label'),d3=G('cfgLayer3Desc'),s3=G('cfgLayer3Sub');
  if(mode==='dlt'){
    if(l1)l1.textContent='Bronze Layer';
    if(d1)d1.textContent='@dlt.table + @dlt.expect_or_drop';
    if(s1)s1.textContent='Quality expectations on landing data';
    if(l2)l2.textContent='Silver Layer';
    if(d2)d2.textContent=cdcMode==='change_tracking'?'dlt.apply_changes() — SCD Type 1':'@dlt.table + DQ validation';
    if(s2)s2.textContent='Cleansing, dedup & quality gates';
    if(l3)l3.textContent='CDF Enabled';
    if(d3)d3.textContent='Change Data Feed on all layers';
    if(s3)s3.textContent='delta.enableChangeDataFeed = true';
  } else {
    // Standard PySpark — Landing → Bronze → Silver
    if(l1)l1.textContent='Landing Zone';
    if(d1)d1.textContent='Volume/ADLS ingestion';
    if(s1)s1.textContent='Raw files → Delta via PySpark notebook';
    if(l2)l2.textContent='Bronze Layer';
    if(d2)d2.textContent='MERGE INTO + audit columns';
    if(s2)s2.textContent='Append/merge with __loaded_at, __source metadata';
    if(l3)l3.textContent='Silver Layer';
    if(d3)d3.textContent='PySpark cleanse + dedup';
    if(s3)s3.textContent='Type casting, null handling, business rules';
  }
}
// Initialize layer cards on page load
try{_wfLayerAutoInit();}catch(e){}
try{cfgDltModeChange();}catch(e){}
try{cfgStorageTestAuthModeChanged();}catch(e){}
try{cfgInfraModeChanged();}catch(e){}

async function cfgTestSourceConn(){
  const badge=G('cfgSrcConnBadge');
  const server=G('cfgSrcServer')?.value?.trim();
  const db=G('cfgSrcDb')?.value?.trim();
  const user=G('cfgSrcUser')?.value?.trim();
  const pwd=G('cfgSrcPass')?.value||'';
  const srcType=G('cfgSrcType')?.value||'sqlserver';
  const nonSql=_NON_SQL_SRC(srcType);
  // Snowflake validation
  if(srcType==='snowflake'){
    const account=G('cfgSrcAccount')?.value?.trim();
    if(!account||!user){toast('Fill in account identifier and username','terr');return;}
  } else if(srcType==='sharepoint'){
    const tenantId=G('cfgSrcSpTenantId')?.value?.trim();
    if(!server){toast('Fill in the SharePoint Site URL','terr');return;}
    if(!tenantId||!user){toast('Fill in Tenant ID and Client ID','terr');return;}
  } else if(srcType==='api'){
    if(!server){toast('Fill in the API Base URL','terr');return;}
    const at=(G('cfgSrcApiAuthType')||{}).value||'none';
    if(at==='basic'&&!user){toast('Fill in a username for Basic Auth','terr');return;}
    if(at!=='none'&&!pwd){toast('Fill in the secret for '+at+' auth','terr');return;}
  } else {
    if(!server||!db||!user){toast('Fill in server, database and username','terr');return;}
  }
  badge.textContent='Testing…';badge.style.background='#f59e0b';badge.style.color='#fff';
  const controller=new AbortController();
  const tid=setTimeout(()=>controller.abort(),90000);
  try{
    const payload={source_type:srcType,server:server,database:nonSql?'':db,username:user,password:pwd};
    if(srcType==='snowflake'){
      payload.account=G('cfgSrcAccount')?.value?.trim()||'';
      payload.warehouse=G('cfgSrcWarehouse')?.value?.trim()||'';
      payload.role=G('cfgSrcRole')?.value?.trim()||'';
      payload.database=G('cfgSrcSnowDb')?.value?.trim()||'';
    }
    if(srcType==='sharepoint'){
      payload.tenant_id=G('cfgSrcSpTenantId')?.value?.trim()||'';
    }
    if(srcType==='api'){
      payload.api_auth_type=(G('cfgSrcApiAuthType')||{}).value||'none';
      payload.api_key_header=G('cfgSrcApiKeyHeader')?.value?.trim()||'';
    }
    const r=await fetch('/api/v1/source/test-connection',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify(payload),signal:controller.signal});
    clearTimeout(tid);
    const ct = r.headers.get('content-type')||'';
    let d;
    if(ct.includes('application/json')){
      d = await r.json();
    } else {
      const txt = await r.text();
      // Detect auth failure
      if(r.status===401){
        throw new Error('Authentication required — access this app through Databricks.');
      }
      // Strip HTML tags for a readable message
      const plain = txt.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,300);
      throw new Error('Server returned HTML (HTTP '+r.status+'): '+(plain||'check server logs'));
    }
    if(d.success){
      badge.textContent='Connected ✓';badge.style.background='#10b981';badge.style.color='#fff';
      badge.className='cfg-conn-badge pass';
      toast('Source connection successful!'+(d.server_version?' — '+d.server_version:''),'tok');
      // Refresh hint to show current source identifier (account for Snowflake, server for SQL)
      if(typeof cfgUpdateStatus==='function') cfgUpdateStatus();
      // Auto-copy to Pipeline Studio fields
      if(G('wfSrcType')) G('wfSrcType').value=srcType;
      if(G('wfSrcServer')) G('wfSrcServer').value=server;
      if(G('wfSrcDb')) G('wfSrcDb').value=db;
      if(G('wfSrcUser')) G('wfSrcUser').value=user;
      if(G('wfSrcPass')) G('wfSrcPass').value=pwd;
      if(srcType==='snowflake'){
        if(G('wfSrcAccount'))   G('wfSrcAccount').value=G('cfgSrcAccount')?.value||'';
        if(G('wfSrcWarehouse')) G('wfSrcWarehouse').value=G('cfgSrcWarehouse')?.value||'';
        if(G('wfSrcRole'))      G('wfSrcRole').value=G('cfgSrcRole')?.value||'';
        if(G('wfSrcSnowDb'))    G('wfSrcSnowDb').value=G('cfgSrcSnowDb')?.value||'';
      }
      if(srcType==='sharepoint'&&G('wfSrcTenantId')){
        G('wfSrcTenantId').value=G('cfgSrcSpTenantId')?.value||'';
      }
      if(srcType==='api'){
        if(G('wfSrcApiAuthType'))  G('wfSrcApiAuthType').value=G('cfgSrcApiAuthType')?.value||'none';
        if(G('wfSrcApiKeyHeader')) G('wfSrcApiKeyHeader').value=G('cfgSrcApiKeyHeader')?.value||'';
      }
    }else{
      throw new Error(d.error||('Connection failed (HTTP '+r.status+')'));
    }
  }catch(e){
    clearTimeout(tid);
    const msg=e.name==='AbortError'?'Connection test timed out (90s) — database may still be resuming, try again.':e.message;
    badge.textContent='Failed ✕';badge.style.background='#ef4444';badge.style.color='#fff';
    badge.className='cfg-conn-badge fail';
    toast(msg,'terr');
    console.error('[Test Connection] error:', e);
  }
}

async function cfgTestDatabricksConn(){
  const badge=G('cfgDbrConnBadge');
  const info=G('cfgDbrConnInfo');
  const host=G('cfgDbrHost')?.value?.trim();
  const token=G('cfgDbrToken')?.value?.trim();
  // Token is optional here — if left blank, the backend falls back to the PAT
  // already stored in the Databricks secret scope (same as everywhere else in
  // the app). Only the host is required since there's no server-side fallback for it.
  if(!host){toast('Fill in the Databricks Host URL','terr');return;}
  badge.textContent='Testing…';badge.style.background='#f59e0b';badge.style.color='#fff';
  if(info) info.textContent='';
  try{
    const r=await fetch('/api/v1/test-databricks',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({databricks_host:host,databricks_token:token})});
    const ct=r.headers.get('content-type')||'';
    let d;
    if(ct.includes('application/json')){
      d=await r.json();
    }else{
      const txt=await r.text();
      if(r.status===401||r.status===302||/<title>\s*Login/i.test(txt)||/name=["']password["']/i.test(txt)){
        throw new Error('Session expired — please log in again and retry.');
      }
      const plain=txt.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,300);
      throw new Error('Server returned HTML (HTTP '+r.status+'): '+(plain||'check server logs'));
    }
    if(d.success){
      badge.textContent='Connected ✓';badge.style.background='#10b981';badge.style.color='#fff';
      badge.className='cfg-conn-badge pass';
      const detail=d.total_clusters!=null?d.running_clusters+'/'+d.total_clusters+' clusters running':'';
      if(info) info.textContent=detail;
      toast('Databricks workspace connected!'+(detail?' — '+detail:''),'tok');
    }else{
      throw new Error(d.error||('Connection failed (HTTP '+r.status+')'));
    }
  }catch(e){
    badge.textContent='Failed ✕';badge.style.background='#ef4444';badge.style.color='#fff';
    badge.className='cfg-conn-badge fail';
    if(info) info.textContent=e.message;
    toast(e.message,'terr');
    console.error('[Databricks Test] error:',e);
  }
}

function cfgPreview(){
  const w=G('cfgJsonWrap');
  if(w) w.style.display=w.style.display==='none'?'':'none';
  G('cfgJsonPreview').textContent=JSON.stringify(_collectConfig(),null,2);
}

/* Always show + refresh the JSON preview (used after Save) — unlike cfgPreview(),
   never hides it, so saving while the preview is already open doesn't make it
   vanish and look like the save silently failed. */
function _cfgShowPreview(){
  const w=G('cfgJsonWrap');
  if(w) w.style.display='';
  G('cfgJsonPreview').textContent=JSON.stringify(_collectConfig(),null,2);
}

async function cfgTestStorageCredential(){
  const credName=(G('cfgStorageCredName')?.value||G('cfgAccessConnector')?.value||'').trim();
  if(!credName){toast('Enter a Storage Credential Name (or Access Connector Name) first','terr');return;}
  const authMode=(G('cfgStorageTestAuthMode')?.value||'pat').trim();
  const host=((G('cfgDbrHost')?.value||'').trim()||((_cachedDeployConfig||{}).databricks_host||'').trim());
  const token=(G('cfgDbrToken')?.value||'').trim();
  const tenantId=(G('cfgTenantId')?.value||'').trim();
  const clientId=(G('cfgClientId')?.value||'').trim();
  const clientSecret=(G('cfgClientSecret')?.value||'').trim();
  if(!host){
    toast('Databricks Host missing — opening "Azure Subscription & Databricks" section above','terr',5000);
    const acc=G('cfgAccAzure');
    if(acc){acc.classList.add('open');acc.scrollIntoView({behavior:'smooth',block:'start'});}
    G('cfgDbrHost')?.focus();
    return;
  }
  if(authMode==='service_principal'&&(!tenantId||!clientId||!clientSecret)){
    toast('Service Principal mode requires Tenant ID, Client ID, and Client Secret','terr',5000);
    const acc=G('cfgAccAzure');
    if(acc){acc.classList.add('open');acc.scrollIntoView({behavior:'smooth',block:'start'});}
    (!tenantId?G('cfgTenantId'):(!clientId?G('cfgClientId'):G('cfgClientSecret')))?.focus();
    return;
  }
  // Build test URL from storage account + container
  const sa=(G('cfgStorageAcct')?.value||'').trim();
  const ct=(G('cfgContainer')?.value||'').trim();
  const testUrl=sa&&ct?'abfss://'+ct+'@'+sa+'.dfs.core.windows.net':'';
  const btn=G('btnTestStorageCred');
  const status=G('storageCredStatus');
  const detail=G('storageCredDetail');
  btn.disabled=true;btn.textContent='Testing…';
  status.textContent='';status.style.cssText='font-size:10px;color:var(--t3);';
  detail.style.display='none';detail.textContent='';
  try{
    const r=await fetch('/api/v1/test-storage-credential',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
      databricks_host:host,
      databricks_token:token,
      storage_credential_name:credName,
      test_url:testUrl,
      auth_mode:authMode,
      azure_tenant_id:tenantId,
      azure_client_id:clientId,
      azure_client_secret:clientSecret,
    })});
    const ctResp=r.headers.get('content-type')||'';
    let d;
    if(ctResp.includes('application/json')){
      d=await r.json();
    }else{
      const txt=await r.text();
      if(r.status===401||r.status===302||/<title>\s*Login/i.test(txt)||/name=["']password["']/i.test(txt)){
        throw new Error('Session expired — please log in again and retry.');
      }
      const plain=txt.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,300);
      throw new Error('Server returned HTML (HTTP '+r.status+'): '+(plain||'check server logs'));
    }
    if(d.success){
      status.style.cssText='font-size:11px;font-weight:600;color:#16a34a;background:#f0fdf4;padding:3px 9px;border-radius:20px;border:1px solid #bbf7d0;display:inline-block;';
      status.textContent='✅ '+(d.message||'Credential valid');
      let lines=['Auth mode: '+(authMode==='service_principal'?'Service Principal':'PAT Token'),'Credential: '+d.credential_name,'ID: '+d.credential_id,'Owner: '+d.owner];
      if(d.access_connector_id) lines.push('Access Connector: '+d.access_connector_id);
      if(d.external_location) lines.push('External Location: '+d.external_location+' (covers this path)');
      if(d.validation&&d.validation.passed) lines.push('Validation: '+(d.validation.overlap?'PASSED (path already managed by external location)':'PASSED for '+d.validation.url));
      detail.textContent=lines.join('\n');
      detail.style.display='block';detail.style.borderColor='#16a34a';
      toast('Storage credential is valid','tok');
    }else{
      status.style.cssText='font-size:11px;font-weight:600;color:#dc2626;background:#fef2f2;padding:3px 9px;border-radius:20px;border:1px solid #fecaca;display:inline-block;';
      status.textContent='❌ '+(d.error||'Validation failed');
      let lines=[d.error||'Failed'];
      if(d.detail) lines.push(d.detail);
      if(d.failed_checks) lines.push('','Failed checks:',  ...d.failed_checks);
      if(d.credential_id) lines.push('','Credential ID: '+d.credential_id,'Owner: '+(d.owner||'?'));
      if(d.access_connector_id) lines.push('Access Connector: '+d.access_connector_id);
      detail.textContent=lines.join('\n');
      detail.style.display='block';detail.style.borderColor='#dc2626';
      toast(d.error||'Storage credential test failed','terr');
    }
  }catch(e){
    status.style.cssText='font-size:11px;font-weight:600;color:#dc2626;background:#fef2f2;padding:3px 9px;border-radius:20px;border:1px solid #fecaca;display:inline-block;';status.textContent='❌ '+e.message;
    toast('Error: '+e.message,'terr');
  }finally{
    btn.disabled=false;
    btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;margin-right:4px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Test Storage Credential';
  }
}

async function cfgCleanMetadata(){
  const inp=G('cleanConfirmInput');
  if(!inp||inp.value.trim()!=='CLEAN'){
    toast('Type "CLEAN" in the confirmation box to proceed','terr');return;
  }
  const cleanAdls=G('cleanChkAdls')?.checked??true;
  const cleanTables=G('cleanChkTables')?.checked??true;
  if(!cleanAdls&&!cleanTables){toast('Select at least one option','terr');return;}
  const btn=G('btnCleanMeta');
  const logsEl=G('cleanMetaLogs');
  btn.disabled=true;btn.textContent='Cleaning…';
  logsEl.style.display='block';logsEl.textContent='Starting metadata cleanup…\n';
  try{
    const r=await fetch('/api/v1/settings/clean-metadata',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({clean_adls:cleanAdls,clean_tables:cleanTables})
    });
    const txt=await r.text();
    if(!r.ok){
      throw new Error(txt.startsWith('<')?'Server error ('+r.status+') — the operation may have timed out. Try cleaning tables and ADLS separately.':txt);
    }
    if(!txt){
      throw new Error('Server returned empty response — the worker may have run out of memory. Please retry.');
    }
    let d;
    try{d=JSON.parse(txt);}catch(pe){throw new Error('Invalid response from server: '+txt.substring(0,200));}
    if(!d.success) throw new Error(d.error||'Cleanup failed');
    logsEl.textContent=(d.log||[]).join('\n')+'\n\n✅ '+d.summary;
    toast('Metadata cleaned successfully','tok');
  }catch(e){
    logsEl.textContent+='\n❌ Error: '+e.message;
    toast(e.message,'terr');
  }finally{
    btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:13px;height:13px;margin-right:4px;"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg> Clean Metadata';
    inp.value='';
  }
}

async function saveDeployConfig(){
  const cfg=_collectConfig();
  // Add selected_setting mode
  cfg.selected_setting = _cfgSelectedMode==='existing' ? 'ExistingSetting' : 'NewSetting';
  // Add existing_setting medallion layer mapping
  cfg.existing_setting = { medallion_layer_mapping: _cfgExCollectLayerMapping() };
  // ExistingSetting: warn if layers incomplete but DO NOT block save
  if(cfg.selected_setting==='ExistingSetting'){
    const mapping=cfg.existing_setting.medallion_layer_mapping;
    const incomplete=[];
    ['landing','bronze','silver'].forEach(layer=>{
      const lm=mapping[layer]||{};
      if(!lm.catalog||!lm.schema){
        incomplete.push(layer);
      }
    });
    if(incomplete.length){
      toast('Note: '+incomplete.join(', ')+' layer(s) need catalog/schema — fill before migration','twarn');
    }
  }
  // subscription_id is optional — save config even without it
  try{
    const r=await fetch('/api/v1/deploy-config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
    const d=await r.json();
    if(!d.success) throw new Error(d.error||'Save failed');
    if(d.durable===false){
      toast(d.warning||'Saved locally only — this will be lost on the next deploy. Retry once the Databricks SQL Warehouse is reachable.','twarn',8000);
    } else {
      toast('Configuration saved','tok');
    }
    _cachedDeployConfig=cfg; // update cache
    const banner=G('cfgSavedBanner'); banner.style.display='block';
    setTimeout(()=>{banner.style.display='none';},4000);
    _cfgShowPreview();
    // Auto-populate all pages from saved config
    _populateConfig(cfg);
    // Silently auto-init MetadataFlow (fire-and-forget, non-blocking)
    if(cfg.databricks_host&&cfg.databricks_token&&cfg.metadata_catalog&&cfg.metadata_schema){
      fetch('/api/v1/workflow/auto-init',{method:'POST'}).then(r=>r.json()).then(d=>{
        if(d.success){_wfMetaReady=true;}
      }).catch(()=>{});
    }
  }catch(e){toast(e.message,'terr');}
}

async function deployInfrastructure(){
  const btn=G('btnDeployInfra');
  const prog=G('cfgDeployProgress');
  const stepsEl=G('cfgDeploySteps');
  const logsEl=G('cfgDeployLogs');
  const summaryEl=G('cfgDeploySummary');

  // First save the config
  const cfg=_collectConfig();
  if(!cfg.subscription_id||!cfg.storage_account){
    toast('Subscription ID and Storage Account are required','terr');return;
  }
  let infraMode=(G('cfgInfraMode')?.value||'existing');
  // Creating Azure resources (Storage Account, Access Connector, RBAC) goes
  // through Azure Resource Manager, which a Databricks PAT cannot authenticate
  // to. Rather than blocking, auto-fallback to "Use existing infrastructure"
  // when no Service Principal is configured — that's almost always what's
  // actually wanted when Storage Account/Access Connector already exist
  // (e.g. an older saved config still has infra_mode="create" from before
  // this dropdown existed).
  if(infraMode==='create'){
    const missing=[['Tenant ID',cfg.azure_tenant_id],['Client ID',cfg.azure_client_id],['Client Secret',cfg.azure_client_secret]]
      .filter(([,v])=>!(v||'').trim()).map(([n])=>n);
    if(missing.length){
      toast('No Azure Service Principal configured — switching to "Use existing infrastructure" (Unity Catalog only, no new Azure resources).','tinfo',6000);
      infraMode='existing';
      cfg.infra_mode='existing';
      if(G('cfgInfraMode')) G('cfgInfraMode').value='existing';
      if(typeof cfgInfraModeChanged==='function') cfgInfraModeChanged();
    }
  }
  if(infraMode==='create'){
    if(!cfg.resource_group){ toast('Resource Group is required to create new infrastructure','terr');return; }
    if(!cfg.access_connector){ toast('Access Connector Name is required to create new infrastructure','terr');return; }
  }
  try{
    const sr=await fetch('/api/v1/deploy-config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
    const sd=await sr.json();
    if(!sd.success) throw new Error(sd.error||'Save failed');
  }catch(e){toast('Failed to save config: '+e.message,'terr');return;}

  // Confirm
  const confirmMsg = infraMode==='create'
    ? ('This will CREATE new Azure resources and assign RBAC:\n'
       +'  • Storage Account + Container + Folders\n'
       +'  • Access Connector (+ Storage Blob Data Owner role)\n'
       +'  • Storage Credential, External Locations, Catalogs, Volume\n\n'
       +'Subscription: '+cfg.subscription_id+'\nResource Group: '+cfg.resource_group
       +'\nStorage: '+cfg.storage_account+'\nRegion: '+cfg.region+'\n\nProceed?')
    : ('This will use EXISTING Azure resources and only configure Unity Catalog:\n'
       +'  • Storage Credential, External Locations, Catalogs, Volume\n\n'
       +'Storage: '+cfg.storage_account+'\nAccess Connector: '+(cfg.access_connector||'(from storage credential)')
       +'\n\nProceed?');
  if(!(await uiConfirm(confirmMsg,{title:infraMode==='create'?'Create Azure Infrastructure':'Configure Unity Catalog',okLabel:'Proceed'}))) return;

  // Show progress panel
  prog.style.display='block';
  stepsEl.innerHTML='';
  logsEl.textContent=(infraMode==='create'?'Connecting to Azure…\n':'Connecting to Databricks…\n');
  summaryEl.textContent='Running…';
  summaryEl.style.color='var(--amber)';
  btn.disabled=true; btn.textContent='Deploying…';

  // SSE URL — all config (including creds) is read from deployconfig.json on the server
  const sseUrl='/api/v1/deploy-infra-stream?mode='+encodeURIComponent(infraMode);

  const statusIcons={
    running:'<span style="color:var(--amber);font-weight:700;" class="cfg-spin">&#9881;</span>',
    success:'<span style="color:var(--green);font-weight:700;">&#10003;</span>',
    error:'<span style="color:var(--red);font-weight:700;">&#10007;</span>',
    skipped:'<span style="color:var(--t4);font-weight:700;">&#8722;</span>'
  };

  // Track rendered steps by step number
  const stepMap={};

  function renderStep(s){
    const id='cfgStep_'+s.step;
    const html='<div id="'+id+'" style="display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:var(--r-xs);border:1px solid var(--border);background:var(--surface-2);font-size:12px;margin-bottom:4px;">'+
      (statusIcons[s.status]||'')+'<span style="font-weight:600;flex:1;">Step '+s.step+': '+s.name+'</span>'+
      '<span style="font-size:11px;color:'+(s.status==='error'?'var(--red)':'var(--t3)')+';">'+s.message+'</span></div>';
    if(stepMap[s.step]){
      stepMap[s.step].outerHTML=html;
      stepMap[s.step]=G(id);
    } else {
      stepsEl.insertAdjacentHTML('beforeend',html);
      stepMap[s.step]=G(id);
    }
    // Append logs — colorize genuine failures red; "already exists"/DEBUG/INFO
    // lines (expected on idempotent re-runs) stay the default log color.
    if(s.logs) logsEl.insertAdjacentHTML('beforeend', _colorizeDeployLogs(s.logs));
  }

  function _escapeHtml(str){
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
  function _colorizeDeployLogs(text){
    return text.split('\n').map(line=>{
      if(!line) return '';
      const isBenign = /ALREADY_EXISTS|already exists|\[DEBUG\]|\[INFO\]/i.test(line);
      const isError = !isBenign && /\[ERROR\]|PERMISSION_DENIED|cannot import|No Azure credentials|Failed to|Traceback/i.test(line);
      const esc = _escapeHtml(line);
      return isError ? '<span style="color:var(--red);">'+esc+'</span>' : esc;
    }).join('\n')+'\n';
  }

  const evtSource=new EventSource(sseUrl);
  evtSource.onmessage=function(e){
    try{
      const d=JSON.parse(e.data);
      if(d.event==='step'){
        renderStep(d);
        logsEl.scrollTop=logsEl.scrollHeight;
      } else if(d.event==='done'){
        evtSource.close();
        summaryEl.textContent=d.summary||'';
        summaryEl.style.color=d.success?'var(--green)':'var(--red)';
        if(d.success){
          toast('Infrastructure deployed successfully!','tok',5000);
        } else {
          toast('Deployment completed with errors — check logs','terr',6000);
        }
        btn.disabled=false;
        btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Deploy Infrastructure';
      }
    }catch(ex){console.error('SSE parse error',ex);}
  };
  evtSource.onerror=function(){
    evtSource.close();
    summaryEl.textContent='Connection lost';
    summaryEl.style.color='var(--red)';
    toast('Deploy stream connection lost','terr');
    btn.disabled=false;
    btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Deploy Infrastructure';
  };
}

async function loadDeployConfig(){
  try{
    const r=await fetch('/api/v1/deploy-config');
    const d=await r.json();
    if(d.success&&d.config){
      _populateConfig(d.config);
      _cfgShowPreview();
    }
  }catch(e){ /* no saved config yet — ignore */ }
}

/* ── Secret Vault — shows what THIS app can actually read from the Databricks secret scope ── */
async function loadSecretVault(){
  const box=G('cfgSecretVaultRows');
  if(!box) return;
  box.innerHTML='<div style="font-size:11px;color:var(--t4);">Loading…</div>';
  try{
    const r=await fetch('/api/v1/settings/secrets');
    if(r.status===403){
      box.innerHTML='<div style="font-size:11px;color:var(--t4);">Admin role required to view/manage secrets.</div>';
      return;
    }
    const d=await r.json();
    if(!d.success){
      box.innerHTML='<div style="font-size:11px;color:#dc2626;">'+_esc(d.error||'Failed to load secret status')+'</div>';
      return;
    }
    if(G('cfgSecretScopeName')) G('cfgSecretScopeName').textContent=d.scope||'migration-studio';
    box.innerHTML=(d.keys||[]).map(k=>{
      const cfgBadge=k.configured
        ?'<span style="font-size:10px;padding:2px 8px;border-radius:10px;background:#d1fae5;color:#059669;font-weight:600;">Configured ✓</span>'
        :'<span style="font-size:10px;padding:2px 8px;border-radius:10px;background:#fee2e2;color:#dc2626;font-weight:600;">Not Set</span>';
      return '<div style="display:flex;align-items:center;gap:8px;padding:8px;border:1px solid var(--border);border-radius:8px;">'
        +'<div style="flex:0 0 200px;"><div style="font-size:11px;font-weight:700;color:var(--t1);">'+_esc(k.key)+'</div>'
        +'<div style="font-size:9px;color:var(--t4);">'+_esc(k.description||'')+'</div></div>'
        +cfgBadge
        +'<input class="inp" type="password" placeholder="set new value…" id="cfgSecretIn_'+_esc(k.key)+'" style="flex:1;height:32px;">'
        +'<button class="btn btn-primary btn-xs" style="height:32px;" onclick="saveSecretVaultItem('+JSON.stringify(k.key)+')">Save</button>'
        +'</div>';
    }).join('')||'<div style="font-size:11px;color:var(--t4);">No known secret keys.</div>';
  }catch(e){
    box.innerHTML='<div style="font-size:11px;color:#dc2626;">'+_esc(e.message)+'</div>';
  }
}

async function saveSecretVaultItem(key){
  const input=G('cfgSecretIn_'+key);
  const value=input?input.value:'';
  if(!value){toast('Enter a value first','terr');return;}
  try{
    const r=await fetch('/api/v1/settings/secrets',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({key,value})});
    const d=await r.json();
    if(d.success){
      toast('\''+key+'\' updated in the Databricks secret scope.','tok');
      if(input) input.value='';
      loadSecretVault();
    }else{
      toast(d.error||'Failed to save secret','terr');
    }
  }catch(e){
    toast(e.message,'terr');
  }
}

// ── Startup self-check ──
(function(){
  const fns=['testSourceConn','loadFromSource','convertSelected','switchTab','toast','toggleSrcConn'];
  const missing=fns.filter(f=>typeof window[f]!=='function');
  if(missing.length){
    const d=document.createElement('div');
    d.style.cssText='position:fixed;top:8px;left:50%;transform:translateX(-50%);z-index:99999;background:#fef3c7;color:#92400e;border:2px solid #f59e0b;padding:12px 18px;border-radius:8px;font:13px/1.4 monospace;max-width:80vw;box-shadow:0 8px 32px rgba(0,0,0,.2);';
    d.textContent='MISSING FUNCTIONS: '+missing.join(', ');
    document.body.appendChild(d);
  }else{
    console.log('[Migration Studio] All '+fns.length+' core functions loaded OK');
  }
})();

// ── Auto-init from deployconfig.json on page load ──
(async function _autoInitFromConfig(){
  try{
    const r=await fetch('/api/v1/deploy-config');
    const d=await r.json();
    if(!d.success||!d.config) return;
    // Populate all UI fields from saved config
    _populateConfig(d.config);
    // If we have Databricks credentials, silently initialize MetadataFlow
    const host=d.config.databricks_host||'';
    const token=d.config.databricks_token||'';
    const metaCat=d.config.metadata_catalog||'';
    const metaSch=d.config.metadata_schema||'';
    if(host&&token&&metaCat&&metaSch){
      const ir=await fetch('/api/v1/workflow/auto-init',{method:'POST',headers:{'Content-Type':'application/json'}});
      const id=await ir.json();
      if(id.success){
        _wfMetaReady=true;
        console.log('[Auto-Init] MetadataFlow initialized:',metaCat+'.'+metaSch);
        // Refresh pipeline list now that metadata is loaded
        if(typeof wfRefreshPipelines==='function') wfRefreshPipelines();
      }
    }
    // Check notebook deployment status
    try{
      const nbr=await fetch('/api/v1/workflow/notebooks/status');const nbd=await nbr.json();
      if(nbd.deployed) _wfNbDeployed=true;
    }catch(e){}
    // Auto-fetch clusters if credentials available
    if(host&&token&&!_wfClustersLoaded){
      try{await wfFetchClusters();}catch(e){}
    }
  }catch(e){console.log('[Auto-Init] No saved config:',e);}
})();

// ── Restore last visited tab from URL hash ──
(function(){
  const hash=location.hash.replace('#','');
  const validTabs=Object.keys(TAB_META);
  if(hash&&validTabs.includes(hash)){
    switchTab(hash,G('nav-'+hash));
  }else{
    switchTab('wf-dashboard',G('nav-wf-dashboard'));
  }
  // Auto-populate source connection from deployconfig.json
  _srcSyncFromConfig();
  // Auto-populate Databricks credentials from deployconfig.json
  _dbrSyncFromConfig();
})();

// ═══════════════════════════════════════════════════════════════════════════════
//  DATA MODELING — Star / Snowflake Schema Builder
// ═══════════════════════════════════════════════════════════════════════════════

let _dmModel = null;    // current model from backend
let _dmModelId = null;  // cache key
let _dmErJson = null;   // ER nodes/edges
let _dmDdl = '';        // DDL text
let _dmZoomLevel = 1;
let _dmPanX = 0, _dmPanY = 0;
var _dmSavedPositions = {};  // Preserve node positions across re-renders
let _dmCatalogSchemas = [];
let _dmAllTables = [];      // Full list — [{table, catalog, schema, fqn}] or legacy strings
let _dmSelectedTables = new Map();  // fqn → {table, catalog, schema}
let _dmTplVisible = true;
let _dmInsightsVisible = true;
let _dmMultiMode = false;
let _dmSources = [];        // [{catalog, schema}] for multi-source mode
let _dmNotation = 'crowsfoot'; // 'arrow' or 'crowsfoot'
let _dmDetectedChanges = [];
let _dmSuggestions = [];

// ── Role hints (pure client-side heuristic, no backend call) ───────────────
const _DM_FACT_RX = /(transaction|order|sale|invoice|payment|event|log|detail|line_?item|entry|fact|history|audit|session)/i;
const _DM_DIM_RX  = /(customer|employee|product|department|location|region|store|category|status|type|dim|lookup|geo|channel|vendor|supplier|account|currency|calendar|date)/i;
function _dmRoleHint(name){
  const n=(name||'').toLowerCase();
  if(_DM_FACT_RX.test(n)) return {role:'fact',color:'#3B82F6',label:'FACT-LIKELY'};
  if(_DM_DIM_RX.test(n))  return {role:'dim', color:'#10B981',label:'DIM-LIKELY'};
  return {role:'?',color:'#94A3B8',label:'?'};
}

// Load catalog/schema dropdown
async function dmInit(){
  try{
    const r=await fetch('/api/v1/datamodel/catalogs-schemas');
    const d=await r.json();
    if(d.success){
      _dmCatalogSchemas=d.catalog_schemas||[];
      const sel=G('dmCatalog');
      sel.innerHTML='<option value="">— Select catalog —</option>';
      const cats=[...new Set(_dmCatalogSchemas.map(c=>c.catalog))];
      cats.forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c;sel.appendChild(o);});
    }
  }catch(e){console.error('dmInit',e);}
  _dmRefreshRecentList();
  _dmInstallShortcuts();
}

function dmOnCatalogChange(){
  const cat=G('dmCatalog').value;
  const sel=G('dmSchema');
  sel.innerHTML='<option value="">— Select schema —</option>';
  if(!cat)return;
  _dmCatalogSchemas.filter(c=>c.catalog===cat).forEach(cs=>{
    const o=document.createElement('option');o.value=cs.schema;o.textContent=cs.schema;sel.appendChild(o);
  });
  if(!_dmMultiMode){_dmAllTables=[];_dmSelectedTables.clear();_dmRenderTableList();}
}

// ── Multi-Source Mode ──────────────────────────────────────────────────────
function dmToggleMultiMode(){
  _dmMultiMode=!_dmMultiMode;
  G('dmMultiSourcePanel').style.display=_dmMultiMode?'':'none';
  G('dmMultiModeBtn').textContent=_dmMultiMode?'✓ Multi-Source Active':'Multi-Source Mode';
  G('dmMultiModeBtn').style.background=_dmMultiMode?'rgba(139,92,246,.1)':'';
  if(_dmMultiMode && _dmSources.length===0){
    const cat=G('dmCatalog').value, sch=G('dmSchema').value;
    if(cat&&sch) dmAddSource();
  }
}

function dmAddSource(){
  const cat=G('dmCatalog').value, sch=G('dmSchema').value;
  if(!cat||!sch){toast('Select catalog and schema first','terr');return;}
  if(_dmSources.some(s=>s.catalog===cat&&s.schema===sch)){toast('Source already added','terr');return;}
  _dmSources.push({catalog:cat,schema:sch});
  _dmRenderSourceTags();
  if(!_dmMultiMode){ _dmMultiMode=true; G('dmMultiSourcePanel').style.display=''; G('dmMultiModeBtn').textContent='✓ Multi-Source Active'; G('dmMultiModeBtn').style.background='rgba(139,92,246,.1)'; }
  else toast(`Added ${cat}.${sch}`,'tok');
}

function dmRemoveSource(idx){ _dmSources.splice(idx,1); _dmRenderSourceTags(); }
function dmClearSources(){ _dmSources=[]; _dmRenderSourceTags(); _dmAllTables=[]; _dmSelectedTables.clear(); _dmRenderTableList(); }

function _dmRenderSourceTags(){
  const el=G('dmSourceTags');
  if(!_dmSources.length){
    el.innerHTML='<span style="font-size:10px;color:var(--t4);padding:4px;">No sources added. Use dropdowns above to add catalog.schema pairs.</span>';
  } else {
    el.innerHTML=_dmSources.map((s,i)=>
      '<span style="display:inline-flex;align-items:center;gap:4px;background:linear-gradient(135deg,rgba(139,92,246,.08),rgba(59,130,246,.06));border:1px solid rgba(139,92,246,.25);border-radius:16px;padding:3px 10px;font-size:10px;font-weight:600;color:#6D28D9;">'+
      '📂 '+s.catalog+'.'+s.schema+
      '<button onclick="dmRemoveSource('+i+')" style="background:none;border:none;cursor:pointer;color:#EF4444;font-size:12px;padding:0 2px;line-height:1;">×</button></span>'
    ).join('');
  }
  G('dmMultiSourceCount').textContent=_dmSources.length+' source'+(_dmSources.length!==1?'s':'');
}

async function dmLoadTablesMulti(){
  // First, refresh catalogs/schemas from live Databricks
  try{
    const cr=await fetch('/api/v1/datamodel/catalogs-schemas');
    const cd=await cr.json();
    if(cd.success && cd.catalog_schemas && cd.catalog_schemas.length){
      _dmCatalogSchemas=cd.catalog_schemas;
      const sel=G('dmCatalog');
      const prevCat=sel.value;
      sel.innerHTML='<option value="">— Select catalog —</option>';
      const cats=[...new Set(_dmCatalogSchemas.map(c=>c.catalog))];
      cats.forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c;sel.appendChild(o);});
      if(prevCat && cats.includes(prevCat)){sel.value=prevCat; dmOnCatalogChange();}
      if(cd.source==='live') toast('Catalogs loaded from Databricks','tok');
    }
  }catch(e){console.warn('Catalog refresh failed',e);}

  if(_dmMultiMode && _dmSources.length>0){
    const box=G('dmTableList');
    box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">Loading tables from '+_dmSources.length+' sources…</div>';
    try{
      const r=await fetch('/api/v1/datamodel/tables-multi',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({selections:_dmSources})});
      const d=await r.json();
      if(d.success&&d.tables&&d.tables.length){
        _dmAllTables=d.tables;
        _dmSelectedTables.clear();
        _dmRenderTableList();
        toast(d.tables.length+' tables loaded from '+_dmSources.length+' sources','tok');
      }else{ _dmAllTables=[]; box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">No tables found</div>'; }
    }catch(e){box.innerHTML='<div style="padding:24px;text-align:center;color:#EF4444;font-size:11px;">Error loading tables</div>';toast('Failed to load tables','terr');}
  } else { dmLoadTables(); }
}

async function dmLoadTables(){
  const cat=G('dmCatalog').value, sch=G('dmSchema').value;
  if(!cat||!sch){toast('Select catalog and schema first','terr');return;}
  const box=G('dmTableList');
  box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">Loading tables…</div>';
  try{
    const r=await fetch('/api/v1/datamodel/tables',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:cat,schema:sch})});
    const d=await r.json();
    if(d.success&&d.tables&&d.tables.length){
      _dmAllTables=d.tables.map(t=>({table:t,catalog:cat,schema:sch,fqn:cat+'.'+sch+'.'+t}));
      _dmSelectedTables.clear();
      _dmRenderTableList();
    }else{
      _dmAllTables=[];
      box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">No tables found</div>';
    }
  }catch(e){box.innerHTML='<div style="padding:24px;text-align:center;color:#EF4444;font-size:11px;">Error loading tables</div>';toast('Failed to load tables','terr');}
}

// ── Render checkbox table list with role hints + multi-source grouping ────────
function _dmRenderTableList(){
  const box=G('dmTableList');
  const q=(G('dmTableSearch').value||'').toLowerCase().trim();
  const isMulti=_dmMultiMode && _dmSources.length>1;

  // Normalize tables to objects
  const tables=_dmAllTables.map(t=> typeof t==='string'?{table:t,catalog:G('dmCatalog').value||'',schema:G('dmSchema').value||'',fqn:(G('dmCatalog').value||'')+'.'+(G('dmSchema').value||'')+'.'+t}:t);
  const filtered=tables.filter(t=>!q || t.table.toLowerCase().includes(q) || (t.fqn||'').toLowerCase().includes(q));

  if(!tables.length){
    box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">Select catalog and schema to load tables\u2026</div>';
    _dmSyncHiddenSelect();_dmUpdateSelBadge();return;
  }
  if(!filtered.length){
    box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">No tables match "'+q+'"</div>';
    _dmSyncHiddenSelect();_dmUpdateSelBadge();return;
  }
  let html='';
  if(isMulti){
    const groups={};
    filtered.forEach(t=>{const key=t.catalog+'.'+t.schema;if(!groups[key])groups[key]=[];groups[key].push(t);});
    Object.entries(groups).forEach(([key,tbls])=>{
      html+='<div style="padding:4px 8px;font-size:10px;font-weight:700;color:#6D28D9;background:rgba(139,92,246,.05);border-radius:4px;margin:4px 0 2px;">\ud83d\udcc2 '+key+' <span style="color:var(--t4);font-weight:400;">('+tbls.length+')</span></div>';
      tbls.forEach(t=>{ html+=_dmTableRowHtml(t); });
    });
  } else {
    filtered.forEach(t=>{ html+=_dmTableRowHtml(t); });
  }
  box.innerHTML=html;
  _dmSyncHiddenSelect();_dmUpdateSelBadge();
}

function _dmTableRowHtml(t){
  const hint=_dmRoleHint(t.table);
  const checked=_dmSelectedTables.has(t.fqn);
  const fqnEsc=(t.fqn||'').replace(/"/g,'&quot;');
  return '<label class="dm-tbl-row" style="display:flex;align-items:center;gap:8px;padding:5px 8px;border-radius:6px;cursor:pointer;font-size:12px;transition:background .15s;'+
    (checked?'background:rgba(245,158,11,.08);':'')+'" '+
    'onmouseover="this.style.background=\'rgba(148,163,184,.08)\'" '+
    'onmouseout="this.style.background=\''+(checked?'rgba(245,158,11,.08)':'transparent')+'\'">'+
    '<input type="checkbox" data-fqn="'+fqnEsc+'" data-tname="'+(t.table||'').replace(/"/g,'&quot;')+'" data-cat="'+(t.catalog||'')+'" data-sch="'+(t.schema||'')+'" '+(checked?'checked':'')+' onchange="_dmOnTableToggle(this)" style="margin:0;cursor:pointer;">'+
    '<span style="flex:1;font-family:\'SF Mono\',Consolas,monospace;color:var(--t1);font-size:11px;">'+t.table+'</span>'+
    (_dmMultiMode?'<span style="font-size:9px;color:var(--t4);">'+t.catalog+'.'+t.schema+'</span>':'')+
    '<span style="font-size:9px;font-weight:700;padding:1px 6px;border-radius:8px;background:'+hint.color+'22;color:'+hint.color+';letter-spacing:.05em;">'+hint.label+'</span>'+
    '</label>';
}

function _dmOnTableToggle(el){
  const fqn=el.getAttribute('data-fqn');
  const tname=el.getAttribute('data-tname');
  const cat=el.getAttribute('data-cat');
  const sch=el.getAttribute('data-sch');
  if(el.checked) _dmSelectedTables.set(fqn,{table:tname,catalog:cat,schema:sch});
  else _dmSelectedTables.delete(fqn);
  _dmSyncHiddenSelect();_dmUpdateSelBadge();
  const row=el.closest('.dm-tbl-row');
  if(row) row.style.background = el.checked ? 'rgba(245,158,11,.08)' : 'transparent';
}

function _dmSyncHiddenSelect(){
  const sel=G('dmTableSelect');if(!sel)return;
  sel.innerHTML='';
  _dmSelectedTables.forEach((v,fqn)=>{
    const o=document.createElement('option');o.value=v.table;o.textContent=v.table;o.selected=true;sel.appendChild(o);
  });
}

function _dmUpdateSelBadge(){
  const b=G('dmSelBadge');if(!b)return;
  const n=_dmSelectedTables.size;
  if(n>0){b.style.display='';b.textContent=n+' selected';}
  else{b.style.display='none';}
}

function dmFilterTables(q){ _dmRenderTableList(); }
function dmSelectAll(){
  const q=(G('dmTableSearch').value||'').toLowerCase().trim();
  const tables=_dmAllTables.map(t=>typeof t==='string'?{table:t,catalog:G('dmCatalog').value||'',schema:G('dmSchema').value||'',fqn:(G('dmCatalog').value||'')+'.'+(G('dmSchema').value||'')+'.'+t}:t);
  tables.filter(t=>!q||t.table.toLowerCase().includes(q)||(t.fqn||'').toLowerCase().includes(q)).forEach(t=>_dmSelectedTables.set(t.fqn,{table:t.table,catalog:t.catalog,schema:t.schema}));
  _dmRenderTableList();
}
function dmSelectNone(){ _dmSelectedTables.clear(); _dmRenderTableList(); }
function dmInvertSelection(){
  const q=(G('dmTableSearch').value||'').toLowerCase().trim();
  const tables=_dmAllTables.map(t=>typeof t==='string'?{table:t,catalog:G('dmCatalog').value||'',schema:G('dmSchema').value||'',fqn:(G('dmCatalog').value||'')+'.'+(G('dmSchema').value||'')+'.'+t}:t);
  tables.filter(t=>!q||t.table.toLowerCase().includes(q)||(t.fqn||'').toLowerCase().includes(q)).forEach(t=>{
    if(_dmSelectedTables.has(t.fqn)) _dmSelectedTables.delete(t.fqn);
    else _dmSelectedTables.set(t.fqn,{table:t.table,catalog:t.catalog,schema:t.schema});
  });
  _dmRenderTableList();
}

// ── Template Gallery ───────────────────────────────────────────────────────
const _DM_TEMPLATES = {
  'sales-star':   {schema:'star',      match:/^(orders?|orderdetails?|customers?|products?|geolocations?|date_?dim)$/i},
  'hr-star':      {schema:'star',      match:/^(employees?|employeesessions?|departments?|date_?dim)$/i},
  'finance-snow': {schema:'snowflake', match:/^(transactions?|accounts?|currencies|customers?|products?|categor(y|ies))$/i},
  'blank':        {schema:'auto',      match:null},
};
function dmApplyTemplate(key){
  const tpl=_DM_TEMPLATES[key];if(!tpl){return;}
  G('dmSchemaChoice').value=tpl.schema;
  if(key==='blank'){
    _dmSelectedTables.clear();_dmRenderTableList();
    toast('Blank canvas — pick your tables below','tok');return;
  }
  // If no tables loaded yet, fall back to sample demo
  if(!_dmAllTables.length){
    dmLoadSample();
    toast('Template "'+key+'" will apply using sample data…','tok');
    return;
  }
  _dmSelectedTables.clear();
  const tables=_dmAllTables.map(t=>typeof t==='string'?{table:t,catalog:G('dmCatalog').value||'',schema:G('dmSchema').value||'',fqn:(G('dmCatalog').value||'')+'.'+(G('dmSchema').value||'')+'.'+t}:t);
  tables.forEach(t=>{ if(tpl.match.test(t.table)) _dmSelectedTables.set(t.fqn,{table:t.table,catalog:t.catalog,schema:t.schema}); });
  _dmRenderTableList();
  if(_dmSelectedTables.size){
    toast('Template applied \u2014 '+_dmSelectedTables.size+' matching tables selected. Click Generate.','tok');
  }else{
    toast('No matching tables in this schema. Try "Load Sample Data" to see the template.','terr');
  }
}
function dmToggleTemplateCard(){
  _dmTplVisible=!_dmTplVisible;
  G('dmTemplateGrid').style.display = _dmTplVisible?'grid':'none';
  G('dmTplToggle').textContent = _dmTplVisible?'Hide':'Show';
}

// ── Recent Models (localStorage) ───────────────────────────────────────────
const _DM_RECENT_KEY = 'dm_recent_models_v1';
const _DM_RECENT_MAX = 8;
function _dmReadRecent(){ try{ return JSON.parse(localStorage.getItem(_DM_RECENT_KEY)||'[]'); }catch(e){return [];} }
function _dmWriteRecent(list){ try{ localStorage.setItem(_DM_RECENT_KEY, JSON.stringify(list.slice(0,_DM_RECENT_MAX))); }catch(e){} }
function _dmRefreshRecentList(){
  const sel=G('dmRecentSelect');if(!sel)return;
  const list=_dmReadRecent();
  sel.innerHTML='<option value="">📂 Recent models…</option>';
  list.forEach((m,i)=>{const o=document.createElement('option');o.value=i;o.textContent=m.name+' · '+m.schema_type;sel.appendChild(o);});
}
function dmSaveCurrent(){
  if(!_dmModel){toast('Generate a model first','terr');return;}
  const name=prompt('Name for this model snapshot:', (G('dmCatalog').value||'model')+'_'+new Date().toISOString().slice(0,10));
  if(!name)return;
  const list=_dmReadRecent();
  const tableNames=[];
  _dmSelectedTables.forEach(v=>tableNames.push(v.table));
  list.unshift({
    name:name, ts:new Date().toISOString(),
    catalog:G('dmCatalog').value, schema:G('dmSchema').value,
    schema_choice:G('dmSchemaChoice').value,
    schema_type:_dmModel.schema_type,
    tables:tableNames,
  });
  _dmWriteRecent(list);_dmRefreshRecentList();
  toast('Saved "'+name+'" to Recent Models','tok');
}
function dmLoadRecent(idx){
  if(idx===''||idx==null)return;
  const list=_dmReadRecent();const m=list[parseInt(idx)];if(!m)return;
  if(m.catalog){G('dmCatalog').value=m.catalog;dmOnCatalogChange();}
  if(m.schema){G('dmSchema').value=m.schema;}
  if(m.schema_choice){G('dmSchemaChoice').value=m.schema_choice;}
  (async()=>{
    await dmLoadTables();
    _dmSelectedTables.clear();
    const cat=m.catalog||'', sch=m.schema||'';
    (m.tables||[]).forEach(t=>{
      const fqn=cat+'.'+sch+'.'+t;
      _dmSelectedTables.set(fqn,{table:t,catalog:cat,schema:sch});
    });
    _dmRenderTableList();
    toast('Loaded "'+m.name+'" \u2014 click Generate to rebuild','tok');
  })();
  G('dmRecentSelect').value='';
}

// ── Keyboard shortcuts ─────────────────────────────────────────────────────
function _dmInstallShortcuts(){
  if(window._dmKbdInstalled)return;window._dmKbdInstalled=true;
  document.addEventListener('keydown',e=>{
    // Only active on Data Modeling pane
    const pane=G('pane-wf-datamodel');if(!pane||!pane.classList.contains('active'))return;
    const tag=(e.target.tagName||'').toLowerCase();
    const inField=(tag==='input'||tag==='textarea'||tag==='select');
    // "/" → focus search
    if(!inField && e.key==='/' && !e.ctrlKey && !e.metaKey){e.preventDefault();const s=G('dmTableSearch');if(s)s.focus();}
    // "g" → generate
    else if(!inField && (e.key==='g'||e.key==='G') && !e.ctrlKey && !e.metaKey){e.preventDefault();dmGenerate();}
    // Ctrl+A in search → select all visible
    else if((e.ctrlKey||e.metaKey) && e.key==='a' && e.target.id==='dmTableSearch'){e.preventDefault();dmSelectAll();}
  });
}

// ── Insights (client-side derivation from generated model) ────────────────
function _dmDeriveInsights(d){
  const out=[];
  const facts=d.facts||[], dims=d.dimensions||[], rels=d.relationships||[];
  // 1. No date dimension detected
  const hasDate = dims.some(x=>/(date|calendar|time)/i.test(x.table_name));
  if(facts.length && !hasDate){
    out.push({icon:'📅',tone:'warn',title:'No date dimension detected',
      desc:'Facts typically need a Date/Calendar dim for time-based analytics. Consider adding one.',
      action:'dmShowAddTableDialog',actionLabel:'+ Add Date Dim'});
  }
  // 2. Fact table without measures
  facts.forEach(f=>{
    const measures=(f.columns||[]).filter(c=>/^(int|bigint|decimal|numeric|float|double|money)/i.test(c.data_type||''));
    if(measures.length<2){
      out.push({icon:'📊',tone:'warn',title:'Thin measures in '+f.table_name,
        desc:'Only '+measures.length+' numeric column(s). Fact tables usually carry 2+ measures (amount, qty, etc.).'});
    }
  });
  // 3. Suggested grain per fact
  facts.forEach(f=>{
    const pk=(f.columns||[]).find(c=>c.is_pk);
    const dt=(f.columns||[]).find(c=>/(date|time|_at$)/i.test(c.name));
    if(pk){
      out.push({icon:'🎯',tone:'info',title:'Grain for '+f.table_name,
        desc:'One row per <b>'+pk.name+'</b>'+(dt?' at <b>'+dt.name+'</b>':'')+'.'});
    }
  });
  // 4. SCD suggestions for dims
  dims.slice(0,5).forEach(dm=>{
    const hasAudit=(dm.columns||[]).some(c=>/(updated_at|modified|effective_date|valid_from)/i.test(c.name));
    out.push({icon:'🔁',tone:hasAudit?'ok':'info',
      title:'SCD for '+dm.table_name,
      desc:hasAudit?'Audit columns found \u2014 SCD Type 2 recommended (track history).':'No audit columns \u2014 SCD Type 1 (overwrite) is simplest.'});
  });
  // 5. Orphan dims (no relationship)
  const related=new Set(rels.flatMap(r=>[r.from,r.to]));
  dims.forEach(dm=>{
    if(!related.has(dm.table_name)){
      out.push({icon:'\u26a0\ufe0f',tone:'warn',title:dm.table_name+' has no relationships',
        desc:'This dimension is not connected to any fact. Use 🧠 Suggest Relations to find connections.',
        action:'dmSuggestRelationships',actionLabel:'🧠 Suggest'});
    }
  });
  // 6. Large fact count \u2014 bus matrix
  if(facts.length>=3){
    out.push({icon:'💡',tone:'info',title:'Multiple facts \u2014 consider a bus matrix',
      desc:facts.length+' fact tables detected. Conformed dimensions across facts will boost analytics.'});
  }
  // 7. Star schema recommendation
  if(d.schema_type==='snowflake' && dims.length<5){
    out.push({icon:'\u2b50',tone:'info',title:'Star may be simpler',
      desc:'Snowflake chosen but only '+dims.length+' dims. Star schema often performs better at this scale.'});
  }
  // 8. Wide fact table normalization suggestion
  facts.forEach(f=>{
    const cols=(f.columns||[]);
    if(cols.length>15){
      out.push({icon:'🛠\ufe0f',tone:'warn',title:'Wide fact: '+f.table_name+' ('+cols.length+' cols)',
        desc:'Consider normalizing. Move descriptive columns to dimensions for better query performance.'});
    }
  });
  // 9. Shared column detection (conformed dimension keys)
  const allColNames={};
  [...facts,...dims].forEach(t=>{
    (t.columns||[]).forEach(c=>{
      const k=c.name.toLowerCase();
      if(!allColNames[k])allColNames[k]=[];
      allColNames[k].push(t.table_name);
    });
  });
  const sharedCols=Object.entries(allColNames).filter(([k,tbls])=>tbls.length>2 && !/_id$|_key$|_date$|_at$/i.test(k));
  if(sharedCols.length>0){
    out.push({icon:'🔗',tone:'info',title:'Shared columns detected (conformed dims)',
      desc:'Columns like <b>'+sharedCols.slice(0,3).map(x=>x[0]).join(', ')+'</b> appear in '+sharedCols[0][1].length+'+ tables. These may be conformed dimension keys.'});
  }
  // 10. Nullable PK detection
  const badPks=[];
  [...facts,...dims].forEach(t=>{
    (t.columns||[]).forEach(c=>{
      if(c.is_pk && c.is_nullable) badPks.push(t.table_name+'.'+c.name);
    });
  });
  if(badPks.length>0){
    out.push({icon:'🚫',tone:'warn',title:'Nullable primary keys detected',
      desc:'PKs should be NOT NULL: <b>'+badPks.slice(0,3).join(', ')+'</b>'+(badPks.length>3?' (+'+(badPks.length-3)+' more)':'')+'.'});
  }
  // 11. Partition suggestion for facts with date columns
  facts.forEach(f=>{
    const dateCols=(f.columns||[]).filter(c=>/(date|time|_at$|_ts$)/i.test(c.name));
    if(dateCols.length>0){
      out.push({icon:'\u26a1',tone:'ok',title:'Partition '+f.table_name+' by date',
        desc:'Partition on <b>'+dateCols[0].name+'</b> for optimal Delta Lake query performance.'});
    }
  });
  // 12. Relationship density score
  const maxRels=facts.length*dims.length;
  const density=maxRels>0?Math.round(rels.length/maxRels*100):0;
  if(density<50 && rels.length>0 && maxRels>2){
    out.push({icon:'📶',tone:'info',title:'Relationship density: '+density+'%',
      desc:'Only '+rels.length+' of '+maxRels+' possible fact-dim connections exist. Use Suggest to find missing links.',
      action:'dmSuggestRelationships',actionLabel:'🧠 Suggest'});
  }
  // 13. Indexing suggestion for FK columns
  const fkCols=[];
  facts.forEach(f=>{
    (f.columns||[]).filter(c=>/_id$|_key$/i.test(c.name) && !c.is_pk).forEach(c=>{
      fkCols.push(f.table_name+'.'+c.name);
    });
  });
  if(fkCols.length>3){
    out.push({icon:'🔍',tone:'ok',title:'Consider Z-ORDER on FK columns',
      desc:'Z-ORDER BY on columns like <b>'+fkCols.slice(0,3).join(', ')+'</b> will speed up joins in Delta Lake.'});
  }
  return out;
}
function _dmRenderInsights(d){
  const panel=G('dmInsightsPanel'),list=G('dmInsightsList'),badge=G('dmInsightBadge');
  if(!panel||!list)return;
  const items=_dmDeriveInsights(d);
  if(!items.length){
    list.innerHTML='<div style="color:var(--t3);font-size:11px;padding:6px;">✔ No issues detected — model looks clean.</div>';
    badge.textContent='0 findings';return;
  }
  badge.textContent=items.length+' findings';
  const palette={warn:{bg:'rgba(245,158,11,.08)',bd:'rgba(245,158,11,.3)',c:'#B45309'},
                 info:{bg:'rgba(59,130,246,.06)',bd:'rgba(59,130,246,.25)',c:'#1D4ED8'},
                 ok:  {bg:'rgba(16,185,129,.06)',bd:'rgba(16,185,129,.25)',c:'#047857'}};
  list.innerHTML=items.map(it=>{
    const p=palette[it.tone]||palette.info;
    const actionHtml=it.action?'<button class="btn btn-ghost btn-xs" onclick="'+it.action+'()" style="font-size:9px;padding:2px 8px;border:1px solid '+p.c+'44;color:'+p.c+';border-radius:6px;margin-top:4px;">'+it.actionLabel+'</button>':'';
    return '<div style="background:'+p.bg+';border:1px solid '+p.bd+';border-radius:8px;padding:8px 10px;">'+
      '<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px;">'+
      '<span style="font-size:14px;">'+it.icon+'</span>'+
      '<span style="font-size:11px;font-weight:700;color:'+p.c+';">'+it.title+'</span></div>'+
      '<div style="font-size:10px;color:var(--t3);line-height:1.4;">'+it.desc+'</div>'+actionHtml+'</div>';
  }).join('');
}
function dmToggleInsights(){
  _dmInsightsVisible=!_dmInsightsVisible;
  G('dmInsightsList').style.display=_dmInsightsVisible?'grid':'none';
  G('dmInsightToggle').textContent=_dmInsightsVisible?'Hide':'Show';
}

async function dmGenerate(){
  const cat=G('dmCatalog').value, sch=G('dmSchema').value;
  const selections=[];
  _dmSelectedTables.forEach((v,fqn)=>{ selections.push({table:v.table,catalog:v.catalog,schema:v.schema}); });
  if(!selections.length){toast('Select at least one table','terr');return;}
  const schemaChoice=G('dmSchemaChoice').value;
  G('dmStatusMsg').textContent='Analyzing tables...';
  G('dmGenerateBtn').disabled=true;
  try{
    // Use multi-generate if multiple catalogs/schemas, else single
    const uniqueSources=new Set(selections.map(s=>s.catalog+'.'+s.schema));
    let r;
    if(uniqueSources.size>1){
      r=await fetch('/api/v1/datamodel/generate-multi',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({tables_selections:selections,schema_choice:schemaChoice})});
    } else {
      const tableNames=selections.map(s=>s.table);
      r=await fetch('/api/v1/datamodel/generate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:selections[0].catalog,schema:selections[0].schema,tables:tableNames,schema_choice:schemaChoice})});
    }
    const d=await r.json();
    if(d.success){
      _dmModel=d;_dmModelId=d.model_id;_dmErJson=d.er_json;_dmDdl=d.ddl;
      G('dmResultArea').style.display='';
      G('dmKpiTables').textContent=selections.length;
      G('dmKpiFacts').textContent=d.facts.length;
      G('dmKpiDims').textContent=d.dimensions.length;
      G('dmKpiSchema').textContent=d.schema_type==='star'?'\u2b50 Star':'\u2744 Snowflake';
      G('dmSchemaTypeBadge').textContent=d.schema_type.toUpperCase()+' SCHEMA';
      G('dmSchemaTypeBadge').style.background=d.schema_type==='star'?'rgba(245,158,11,.15)':'rgba(59,130,246,.15)';
      G('dmSchemaTypeBadge').style.color=d.schema_type==='star'?'#F59E0B':'#3B82F6';
      _dmSavedPositions={};
      dmRenderER(d.er_json);
      dmRenderDetails(d);
      _dmRenderInsights(d);
      G('dmDdlCode').textContent=d.ddl;
      G('dmStatusMsg').textContent='Model generated successfully!';
      toast('Data model generated \u2014 '+d.schema_type.toUpperCase()+' schema with '+d.facts.length+' facts & '+d.dimensions.length+' dims','tok');
    }else{
      toast(d.error||'Generation failed','terr');
      G('dmStatusMsg').textContent=d.error||'Failed';
    }
  }catch(e){toast('Error: '+e.message,'terr');G('dmStatusMsg').textContent='Error';}
  G('dmGenerateBtn').disabled=false;
}

// ── Load Sample / Demo Data (no Databricks needed) ──────────────────────────
async function dmLoadSample(){
  G('dmSampleBtn').disabled=true;
  G('dmStatusMsg').textContent='Loading sample data...';
  try{
    // 1. Fetch sample table list
    const lr=await fetch('/api/v1/datamodel/sample-tables');
    const ld=await lr.json();
    if(ld.success&&ld.tables){
      _dmAllTables=ld.tables.map(t=>({table:t,catalog:'sample_catalog',schema:'sample_schema',fqn:'sample_catalog.sample_schema.'+t}));
      _dmSelectedTables=new Map();
      ld.tables.forEach(t=>_dmSelectedTables.set('sample_catalog.sample_schema.'+t,{table:t,catalog:'sample_catalog',schema:'sample_schema'}));
      G('dmCatalog').innerHTML='<option value="sample_catalog" selected>sample_catalog</option>';
      G('dmSchema').innerHTML='<option value="sample_schema" selected>sample_schema</option>';
      _dmRenderTableList();
    }
    // 2. Auto-generate model
    const schemaChoice=G('dmSchemaChoice').value;
    const r=await fetch('/api/v1/datamodel/sample-generate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({tables:[],schema_choice:schemaChoice})});
    const d=await r.json();
    if(d.success){
      _dmModel=d;_dmModelId=d.model_id;_dmErJson=d.er_json;_dmDdl=d.ddl;
      G('dmResultArea').style.display='';
      const totalTables=d.facts.length+d.dimensions.length;
      G('dmKpiTables').textContent=totalTables;
      G('dmKpiFacts').textContent=d.facts.length;
      G('dmKpiDims').textContent=d.dimensions.length;
      G('dmKpiSchema').textContent=d.schema_type==='star'?'⭐ Star':'❄ Snowflake';
      G('dmSchemaTypeBadge').textContent=d.schema_type.toUpperCase()+' SCHEMA';
      G('dmSchemaTypeBadge').style.background=d.schema_type==='star'?'rgba(245,158,11,.15)':'rgba(59,130,246,.15)';
      G('dmSchemaTypeBadge').style.color=d.schema_type==='star'?'#F59E0B':'#3B82F6';
      _dmSavedPositions={};
      dmRenderER(d.er_json);
      dmRenderDetails(d);
      _dmRenderInsights(d);
      G('dmDdlCode').textContent=d.ddl;
      G('dmStatusMsg').textContent='Sample model generated!';
      toast('Sample data model generated — '+d.schema_type.toUpperCase()+' schema with '+d.facts.length+' facts & '+d.dimensions.length+' dims','tok');
    }else{
      toast(d.error||'Sample generation failed','terr');
      G('dmStatusMsg').textContent='Failed';
    }
  }catch(e){toast('Error: '+e.message,'terr');G('dmStatusMsg').textContent='Error';}
  G('dmSampleBtn').disabled=false;
}

// Sub-tab switching
function dmSwitchSubTab(tab,btn){
  document.querySelectorAll('.dmSubTab').forEach(b=>{b.classList.remove('active');b.style.borderBottom='2px solid transparent';b.style.fontWeight='';});
  btn.classList.add('active');btn.style.borderBottom='2px solid var(--accent-primary)';btn.style.fontWeight='700';
  G('dmSubER').style.display=tab==='er'?'':'none';
  G('dmSubDetails').style.display=tab==='details'?'':'none';
  G('dmSubDDL').style.display=tab==='ddl'?'':'none';
}

// ── ER Diagram Rendering (SVG) ─ Enhanced with Crow's Foot, Interactive Edges ───
function dmRenderER(er){
  const g=G('dmErGroup');
  g.innerHTML='';
  if(!er||!er.nodes) return;
  const svg=G('dmErSvg');
  _dmZoomLevel=1; _dmPanX=0; _dmPanY=0;
  g.setAttribute('transform','translate(0,0) scale(1)');

  // Restore saved positions from previous render
  if(typeof _dmSavedPositions==='object'){
    er.nodes.forEach(n=>{
      if(_dmSavedPositions[n.id]){
        n.x=_dmSavedPositions[n.id].x;
        n.y=_dmSavedPositions[n.id].y;
        n._autoLayout=false;
      }
    });
  }

  const facts=er.nodes.filter(n=>n.type==='fact');
  const dims=er.nodes.filter(n=>n.type==='dimension');
  const views=er.nodes.filter(n=>n.type==='view');
  const nodeW_layout=300, gapX=80, gapY=60, pad=40;
  // padTop accounts for the metadata panel height so nodes don't overlap
  const padTop=200;

  // Calculate actual height for a node
  function _nodeH(n){
    const cols=(n.columns||[]).length;
    const pkCols=(n.columns||[]).filter(c=>c.is_pk).length;
    const fkCols=(n.columns||[]).filter(c=>c.fk_table).length;
    const constraintLines=(pkCols?1:0)+fkCols;
    return 28+cols*22+6+(constraintLines?(constraintLines*20+12):0)+4;
  }

  // ── Smart Radial Layout: facts center, dims around in ring ──
  // Build adjacency: which dims connect to which facts
  const edges=er.edges||[];
  const factIds=new Set(facts.map(n=>n.id));
  const dimToFact={};
  edges.forEach(e=>{
    if(factIds.has(e.from)&&!factIds.has(e.to)) dimToFact[e.to]=e.from;
    if(factIds.has(e.to)&&!factIds.has(e.from)) dimToFact[e.from]=e.to;
  });

  // Calculate heights for all nodes
  const nodeHeights={};
  er.nodes.forEach(n=>{nodeHeights[n.id]=_nodeH(n);});

  // If only manual positions exist, skip auto layout
  const needsLayout=er.nodes.some(n=>n.x===undefined||n._autoLayout!==false);

  if(needsLayout){
    // Calculate fact bounding
    let factMaxH=0;
    facts.forEach(n=>{const h=nodeHeights[n.id];if(h>factMaxH)factMaxH=h;});

    // For a single fact with many dims: radial layout
    // For multiple facts: horizontal facts with dims distributed
    const totalNodes=er.nodes.length;
    const dimMaxH=Math.max(...dims.map(n=>nodeHeights[n.id]),200);

    if(facts.length<=2 && dims.length>=3){
      // ── RADIAL LAYOUT: Place fact(s) at center, dims in a ring ──
      const ringRadius=Math.max(400, dims.length*80);
      const centerX=ringRadius+nodeW_layout/2+pad;
      const centerY=ringRadius+factMaxH/2+padTop;

      // Place facts at center
      facts.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){
        n.x=centerX-nodeW_layout/2 + i*(nodeW_layout+gapX);
        n.y=centerY-factMaxH/2;
        n._autoLayout=true;
      }});

      // Place dims in a ring around the fact(s)
      const factCenterX=centerX + (facts.length-1)*(nodeW_layout+gapX)/2;
      dims.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){
        const angle=-Math.PI/2 + (2*Math.PI*i)/dims.length;
        const rx=ringRadius+nodeW_layout*0.2;
        const ry=ringRadius*0.7;
        n.x=factCenterX+Math.cos(angle)*rx - nodeW_layout/2;
        n.y=centerY+Math.sin(angle)*ry - nodeHeights[n.id]/2;
        n._autoLayout=true;
      }});

      // Place views outside the ring at bottom
      const viewY=centerY+ringRadius*0.7+dimMaxH/2+gapY;
      views.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){
        n.x=pad+i*(nodeW_layout+gapX);
        n.y=viewY;
        n._autoLayout=true;
      }});

    } else {
      // ── GRID LAYOUT: multiple facts or few dims ──
      const cols=Math.max(3, Math.ceil(Math.sqrt(totalNodes*1.8)));

      // Place facts first, centered
      const factRow=Math.floor(cols/2)-Math.floor(facts.length/2);
      facts.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){
        n.x=pad+(factRow+i)*(nodeW_layout+gapX);
        n.y=padTop;
        n._autoLayout=true;
      }});

      // Place dims in rows below
      const dimStartY=padTop+factMaxH+gapY;
      const dimCols=Math.max(2, Math.ceil(Math.sqrt(dims.length*2)));
      dims.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){
        const col=i%dimCols;
        const row=Math.floor(i/dimCols);
        const rowMaxH=dims.slice(row*dimCols, (row+1)*dimCols).reduce((m,d)=>Math.max(m,nodeHeights[d.id]),0);
        n.x=pad+col*(nodeW_layout+gapX);
        n.y=dimStartY+row*(rowMaxH+gapY);
        n._autoLayout=true;
      }});

      // Views at the bottom
      let bottomY=0;
      er.nodes.forEach(n=>{const b=(n.y||0)+nodeHeights[n.id];if(b>bottomY)bottomY=b;});
      views.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){
        n.x=pad+i*(nodeW_layout+gapX);
        n.y=bottomY+gapY;
        n._autoLayout=true;
      }});
    }

    // ── Overlap resolution pass: push overlapping nodes apart ──
    for(let iter=0;iter<5;iter++){
      let moved=false;
      for(let i=0;i<er.nodes.length;i++){
        for(let j=i+1;j<er.nodes.length;j++){
          const a=er.nodes[i], b=er.nodes[j];
          const aW=nodeW_layout, aH=nodeHeights[a.id];
          const bW=nodeW_layout, bH=nodeHeights[b.id];
          const overlapX=(aW+gapX/2)-(Math.abs((a.x+aW/2)-(b.x+bW/2)));
          const overlapY=((aH+bH)/2+gapY/2)-(Math.abs((a.y+aH/2)-(b.y+bH/2)));
          if(overlapX>0 && overlapY>0){
            // Push apart in the direction of least overlap
            if(overlapX<overlapY){
              const shift=overlapX/2+gapX/4;
              if(a.x<b.x){a.x-=shift;b.x+=shift;}
              else{a.x+=shift;b.x-=shift;}
            } else {
              const shift=overlapY/2+gapY/4;
              if(a.y<b.y){a.y-=shift;b.y+=shift;}
              else{a.y+=shift;b.y-=shift;}
            }
            moved=true;
          }
        }
      }
      if(!moved) break;
    }

    // Ensure no negative positions and no overlap with metadata panels
    let minX=Infinity, minY=Infinity;
    er.nodes.forEach(n=>{if(n.x<minX)minX=n.x; if(n.y<minY)minY=n.y;});
    if(minX<pad||minY<padTop){
      const shiftX=minX<pad?pad-minX:0;
      const shiftY=minY<padTop?padTop-minY:0;
      er.nodes.forEach(n=>{n.x+=shiftX; n.y+=shiftY;});
    }
  }

  // Compute total dimensions and resize SVG
  let totalW=0, totalH=0;
  er.nodes.forEach(n=>{
    const r=(n.x||0)+nodeW_layout+pad;
    const b=(n.y||0)+nodeHeights[n.id]+pad;
    if(r>totalW)totalW=r;
    if(b>totalH)totalH=b;
  });
  const W=Math.max(totalW, svg.clientWidth||1400);
  const H=Math.max(totalH, 900);
  svg.setAttribute('height', H);
  svg.setAttribute('width', W);

  // Draw edges first (behind nodes)
  _dmDrawEdges(er, g);

  // ── Metadata Info Panel (SQL Developer style) ──
  _dmRenderMetadataPanel(er, g);

  // Draw nodes — SQL Developer Data Modeler style
  const nodeW=300;
  er.nodes.forEach(n=>{
    const isFact=n.type==='fact';
    const isView=n.type==='view';
    const ng=document.createElementNS('http://www.w3.org/2000/svg','g');
    ng.setAttribute('transform','translate('+n.x+','+n.y+')');
    ng.setAttribute('data-node-id', n.id);
    ng.style.cursor='move';

    const cols=n.columns||[];
    const fkConstraints=cols.filter(c=>c.fk_table).map(c=>({col:c.name,ref:c.fk_table}));
    const pkCols=cols.filter(c=>c.is_pk);

    const rowH=22;
    const hdrH=28;
    const colSectionH=cols.length*rowH+6;
    const constraintLines=[];
    if(pkCols.length)constraintLines.push({icon:'P>',text:n.label+'_PK ('+pkCols.map(c=>c.name).join(', ')+')',color:'#7C3AED'});
    fkConstraints.forEach(fk=>{constraintLines.push({icon:'F>',text:n.label+'_'+fk.ref+'_FK ('+fk.col+')',color:'#059669'});});
    const constraintSectionH=constraintLines.length?(constraintLines.length*20+12):0;
    const boxH=hdrH+colSectionH+constraintSectionH+4;

    // Drop shadow
    const shadow=document.createElementNS('http://www.w3.org/2000/svg','rect');
    shadow.setAttribute('x','2');shadow.setAttribute('y','2');
    shadow.setAttribute('width',nodeW);shadow.setAttribute('height',boxH);
    shadow.setAttribute('rx','2');shadow.setAttribute('fill','rgba(0,0,0,0.08)');
    ng.appendChild(shadow);

    // Main box — white with colored left border (SQL Developer style)
    const borderColor=isView?'#D97706':isFact?'#BE185D':'#047857';
    const rect=document.createElementNS('http://www.w3.org/2000/svg','rect');
    rect.setAttribute('width',nodeW);rect.setAttribute('height',boxH);rect.setAttribute('rx','2');
    rect.setAttribute('fill','#FFFFFF');
    rect.setAttribute('stroke',borderColor);rect.setAttribute('stroke-width','1.5');
    ng.appendChild(rect);

    // Header background (SQL Developer: teal for dims, pink for facts, amber for views)
    const hdrBg=isView?'#FEF3C7':isFact?'#FCE7F3':'#D1FAE5';
    const hdr=document.createElementNS('http://www.w3.org/2000/svg','rect');
    hdr.setAttribute('width',nodeW);hdr.setAttribute('height',hdrH);hdr.setAttribute('rx','2');
    hdr.setAttribute('fill',hdrBg);
    ng.appendChild(hdr);
    const hdr2=document.createElementNS('http://www.w3.org/2000/svg','rect');
    hdr2.setAttribute('y',String(hdrH-2));hdr2.setAttribute('width',nodeW);hdr2.setAttribute('height','2');
    hdr2.setAttribute('fill',hdrBg);ng.appendChild(hdr2);

    // Header separator line
    const hdrLine=document.createElementNS('http://www.w3.org/2000/svg','line');
    hdrLine.setAttribute('x1','0');hdrLine.setAttribute('y1',hdrH);hdrLine.setAttribute('x2',nodeW);hdrLine.setAttribute('y2',hdrH);
    hdrLine.setAttribute('stroke',borderColor);hdrLine.setAttribute('stroke-width','1');
    ng.appendChild(hdrLine);

    // Role badge (D/F/V)
    const roleBadge=document.createElementNS('http://www.w3.org/2000/svg','text');
    roleBadge.setAttribute('x','10');roleBadge.setAttribute('y',String(hdrH-8));
    roleBadge.setAttribute('fill',borderColor);roleBadge.setAttribute('font-size','13');
    roleBadge.setAttribute('font-weight','900');roleBadge.setAttribute('font-family','Consolas, monospace');
    roleBadge.textContent=isView?'V':isFact?'F':'D';
    ng.appendChild(roleBadge);

    // Table name
    const title=document.createElementNS('http://www.w3.org/2000/svg','text');
    title.setAttribute('x','28');title.setAttribute('y',String(hdrH-8));title.setAttribute('text-anchor','start');
    title.setAttribute('fill','#1E293B');title.setAttribute('font-size','13');title.setAttribute('font-weight','700');
    title.setAttribute('font-family','Segoe UI, system-ui, sans-serif');
    title.textContent=n.label;ng.appendChild(title);

    // Edit dropdown icon (top right)
    const editIcon=document.createElementNS('http://www.w3.org/2000/svg','text');
    editIcon.setAttribute('x',String(nodeW-12));editIcon.setAttribute('y',String(hdrH-8));editIcon.setAttribute('text-anchor','end');
    editIcon.setAttribute('fill',borderColor);editIcon.setAttribute('font-size','12');
    editIcon.setAttribute('cursor','pointer');editIcon.textContent='\u25BC';
    editIcon.addEventListener('click',ev=>{ev.stopPropagation();dmOpenTableEditor(n);});
    ng.appendChild(editIcon);

    // Columns section — SQL Developer style with P/F/* indicators
    cols.forEach((c,ci)=>{
      const cy2=hdrH+6+ci*rowH;

      // Key indicators: P=PK, F=FK, *=NOT NULL, U=Unique
      let keyStr='';
      if(c.is_pk)keyStr+='P';
      if(c.fk_table)keyStr+='F';
      if(!c.is_nullable&&!c.is_pk)keyStr+='*';
      if(c.is_unique)keyStr+='U';

      // Key indicator text
      const keyTxt=document.createElementNS('http://www.w3.org/2000/svg','text');
      keyTxt.setAttribute('x','8');keyTxt.setAttribute('y',String(cy2+15));
      keyTxt.setAttribute('fill',c.is_pk?'#7C3AED':c.fk_table?'#059669':'#64748B');
      keyTxt.setAttribute('font-size','12');keyTxt.setAttribute('font-family','Consolas, monospace');
      keyTxt.setAttribute('font-weight','700');
      keyTxt.textContent=keyStr;
      ng.appendChild(keyTxt);

      // NOT NULL red asterisk
      if(!c.is_nullable){
        const mand=document.createElementNS('http://www.w3.org/2000/svg','text');
        mand.setAttribute('x','32');mand.setAttribute('y',String(cy2+15));
        mand.setAttribute('fill','#DC2626');mand.setAttribute('font-size','13');
        mand.setAttribute('font-family','Consolas, monospace');
        mand.textContent='*';
        ng.appendChild(mand);
      }

      // Column name
      const ct=document.createElementNS('http://www.w3.org/2000/svg','text');
      ct.setAttribute('x','42');ct.setAttribute('y',String(cy2+15));ct.setAttribute('fill','#1E293B');
      ct.setAttribute('font-size','12');ct.setAttribute('font-family','Consolas, monospace');
      ct.setAttribute('font-weight',c.is_pk?'700':'400');
      ct.textContent=c.name;
      ng.appendChild(ct);

      // Data type right-aligned (indigo color like SQL Developer)
      const tt=document.createElementNS('http://www.w3.org/2000/svg','text');
      tt.setAttribute('x',String(nodeW-10));tt.setAttribute('y',String(cy2+15));tt.setAttribute('text-anchor','end');
      tt.setAttribute('fill','#4F46E5');tt.setAttribute('font-size','11');tt.setAttribute('font-family','Consolas, monospace');
      tt.textContent=c.data_type||'VARCHAR';
      ng.appendChild(tt);

      // Comment tooltip
      if(c.comment){
        const ttip=document.createElementNS('http://www.w3.org/2000/svg','title');
        ttip.textContent=c.comment;
        ct.appendChild(ttip);
      }

      // Row separator (thin dotted line)
      if(ci<cols.length-1){
        const rowSep=document.createElementNS('http://www.w3.org/2000/svg','line');
        rowSep.setAttribute('x1','4');rowSep.setAttribute('y1',String(cy2+rowH-1));
        rowSep.setAttribute('x2',String(nodeW-4));rowSep.setAttribute('y2',String(cy2+rowH-1));
        rowSep.setAttribute('stroke','#E2E8F0');rowSep.setAttribute('stroke-width','0.5');
        ng.appendChild(rowSep);
      }
    });

    // Constraints section (PK/FK listed at bottom like SQL Developer)
    if(constraintLines.length){
      const cSepY=hdrH+colSectionH;
      const cSep=document.createElementNS('http://www.w3.org/2000/svg','line');
      cSep.setAttribute('x1','0');cSep.setAttribute('y1',String(cSepY));
      cSep.setAttribute('x2',nodeW);cSep.setAttribute('y2',String(cSepY));
      cSep.setAttribute('stroke',borderColor);cSep.setAttribute('stroke-width','0.8');
      cSep.setAttribute('stroke-dasharray','3,2');
      ng.appendChild(cSep);

      constraintLines.forEach((cl,cli)=>{
        const clY=cSepY+6+cli*20;
        const clIcon=document.createElementNS('http://www.w3.org/2000/svg','text');
        clIcon.setAttribute('x','8');clIcon.setAttribute('y',String(clY+14));
        clIcon.setAttribute('fill',cl.color);clIcon.setAttribute('font-size','11');
        clIcon.setAttribute('font-family','Consolas, monospace');clIcon.setAttribute('font-weight','700');
        clIcon.textContent=cl.icon;
        ng.appendChild(clIcon);

        const clTxt=document.createElementNS('http://www.w3.org/2000/svg','text');
        clTxt.setAttribute('x','28');clTxt.setAttribute('y',String(clY+14));
        clTxt.setAttribute('fill',cl.color);clTxt.setAttribute('font-size','11');
        clTxt.setAttribute('font-family','Consolas, monospace');
        clTxt.textContent=cl.text.length>38?cl.text.substring(0,38)+'...':cl.text;
        ng.appendChild(clTxt);
      });
    }

    // Drag support
    let dragging=false, ddx=0, ddy=0;
    ng.addEventListener('mousedown',ev=>{
      if(ev.button!==0)return;
      if(ev.target===editIcon)return;
      dragging=true;ddx=ev.clientX/_dmZoomLevel-n.x;ddy=ev.clientY/_dmZoomLevel-n.y;
      ev.preventDefault();ev.stopPropagation();
      svg.style.cursor='grabbing';
      n._autoLayout=false;
    });
    const onMove=ev=>{
      if(!dragging)return;
      let newX=ev.clientX/_dmZoomLevel-ddx;
      let newY=ev.clientY/_dmZoomLevel-ddy;
      // Constrain: don't let node go to extreme negatives (keep within reasonable bounds)
      if(newX<-200) newX=-200;
      if(newY<-100) newY=-100;
      n.x=newX;
      n.y=newY;
      _dmSavedPositions[n.name]={x:n.x,y:n.y};
      ng.setAttribute('transform','translate('+n.x+','+n.y+')');
      _dmDrawEdges(er, g);
      _dmResizeSvg();
    };
    const onUp=()=>{if(dragging){dragging=false;svg.style.cursor='grab';}};
    svg.addEventListener('mousemove',onMove);
    svg.addEventListener('mouseup',onUp);
    svg.addEventListener('mouseleave',onUp);

    // Double-click to open editor
    ng.addEventListener('dblclick',ev=>{ev.stopPropagation();dmOpenTableEditor(n);});

    // Right-click context menu
    ng.addEventListener('contextmenu',ev=>{
      ev.preventDefault();ev.stopPropagation();
      _dmShowNodeContextMenu(n, ev);
    });

    g.appendChild(ng);
  });

  // Scroll-to-zoom on SVG (centered on cursor)
  svg.onwheel=ev=>{
    ev.preventDefault();
    const factor=ev.deltaY<0?1.1:0.9;
    const newZoom=Math.max(0.15,Math.min(3,_dmZoomLevel*factor));
    // Zoom towards cursor position within container
    const container=G('dmErContainer');
    const rect=container.getBoundingClientRect();
    const mx=ev.clientX-rect.left+container.scrollLeft;
    const my=ev.clientY-rect.top+container.scrollTop;
    _dmPanX=mx-(mx-_dmPanX)*(newZoom/_dmZoomLevel);
    _dmPanY=my-(my-_dmPanY)*(newZoom/_dmZoomLevel);
    _dmZoomLevel=newZoom;
    // Resize SVG to accommodate zoomed content
    _dmResizeSvg();
    g.setAttribute('transform','translate('+_dmPanX+','+_dmPanY+') scale('+_dmZoomLevel+')');
  };

  // Pan on SVG background drag
  let _svgDragging=false, _sx=0, _sy=0;
  svg.addEventListener('mousedown',ev=>{
    if(ev.target===svg||ev.target.tagName==='svg'){
      _svgDragging=true;_sx=ev.clientX-_dmPanX;_sy=ev.clientY-_dmPanY;svg.style.cursor='grabbing';
    }
  });
  svg.addEventListener('mousemove',ev=>{
    if(!_svgDragging)return;
    _dmPanX=ev.clientX-_sx;_dmPanY=ev.clientY-_sy;
    _dmResizeSvg();
    g.setAttribute('transform','translate('+_dmPanX+','+_dmPanY+') scale('+_dmZoomLevel+')');
  });
  svg.addEventListener('mouseup',()=>{_svgDragging=false;svg.style.cursor='grab';});
  svg.addEventListener('mouseleave',()=>{_svgDragging=false;svg.style.cursor='grab';});

  // Auto-fit to viewport after rendering
  setTimeout(()=>{dmFitView();_dmInitTopScroll();_dmSyncTopScroll();}, 80);
}

// ═══════════════════════════════════════════════════════════════════════════════
// INTERACTIVE TABLE EDITOR — Opens when clicking pencil icon or double-click
// ═══════════════════════════════════════════════════════════════════════════════
function dmOpenTableEditor(node, mode){
  // Find table/view data in model
  const isView=node.type==='view';
  let tblData=null;
  if(isView){
    tblData=(_dmModel.views||[]).find(v=>v.view_name===node.id);
  } else {
    tblData=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].find(t=>t.table_name===node.id);
  }
  if(!tblData&&!isView)return;

  // Remove existing editor if open
  const existing=document.getElementById('dmTableEditorPanel');
  if(existing)existing.remove();

  const panel=document.createElement('div');
  panel.id='dmTableEditorPanel';
  panel.style.cssText='position:fixed;top:60px;right:10px;width:460px;max-height:calc(100vh - 80px);overflow-y:auto;background:var(--bg1,#fff);border:2px solid '+(isView?'#F59E0B':node.type==='fact'?'#3B82F6':'#10B981')+';border-radius:14px;padding:18px;box-shadow:0 20px 60px rgba(0,0,0,.25);z-index:9999;';

  const hdrColor=isView?'#F59E0B':node.type==='fact'?'#3B82F6':'#10B981';
  const tableName=isView?tblData.view_name:tblData.table_name;
  const cols=isView?(tblData.columns||[]):(tblData.columns||[]);

  let html='<div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">';
  html+='<span style="font-size:16px;font-weight:800;color:'+hdrColor+';">'+(isView?'\ud83d\udc41 View':'\ud83d\udcdd Table')+': '+tableName+'</span>';
  html+='<div style="margin-left:auto;display:flex;gap:6px;">';
  if(!isView){
    html+='<button class="btn btn-ghost btn-xs" onclick="dmERAddRelFrom(\''+tableName.replace(/'/g,"\\'")+'\')">\ud83d\udd17 Add FK</button>';
  }
  html+='<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\'dmTableEditorPanel\').remove()" style="font-size:14px;">\u2715</button>';
  html+='</div></div>';

  // Table comment
  const tblComment=tblData.comment||'';
  html+='<div style="margin-bottom:10px;"><label style="font-size:10px;font-weight:600;color:var(--t3);">Table Comment</label>';
  html+='<input class="inp" id="dmTblEdComment" value="'+_escAttr(tblComment)+'" placeholder="Describe this table..." style="font-size:11px;width:100%;" onchange="dmSaveTableComment(\''+_escJs(tableName)+'\',this.value)"></div>';

  if(isView){
    // View definition editor
    html+='<div style="margin-bottom:10px;"><label style="font-size:10px;font-weight:600;color:var(--t3);">View SQL Definition</label>';
    html+='<textarea class="inp" id="dmViewDefEditor" style="font-size:11px;width:100%;height:120px;font-family:monospace;" onchange="dmSaveViewDef(\''+_escJs(tableName)+'\',this.value)">'+_escHtml(tblData.definition||'')+'</textarea></div>';
  }

  // Columns table
  html+='<div style="font-size:11px;font-weight:700;margin-bottom:6px;display:flex;align-items:center;gap:8px;">';
  html+='<span>Columns ('+cols.length+')</span>';
  html+='<button class="btn btn-ghost btn-xs" onclick="dmERAddColumn(\''+_escJs(tableName)+'\',\''+node.type+'\')">+ Add Column</button></div>';
  html+='<table style="width:100%;border-collapse:collapse;font-size:10px;">';
  html+='<thead><tr style="background:var(--bg2);"><th style="padding:4px;">Name</th><th style="padding:4px;">Type</th><th style="padding:4px;text-align:center;">PK</th><th style="padding:4px;text-align:center;">UQ</th><th style="padding:4px;text-align:center;">FK</th><th style="padding:4px;text-align:center;">Null</th><th style="padding:4px;">Comment</th><th style="padding:4px;"></th></tr></thead>';
  html+='<tbody>';
  cols.forEach((c,i)=>{
    const tn=_escJs(tableName), cn=_escJs(c.name);
    html+='<tr style="border-bottom:1px solid var(--border);">';
    html+='<td style="padding:3px;"><input class="inp" value="'+_escAttr(c.name)+'" style="font-size:10px;width:80px;font-family:monospace;" onchange="dmEREditCol(\''+tn+'\',\''+cn+'\',\'name\',this.value)"></td>';
    html+='<td style="padding:3px;"><select class="inp" style="font-size:10px;width:85px;" onchange="dmEREditCol(\''+tn+'\',\''+cn+'\',\'data_type\',this.value)">'+_dmTypeOptions(c.data_type)+'</select></td>';
    html+='<td style="padding:3px;text-align:center;"><input type="checkbox" '+(c.is_pk?'checked':'')+' onchange="dmEREditCol(\''+tn+'\',\''+cn+'\',\'is_pk\',this.checked)"></td>';
    html+='<td style="padding:3px;text-align:center;"><input type="checkbox" '+(c.is_unique?'checked':'')+' onchange="dmEREditCol(\''+tn+'\',\''+cn+'\',\'is_unique\',this.checked)"></td>';
    html+='<td style="padding:3px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmERSetFK(\''+tn+'\',\''+cn+'\',\''+_escJs(c.fk_table||'')+'\')">'+( c.fk_table? '\ud83d\udd17'+c.fk_table:'\u2014')+'</button></td>';
    html+='<td style="padding:3px;text-align:center;"><input type="checkbox" '+(c.is_nullable?'checked':'')+' onchange="dmEREditCol(\''+tn+'\',\''+cn+'\',\'is_nullable\',this.checked)"></td>';
    html+='<td style="padding:3px;"><input class="inp" value="'+_escAttr(c.comment||'')+'" placeholder="..." style="font-size:9px;width:70px;" onchange="dmEREditCol(\''+tn+'\',\''+cn+'\',\'comment\',this.value)"></td>';
    html+='<td style="padding:3px;"><button class="btn btn-ghost btn-xs" onclick="dmERDelCol(\''+tn+'\',\''+cn+'\')" style="color:#EF4444;font-size:10px;">\u2715</button></td>';
    html+='</tr>';
  });
  html+='</tbody></table>';

  panel.innerHTML=html;
  document.body.appendChild(panel);

  // Auto-focus add-column if mode requests it
  if(mode==='addcol'){setTimeout(()=>dmERAddColumn(tableName,node.type),100);}
}

// Helper for HTML entity escaping in editor
function _escAttr(s){return String(s||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');}
function _escHtml(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function _escJs(s){return String(s||'').replace(/\\/g,'\\\\').replace(/'/g,"\\'");}

function _dmTypeOptions(current){
  const types=['STRING','INT','BIGINT','SMALLINT','TINYINT','DOUBLE','FLOAT','DECIMAL(18,2)','DECIMAL(10,2)','BOOLEAN','DATE','TIMESTAMP','BINARY','ARRAY<STRING>','MAP<STRING,STRING>'];
  return types.map(t=>'<option value="'+t+'"'+(current&&current.toUpperCase()===t?' selected':'')+'>'+t+'</option>').join('');
}

// ═══════════════════════════════════════════════════════════════════════════════
// ER EDITOR API CALLS — Column edits from the editor panel
// ═══════════════════════════════════════════════════════════════════════════════
async function dmEREditCol(tableName, colName, field, value){
  await _dmEdit({column_edits:[{table_name:tableName, column_name:colName, field:field, value:value}]}, field+' updated');
  // Re-open editor on same node after refresh
  const node=(_dmErJson&&_dmErJson.nodes||[]).find(n=>n.id===tableName);
  if(node)setTimeout(()=>dmOpenTableEditor(node),200);
}

async function dmERDelCol(tableName, colName){
  if(!(await uiConfirm('Delete column "'+colName+'" from '+tableName+'?',{danger:true})))return;
  await _dmEdit({column_removes:[{table_name:tableName, column_name:colName}]}, 'Column deleted');
  const node=(_dmErJson&&_dmErJson.nodes||[]).find(n=>n.id===tableName);
  if(node)setTimeout(()=>dmOpenTableEditor(node),200);
}

function dmERAddColumn(tableName, nodeType){
  const panel=document.getElementById('dmTableEditorPanel');
  if(!panel)return;
  // Inject add-column inline form
  const existingAdd=document.getElementById('dmERAddColForm');
  if(existingAdd)existingAdd.remove();
  const div=document.createElement('div');
  div.id='dmERAddColForm';
  div.style.cssText='background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:8px;margin-top:8px;';
  div.innerHTML='<div style="font-size:10px;font-weight:700;margin-bottom:4px;">New Column</div>'+
    '<div style="display:flex;gap:4px;flex-wrap:wrap;align-items:end;">'+
    '<div><label style="font-size:9px;">Name</label><input class="inp" id="dmERNewColName" placeholder="column_name" style="font-size:10px;width:100px;"></div>'+
    '<div><label style="font-size:9px;">Type</label><select class="inp" id="dmERNewColType" style="font-size:10px;width:100px;">'+_dmTypeOptions('')+'</select></div>'+
    '<label style="font-size:9px;"><input type="checkbox" id="dmERNewColPK"> PK</label>'+
    '<label style="font-size:9px;"><input type="checkbox" id="dmERNewColUQ"> UQ</label>'+
    '<label style="font-size:9px;"><input type="checkbox" id="dmERNewColNull" checked> Null</label>'+
    '<div><label style="font-size:9px;">Comment</label><input class="inp" id="dmERNewColComment" placeholder="" style="font-size:9px;width:80px;"></div>'+
    '<button class="btn btn-primary btn-xs" onclick="dmERSaveNewCol(\''+_escJs(tableName)+'\')">Add</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\'dmERAddColForm\').remove()">Cancel</button>'+
    '</div>';
  panel.appendChild(div);
  document.getElementById('dmERNewColName').focus();
}

async function dmERSaveNewCol(tableName){
  const name=document.getElementById('dmERNewColName').value.trim();
  if(!name){toast('Column name required','terr');return;}
  const col={
    name:name,
    data_type:document.getElementById('dmERNewColType').value,
    is_pk:document.getElementById('dmERNewColPK').checked,
    is_unique:document.getElementById('dmERNewColUQ').checked,
    is_nullable:document.getElementById('dmERNewColNull').checked,
    comment:document.getElementById('dmERNewColComment').value.trim()
  };
  await _dmEdit({column_adds:[{table_name:tableName, column:col}]}, 'Column "'+name+'" added');
  const node=(_dmErJson&&_dmErJson.nodes||[]).find(n=>n.id===tableName);
  if(node)setTimeout(()=>dmOpenTableEditor(node),200);
}

async function dmSaveTableComment(tableName, comment){
  await _dmEdit({table_comments:[{table_name:tableName, comment:comment}]}, 'Comment saved');
}

async function dmSaveViewDef(viewName, definition){
  await _dmEdit({view_edits:[{view_name:viewName, definition:definition}]}, 'View definition saved');
}

// ═══════════════════════════════════════════════════════════════════════════════
// FK ASSIGNMENT — Set foreign key reference on a column
// ═══════════════════════════════════════════════════════════════════════════════
function dmERSetFK(tableName, colName, currentFK){
  if(!_dmModel)return;
  const allTables=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].map(t=>t.table_name).filter(t=>t!==tableName);
  const opts=allTables.map(t=>'<option value="'+t+'"'+(t===currentFK?' selected':'')+'>'+t+'</option>').join('');
  const div=document.createElement('div');
  div.id='dmFKSetterDlg';
  div.style.cssText='position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:99999;background:var(--bg1,#fff);border:2px solid #6366F1;border-radius:12px;padding:16px;width:320px;box-shadow:0 20px 60px rgba(0,0,0,.3);';
  div.innerHTML='<div style="font-weight:700;font-size:13px;margin-bottom:10px;color:#6366F1;">\ud83d\udd17 Set Foreign Key Reference</div>'+
    '<div style="font-size:11px;margin-bottom:8px;color:var(--t2);">Column: <b>'+tableName+'.'+colName+'</b></div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">References Table:</label>'+
    '<select class="inp" id="dmFKTarget" style="width:100%;font-size:11px;"><option value="">\u2014 None (remove FK) \u2014</option>'+opts+'</select></div>'+
    '<div style="display:flex;gap:6px;"><button class="btn btn-primary btn-xs" onclick="dmERSaveFK(\''+_escJs(tableName)+'\',\''+_escJs(colName)+'\')">Save</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\'dmFKSetterDlg\').remove()">Cancel</button></div>';
  const old=document.getElementById('dmFKSetterDlg');if(old)old.remove();
  document.body.appendChild(div);
}

async function dmERSaveFK(tableName, colName){
  const target=document.getElementById('dmFKTarget').value;
  document.getElementById('dmFKSetterDlg').remove();
  const edits={column_edits:[{table_name:tableName, column_name:colName, field:'fk_table', value:target}]};
  // Also add/update relationship if target is set
  if(target){
    edits.relationship_adds=[{from:tableName,to:target,type:'many-to-one',via_column:colName}];
  }
  await _dmEdit(edits, target?'FK set: '+colName+' -> '+target:'FK removed');
  const node=(_dmErJson&&_dmErJson.nodes||[]).find(n=>n.id===tableName);
  if(node)setTimeout(()=>dmOpenTableEditor(node),200);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ADD RELATIONSHIP FROM ER (from a specific table)
// ═══════════════════════════════════════════════════════════════════════════════
function dmERAddRelFrom(tableName){
  if(!_dmModel)return;
  const allTables=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].map(t=>t.table_name).filter(t=>t!==tableName);
  const fromCols=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].find(t=>t.table_name===tableName);
  const colOpts=(fromCols?.columns||[]).map(c=>'<option value="'+c.name+'">'+c.name+'</option>').join('');
  const toOpts=allTables.map(t=>'<option value="'+t+'">'+t+'</option>').join('');

  const div=document.createElement('div');
  div.id='dmAddRelFromDlg';
  div.style.cssText='position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:99999;background:var(--bg1,#fff);border:2px solid #10B981;border-radius:12px;padding:16px;width:380px;box-shadow:0 20px 60px rgba(0,0,0,.3);';
  div.innerHTML='<div style="font-weight:700;font-size:13px;margin-bottom:10px;color:#10B981;">\ud83d\udd17 Create Relationship</div>'+
    '<div style="font-size:11px;margin-bottom:8px;color:var(--t2);">From: <b>'+tableName+'</b></div>'+
    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;">'+
    '<div><label style="font-size:10px;font-weight:600;">FK Column</label><select class="inp" id="dmRelFKCol" style="width:100%;font-size:11px;">'+colOpts+'</select></div>'+
    '<div><label style="font-size:10px;font-weight:600;">To Table</label><select class="inp" id="dmRelToTbl" style="width:100%;font-size:11px;">'+toOpts+'</select></div>'+
    '</div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">Cardinality</label>'+
    '<select class="inp" id="dmRelType" style="width:100%;font-size:11px;"><option value="many-to-one">Many-to-One (*..1)</option><option value="one-to-many">One-to-Many (1..*)</option><option value="one-to-one">One-to-One (1..1)</option><option value="many-to-many">Many-to-Many (*..*)</option></select></div>'+
    '<div style="display:flex;gap:6px;"><button class="btn btn-primary btn-xs" onclick="dmERSaveRelFrom(\''+_escJs(tableName)+'\')">Create</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\'dmAddRelFromDlg\').remove()">Cancel</button></div>';
  const old=document.getElementById('dmAddRelFromDlg');if(old)old.remove();
  document.body.appendChild(div);
}

async function dmERSaveRelFrom(tableName){
  const toTbl=document.getElementById('dmRelToTbl').value;
  const fkCol=document.getElementById('dmRelFKCol').value;
  const relType=document.getElementById('dmRelType').value;
  document.getElementById('dmAddRelFromDlg').remove();
  if(!toTbl){toast('Select a target table','terr');return;}
  await _dmEdit({
    relationship_adds:[{from:tableName,to:toTbl,type:relType,via_column:fkCol}],
    column_edits:[{table_name:tableName,column_name:fkCol,field:'fk_table',value:toTbl}]
  }, 'Relationship created: '+tableName+' -> '+toTbl);
}

// ═══════════════════════════════════════════════════════════════════════════════
// NODE CONTEXT MENU (right-click)
// ═══════════════════════════════════════════════════════════════════════════════
function _dmShowNodeContextMenu(node, ev){
  const old=document.getElementById('dmNodeCtxMenu');if(old)old.remove();
  const menu=document.createElement('div');
  menu.id='dmNodeCtxMenu';
  menu.style.cssText='position:fixed;left:'+ev.clientX+'px;top:'+ev.clientY+'px;z-index:99999;background:var(--bg1,#fff);border:1px solid var(--border);border-radius:10px;padding:6px 0;box-shadow:0 8px 30px rgba(0,0,0,.2);min-width:180px;';
  const items=[
    {icon:'\u270E',label:'Edit Table',fn:()=>dmOpenTableEditor(node)},
    {icon:'\ud83d\udd17',label:'Add Relationship',fn:()=>dmERAddRelFrom(node.id)},
    {icon:'+',label:'Add Column',fn:()=>dmOpenTableEditor(node,'addcol')},
    {icon:'\u2b50',label:'Toggle PK (first col)',fn:()=>{const c=(node.columns||[])[0];if(c)dmEREditCol(node.id,c.name,'is_pk',!c.is_pk);}},
    {icon:'\ud83c\udfaf',label:'Set Unique Key',fn:()=>dmOpenTableEditor(node)},
    {icon:'\ud83d\udcac',label:'Add Comment',fn:()=>dmOpenTableEditor(node)},
    {icon:'\u2715',label:'Remove Table',fn:()=>dmRemoveTable(node.id),style:'color:#EF4444'},
  ];
  menu.innerHTML=items.map(it=>'<div style="padding:6px 14px;font-size:11px;cursor:pointer;display:flex;align-items:center;gap:8px;'+(it.style||'')+'" onmouseover="this.style.background=\'var(--bg2)\'" onmouseout="this.style.background=\'\'" onclick="this.parentElement.remove();('+_fnRef(it.fn)+')()"><span>'+it.icon+'</span><span>'+it.label+'</span></div>').join('');
  document.body.appendChild(menu);
  setTimeout(()=>{document.addEventListener('click',function _c(){menu.remove();document.removeEventListener('click',_c);},true);},50);
}

function _fnRef(fn){
  // Store fn reference and return a global callable
  if(!window._dmCtxFns)window._dmCtxFns=[];
  const idx=window._dmCtxFns.length;
  window._dmCtxFns.push(fn);
  return 'window._dmCtxFns['+idx+']';
}

// ═══════════════════════════════════════════════════════════════════════════════
// VIEWS — Load from Databricks / Create new
// ═══════════════════════════════════════════════════════════════════════════════
async function dmLoadViews(){
  const cat=G('dmCatalog').value, sch=G('dmSchema').value;
  if(!cat||!sch){toast('Select catalog & schema first','terr');return;}
  toast('Loading views...','tok');
  try{
    const r=await fetch('/api/v1/datamodel/views',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:cat,schema:sch})});
    const d=await r.json();
    if(d.success){
      const views=d.views||[];
      if(!views.length){toast('No views found in '+cat+'.'+sch,'tok');return;}
      // Add views to model
      const adds=views.map(v=>({view_name:v.view_name,definition:v.definition||'',comment:'',columns:[]}));
      await _dmEdit({view_adds:adds},'Loaded '+views.length+' view(s) from Databricks');
    }else{toast(d.error||'Failed to load views','terr');}
  }catch(e){toast('Error: '+e.message,'terr');}
}

function dmShowCreateViewDialog(){
  if(!_dmModel){toast('Generate a model first','terr');return;}
  const old=document.getElementById('dmCreateViewDlg');if(old)old.remove();
  const allTables=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].map(t=>t.table_name);
  const div=document.createElement('div');
  div.id='dmCreateViewDlg';
  div.style.cssText='position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:99999;background:var(--bg1,#fff);border:2px solid #F59E0B;border-radius:12px;padding:20px;width:480px;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.3);';
  div.innerHTML='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">'+
    '<span style="font-weight:700;font-size:14px;color:#F59E0B;">\ud83d\udc41 Create View</span>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\'dmCreateViewDlg\').remove()" style="font-size:14px;">\u2715</button></div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">View Name</label>'+
    '<input class="inp" id="dmNewViewName" placeholder="v_my_view" style="width:100%;font-size:11px;"></div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">SQL Definition</label>'+
    '<textarea class="inp" id="dmNewViewSQL" rows="6" style="width:100%;font-size:11px;font-family:monospace;" placeholder="SELECT col1, col2\nFROM table1\nJOIN table2 ON ..."></textarea></div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">Comment (optional)</label>'+
    '<input class="inp" id="dmNewViewComment" placeholder="Description..." style="width:100%;font-size:11px;"></div>'+
    '<div style="display:flex;gap:8px;">'+
    '<button class="btn btn-primary btn-sm" onclick="dmSaveNewView()" style="background:#F59E0B;border-color:#F59E0B;flex:1;">Create View</button>'+
    '<button class="btn btn-ghost btn-sm" onclick="document.getElementById(\'dmCreateViewDlg\').remove()">Cancel</button></div>';
  document.body.appendChild(div);
}

async function dmSaveNewView(){
  const name=document.getElementById('dmNewViewName').value.trim();
  const sql=document.getElementById('dmNewViewSQL').value.trim();
  const comment=document.getElementById('dmNewViewComment').value.trim();
  if(!name){toast('View name required','terr');return;}
  if(!sql){toast('SQL definition required','terr');return;}
  await _dmEdit({view_adds:[{view_name:name,definition:sql,comment:comment,columns:[]}]},'View "'+name+'" created');
  document.getElementById('dmCreateViewDlg').remove();
}

async function dmRemoveView(viewName){
  if(!(await uiConfirm('Remove view "'+viewName+'" from model?',{danger:true})))return;
  await _dmEdit({view_removes:[{view_name:viewName}]},'View removed');
}

// ═══════════════════════════════════════════════════════════════════════════════
// METADATA INFO PANEL — Editable model metadata (SQL Developer style)
// ═══════════════════════════════════════════════════════════════════════════════
function _dmRenderMetadataPanel(er, g){
  // Remove old metadata panels
  g.querySelectorAll('.dm-meta-panel').forEach(el=>el.remove());

  const meta=er.metadata||{};
  const now=new Date().toISOString().replace('T',' ').substring(0,19)+' UTC';

  // Initialize metadata defaults if not present
  if(!er.metadata) er.metadata={};
  if(!er.metadata.diagram) er.metadata.diagram=er.schema_type?er.schema_type.charAt(0).toUpperCase()+er.schema_type.slice(1)+' Model':'Data Model';
  if(!er.metadata.author) er.metadata.author='Migration Studio';
  if(!er.metadata.created_on) er.metadata.created_on=now;
  if(!er.metadata.modified_on) er.metadata.modified_on=now;
  if(!er.metadata.modified_by) er.metadata.modified_by='user';
  if(!er.metadata.design) er.metadata.design='v1.0';
  if(!er.metadata.model_type) er.metadata.model_type='RelationalModel';
  if(!er.metadata.scope) er.metadata.scope='';

  const m=er.metadata;

  // ── Left panel: Diagram metadata table ──
  const panelX=10, panelY=10;
  const panelW=280, rowH2=20, hdrH2=22;
  const fields=[
    {label:'Diagram:', key:'diagram', value:m.diagram},
    {label:'Author:', key:'author', value:m.author},
    {label:'Created on:', key:'created_on', value:m.created_on},
    {label:'Modified on:', key:'modified_on', value:m.modified_on},
    {label:'Modified by:', key:'modified_by', value:m.modified_by},
    {label:'Design:', key:'design', value:m.design},
    {label:'Model:', key:'model_type', value:m.model_type},
  ];
  const panelH=hdrH2+fields.length*rowH2+4;

  const pg=document.createElementNS('http://www.w3.org/2000/svg','g');
  pg.classList.add('dm-meta-panel');
  pg.setAttribute('transform','translate('+panelX+','+panelY+')');

  // Background
  const bg=document.createElementNS('http://www.w3.org/2000/svg','rect');
  bg.setAttribute('width',panelW);bg.setAttribute('height',panelH);bg.setAttribute('rx','2');
  bg.setAttribute('fill','#FFFFFF');bg.setAttribute('stroke','#1E293B');bg.setAttribute('stroke-width','1.5');
  pg.appendChild(bg);

  // Header row (dark background)
  const hdrBg=document.createElementNS('http://www.w3.org/2000/svg','rect');
  hdrBg.setAttribute('width',panelW);hdrBg.setAttribute('height',hdrH2);hdrBg.setAttribute('rx','2');
  hdrBg.setAttribute('fill','#1E293B');
  pg.appendChild(hdrBg);
  const hdrBg2=document.createElementNS('http://www.w3.org/2000/svg','rect');
  hdrBg2.setAttribute('y',String(hdrH2-2));hdrBg2.setAttribute('width',panelW);hdrBg2.setAttribute('height','2');
  hdrBg2.setAttribute('fill','#1E293B');
  pg.appendChild(hdrBg2);

  // Header text
  const hdrTxt=document.createElementNS('http://www.w3.org/2000/svg','text');
  hdrTxt.setAttribute('x','8');hdrTxt.setAttribute('y',String(hdrH2-6));
  hdrTxt.setAttribute('fill','#FFFFFF');hdrTxt.setAttribute('font-size','11');
  hdrTxt.setAttribute('font-weight','700');hdrTxt.setAttribute('font-family','Segoe UI, system-ui, sans-serif');
  hdrTxt.textContent='Diagram Properties';
  pg.appendChild(hdrTxt);

  // Field rows
  const labelW=90;
  fields.forEach((f,i)=>{
    const ry=hdrH2+i*rowH2;

    // Row separator
    if(i>0){
      const sep=document.createElementNS('http://www.w3.org/2000/svg','line');
      sep.setAttribute('x1','0');sep.setAttribute('y1',String(ry));
      sep.setAttribute('x2',panelW);sep.setAttribute('y2',String(ry));
      sep.setAttribute('stroke','#E2E8F0');sep.setAttribute('stroke-width','0.5');
      pg.appendChild(sep);
    }

    // Label column separator
    const lsep=document.createElementNS('http://www.w3.org/2000/svg','line');
    lsep.setAttribute('x1',String(labelW));lsep.setAttribute('y1',String(ry));
    lsep.setAttribute('x2',String(labelW));lsep.setAttribute('y2',String(ry+rowH2));
    lsep.setAttribute('stroke','#E2E8F0');lsep.setAttribute('stroke-width','0.5');
    pg.appendChild(lsep);

    // Label
    const lbl=document.createElementNS('http://www.w3.org/2000/svg','text');
    lbl.setAttribute('x','6');lbl.setAttribute('y',String(ry+14));
    lbl.setAttribute('fill','#334155');lbl.setAttribute('font-size','10');
    lbl.setAttribute('font-weight','600');lbl.setAttribute('font-family','Segoe UI, system-ui, sans-serif');
    lbl.textContent=f.label;
    pg.appendChild(lbl);

    // Value (clickable to edit)
    const val=document.createElementNS('http://www.w3.org/2000/svg','text');
    val.setAttribute('x',String(labelW+6));val.setAttribute('y',String(ry+14));
    val.setAttribute('fill','#1E293B');val.setAttribute('font-size','10');
    val.setAttribute('font-family','Consolas, monospace');
    val.setAttribute('cursor','pointer');
    val.textContent=f.value||'—';
    val.addEventListener('click',ev=>{
      ev.stopPropagation();
      _dmEditMetaField(er, f.key, f.label, g);
    });
    // Hover underline effect
    val.addEventListener('mouseover',()=>{val.setAttribute('text-decoration','underline');val.setAttribute('fill','#2563EB');});
    val.addEventListener('mouseout',()=>{val.setAttribute('text-decoration','none');val.setAttribute('fill','#1E293B');});
    pg.appendChild(val);
  });

  g.appendChild(pg);

  // ── Right panel: Scope of Model (editable note) ──
  const scopeX=panelX+panelW+20, scopeY=panelY;
  const scopeW=260;
  const scopeLines=(m.scope||'').split('\n');
  const scopeRowH=16;
  const scopeBodyH=Math.max(scopeLines.length*scopeRowH+10, 100);
  const scopeH=hdrH2+scopeBodyH;

  const sg=document.createElementNS('http://www.w3.org/2000/svg','g');
  sg.classList.add('dm-meta-panel');
  sg.setAttribute('transform','translate('+scopeX+','+scopeY+')');

  // Scope background with folded corner effect
  const sBg=document.createElementNS('http://www.w3.org/2000/svg','path');
  const foldSize=14;
  sBg.setAttribute('d','M 0 0 L '+(scopeW-foldSize)+' 0 L '+scopeW+' '+foldSize+' L '+scopeW+' '+scopeH+' L 0 '+scopeH+' Z');
  sBg.setAttribute('fill','#FFFDE7');sBg.setAttribute('stroke','#94A3B8');sBg.setAttribute('stroke-width','1');
  sg.appendChild(sBg);

  // Folded corner triangle
  const fold=document.createElementNS('http://www.w3.org/2000/svg','path');
  fold.setAttribute('d','M '+(scopeW-foldSize)+' 0 L '+(scopeW-foldSize)+' '+foldSize+' L '+scopeW+' '+foldSize+' Z');
  fold.setAttribute('fill','#E2E8F0');fold.setAttribute('stroke','#94A3B8');fold.setAttribute('stroke-width','0.5');
  sg.appendChild(fold);

  // Scope header
  const sHdr=document.createElementNS('http://www.w3.org/2000/svg','text');
  sHdr.setAttribute('x','8');sHdr.setAttribute('y','15');
  sHdr.setAttribute('fill','#1E293B');sHdr.setAttribute('font-size','11');
  sHdr.setAttribute('font-weight','700');sHdr.setAttribute('font-family','Segoe UI, system-ui, sans-serif');
  sHdr.textContent='Scope of Model:';
  sg.appendChild(sHdr);

  // Scope separator
  const sSep=document.createElementNS('http://www.w3.org/2000/svg','line');
  sSep.setAttribute('x1','0');sSep.setAttribute('y1',String(hdrH2));
  sSep.setAttribute('x2',scopeW);sSep.setAttribute('y2',String(hdrH2));
  sSep.setAttribute('stroke','#CBD5E1');sSep.setAttribute('stroke-width','0.5');
  sg.appendChild(sSep);

  // Scope content lines
  if(m.scope){
    scopeLines.forEach((line,li)=>{
      const st=document.createElementNS('http://www.w3.org/2000/svg','text');
      st.setAttribute('x','8');st.setAttribute('y',String(hdrH2+14+li*scopeRowH));
      st.setAttribute('fill','#334155');st.setAttribute('font-size','10');
      st.setAttribute('font-family','Segoe UI, system-ui, sans-serif');
      st.textContent=line;
      sg.appendChild(st);
    });
  } else {
    const placeholder=document.createElementNS('http://www.w3.org/2000/svg','text');
    placeholder.setAttribute('x','8');placeholder.setAttribute('y',String(hdrH2+14));
    placeholder.setAttribute('fill','#94A3B8');placeholder.setAttribute('font-size','10');
    placeholder.setAttribute('font-style','italic');
    placeholder.setAttribute('font-family','Segoe UI, system-ui, sans-serif');
    placeholder.textContent='Click to add scope description...';
    sg.appendChild(placeholder);
  }

  // Click whole scope panel to edit
  sg.style.cursor='pointer';
  sg.addEventListener('click',ev=>{
    ev.stopPropagation();
    _dmEditMetaScope(er, g);
  });
  sg.addEventListener('mouseover',()=>{sBg.setAttribute('fill','#FFFBEB');});
  sg.addEventListener('mouseout',()=>{sBg.setAttribute('fill','#FFFDE7');});

  g.appendChild(sg);
}

// Edit a single metadata field via prompt
function _dmEditMetaField(er, key, label, g){
  const current=er.metadata[key]||'';
  const newVal=prompt('Edit '+label.replace(':','')+':',current);
  if(newVal===null)return;
  er.metadata[key]=newVal;
  er.metadata.modified_on=new Date().toISOString().replace('T',' ').substring(0,19)+' UTC';
  // Re-render metadata panel
  _dmRenderMetadataPanel(er, g);
  // Persist to backend
  _dmSaveMetadata(er.metadata);
}

// Edit scope via textarea modal
function _dmEditMetaScope(er, g){
  const current=er.metadata.scope||'';
  // Create a modal overlay
  let overlay=document.getElementById('dmScopeEditOverlay');
  if(overlay)overlay.remove();
  overlay=document.createElement('div');
  overlay.id='dmScopeEditOverlay';
  overlay.style.cssText='position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.4);display:flex;align-items:center;justify-content:center;z-index:9999;';
  const modal=document.createElement('div');
  modal.style.cssText='background:#fff;border-radius:8px;padding:20px;width:440px;max-width:90%;box-shadow:0 8px 32px rgba(0,0,0,0.2);';
  modal.innerHTML='<div style="font-size:14px;font-weight:700;color:#1E293B;margin-bottom:12px;">Edit Scope of Model</div>'+
    '<textarea id="dmScopeTextarea" style="width:100%;height:180px;border:1px solid #CBD5E1;border-radius:6px;padding:10px;font-size:12px;font-family:Segoe UI,system-ui,sans-serif;resize:vertical;outline:none;" placeholder="Enter scope description...\n- Hardware & SW\n- Services SKUs\n- Pricing Conditions">'+_escHtml(current)+'</textarea>'+
    '<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px;">'+
    '<button onclick="document.getElementById(\'dmScopeEditOverlay\').remove()" style="padding:6px 16px;border:1px solid #CBD5E1;border-radius:6px;background:#fff;cursor:pointer;font-size:12px;">Cancel</button>'+
    '<button id="dmScopeSaveBtn" style="padding:6px 16px;border:none;border-radius:6px;background:#2563EB;color:#fff;cursor:pointer;font-size:12px;font-weight:600;">Save</button>'+
    '</div>';
  overlay.appendChild(modal);
  document.body.appendChild(overlay);
  overlay.addEventListener('click',ev=>{if(ev.target===overlay)overlay.remove();});
  document.getElementById('dmScopeSaveBtn').addEventListener('click',()=>{
    const val=document.getElementById('dmScopeTextarea').value;
    er.metadata.scope=val;
    er.metadata.modified_on=new Date().toISOString().replace('T',' ').substring(0,19)+' UTC';
    _dmRenderMetadataPanel(er, g);
    _dmSaveMetadata(er.metadata);
    overlay.remove();
  });
  document.getElementById('dmScopeTextarea').focus();
}

// Persist metadata to backend
async function _dmSaveMetadata(metadata){
  try{
    await fetch('/api/v1/datamodel/metadata',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({metadata})
    });
  }catch(e){console.warn('Failed to save metadata:',e);}
}

function _dmDrawEdges(er, g){
  // Remove old edge elements
  g.querySelectorAll('.dm-edge-el').forEach(el=>el.remove());

  const firstNodeG=g.querySelector('g[data-node-id]');
  const nodeW=300;

  // Calculate actual box height for a node
  function _edgeNodeH(n){
    const cols=(n.columns||[]).length;
    const pkCols=(n.columns||[]).filter(c=>c.is_pk).length;
    const fkCols=(n.columns||[]).filter(c=>c.fk_table).length;
    const constraintLines=(pkCols?1:0)+fkCols;
    return 28+cols*22+6+(constraintLines?(constraintLines*20+12):0)+4;
  }

  // Track port usage to offset multiple connections on same side
  const portCounters={};
  function _getPort(nodeId, side){
    const key=nodeId+'_'+side;
    if(!portCounters[key])portCounters[key]=0;
    portCounters[key]++;
    return portCounters[key];
  }

  er.edges.forEach((e,ei)=>{
    const from=er.nodes.find(n=>n.id===e.from);
    const to=er.nodes.find(n=>n.id===e.to);
    if(!from||!to)return;

    const fromH=_edgeNodeH(from);
    const toH=_edgeNodeH(to);

    // Find best connection points (nearest edges, with port offsets)
    const fromCx=from.x+nodeW/2, fromCy=from.y+fromH/2;
    const toCx=to.x+nodeW/2, toCy=to.y+toH/2;
    const dx=toCx-fromCx, dy=toCy-fromCy;

    let x1,y1,x2,y2,side1,side2;
    if(Math.abs(dx)>Math.abs(dy)*0.6){
      // Connect left/right sides
      if(dx>0){
        side1='right';side2='left';
        x1=from.x+nodeW;x2=to.x;
      } else {
        side1='left';side2='right';
        x1=from.x;x2=to.x+nodeW;
      }
      // Offset y based on port count to avoid overlapping lines
      const p1=_getPort(from.id,side1);
      const p2=_getPort(to.id,side2);
      const fromSlots=Math.max(4, Math.floor(fromH/30));
      const toSlots=Math.max(4, Math.floor(toH/30));
      y1=from.y+Math.min(fromH-10, 28+(p1*((fromH-36)/fromSlots)));
      y2=to.y+Math.min(toH-10, 28+(p2*((toH-36)/toSlots)));
    } else {
      // Connect top/bottom
      if(dy>0){
        side1='bottom';side2='top';
        y1=from.y+fromH;y2=to.y;
      } else {
        side1='top';side2='bottom';
        y1=from.y;y2=to.y+toH;
      }
      const p1=_getPort(from.id,side1);
      const p2=_getPort(to.id,side2);
      const maxPorts=Math.max(3, Math.floor(nodeW/80));
      x1=from.x+Math.min(nodeW-20, 40+(p1*((nodeW-60)/maxPorts)));
      x2=to.x+Math.min(nodeW-20, 40+(p2*((nodeW-60)/maxPorts)));
    }

    // Orthogonal path (SQL Developer style — right-angle connectors)
    const mx=(x1+x2)/2, my=(y1+y2)/2;
    let pathD;
    if(side1==='right'||side1==='left'){
      // Horizontal exit: go halfway then turn vertical
      const midX=x1+(x2-x1)*0.5;
      pathD='M '+x1+' '+y1+' L '+midX+' '+y1+' L '+midX+' '+y2+' L '+x2+' '+y2;
    } else {
      // Vertical exit: go halfway then turn horizontal
      const midY=y1+(y2-y1)*0.5;
      pathD='M '+x1+' '+y1+' L '+x1+' '+midY+' L '+x2+' '+midY+' L '+x2+' '+y2;
    }

    const path=document.createElementNS('http://www.w3.org/2000/svg','path');
    path.classList.add('dm-edge-el');
    path.setAttribute('d', pathD);
    path.setAttribute('fill','none');
    path.setAttribute('stroke','#475569');
    path.setAttribute('stroke-width','1.8');
    path.setAttribute('stroke-dasharray', e.label==='many-to-many'?'6,3':'none');

    // Crow's foot markers
    const relType=e.label||'many-to-one';
    if(_dmNotation==='crowsfoot'){
      if(relType.includes('many-to-one')){
        path.setAttribute('marker-start','url(#dmCrowManyFill)');
        path.setAttribute('marker-end','url(#dmCrowOne)');
      } else if(relType.includes('one-to-many')){
        path.setAttribute('marker-start','url(#dmCrowOneFill)');
        path.setAttribute('marker-end','url(#dmCrowMany)');
      } else if(relType.includes('one-to-one')){
        path.setAttribute('marker-start','url(#dmCrowOneFill)');
        path.setAttribute('marker-end','url(#dmCrowOne)');
      } else if(relType.includes('many-to-many')){
        path.setAttribute('marker-start','url(#dmCrowManyFill)');
        path.setAttribute('marker-end','url(#dmCrowMany)');
      }
    } else {
      path.setAttribute('marker-end','url(#dmArrow)');
    }

    // Click to edit
    path.style.cursor='pointer';
    path.addEventListener('click',ev=>{
      ev.stopPropagation();
      _dmShowEdgeEditor(e, ei, mx, my);
    });

    // Hover effect
    path.addEventListener('mouseover',()=>{path.setAttribute('stroke','#2563EB');path.setAttribute('stroke-width','2.8');});
    path.addEventListener('mouseout',()=>{path.setAttribute('stroke','#475569');path.setAttribute('stroke-width','1.8');});

    if(firstNodeG) g.insertBefore(path,firstNodeG); else g.appendChild(path);

    // Relationship label on a background pill near midpoint
    const lblX=mx, lblY=my-6;
    // Label background for readability
    const lblBg=document.createElementNS('http://www.w3.org/2000/svg','rect');
    lblBg.classList.add('dm-edge-el');
    const relLabel=_dmNotation==='crowsfoot'?_dmCrowsFootLabel(e.label):(e.label||'');
    const lblW=Math.max(40, relLabel.length*7+12);
    lblBg.setAttribute('x',String(lblX-lblW/2));lblBg.setAttribute('y',String(lblY-12));
    lblBg.setAttribute('width',String(lblW));lblBg.setAttribute('height','16');
    lblBg.setAttribute('rx','3');
    lblBg.setAttribute('fill','#FFFFFF');lblBg.setAttribute('stroke','#C7D2FE');lblBg.setAttribute('stroke-width','1');
    if(firstNodeG) g.insertBefore(lblBg,firstNodeG); else g.appendChild(lblBg);

    const lbl=document.createElementNS('http://www.w3.org/2000/svg','text');
    lbl.classList.add('dm-edge-el');
    lbl.setAttribute('x',String(lblX));lbl.setAttribute('y',String(lblY));
    lbl.setAttribute('text-anchor','middle');
    lbl.setAttribute('fill','#4338CA');lbl.setAttribute('font-size','10');
    lbl.setAttribute('font-family','Consolas, monospace');lbl.setAttribute('font-weight','600');
    lbl.textContent=relLabel;
    if(firstNodeG) g.insertBefore(lbl,firstNodeG); else g.appendChild(lbl);

    // FK column label near source
    if(e.via_column){
      const fkLbl=document.createElementNS('http://www.w3.org/2000/svg','text');
      fkLbl.classList.add('dm-edge-el');
      fkLbl.setAttribute('x',String(x1+(dx>0?14:-14)));fkLbl.setAttribute('y',String(y1-6));
      fkLbl.setAttribute('text-anchor',dx>0?'start':'end');
      fkLbl.setAttribute('fill','#059669');fkLbl.setAttribute('font-size','10');
      fkLbl.setAttribute('font-family','Consolas, monospace');
      fkLbl.textContent=e.via_column;
      if(firstNodeG) g.insertBefore(fkLbl,firstNodeG); else g.appendChild(fkLbl);
    }
  });
}

function _dmCrowsFootLabel(type){
  if(!type)return '';
  if(type==='many-to-one')return '*..1';
  if(type==='one-to-many')return '1..*';
  if(type==='one-to-one')return '1..1';
  if(type==='many-to-many')return '*..*';
  return type;
}

function _dmShowEdgeEditor(edge, edgeIdx, x, y){
  // Show Foreign Key Properties dialog (SQL Developer style)
  const existing=document.getElementById('dmFKPropsOverlay');
  if(existing)existing.remove();

  const fromNode=_dmErJson?_dmErJson.nodes.find(n=>n.id===edge.from):null;
  const toNode=_dmErJson?_dmErJson.nodes.find(n=>n.id===edge.to):null;
  const fromLabel=fromNode?fromNode.label:edge.from;
  const toLabel=toNode?toNode.label:edge.to;
  const fkName=edge.via_column?fromLabel+'_'+toLabel+'_FK':'FK_'+fromLabel+'_'+toLabel;

  // Find PK columns of target table
  const toCols=(toNode?toNode.columns:[]).filter(c=>c.is_pk);
  const fromCols=(fromNode?fromNode.columns:[])||[];
  const fkCols=fromCols.filter(c=>c.fk_table===edge.to);

  const overlay=document.createElement('div');
  overlay.id='dmFKPropsOverlay';
  overlay.style.cssText='position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.45);display:flex;align-items:center;justify-content:center;z-index:9999;';

  const dlg=document.createElement('div');
  dlg.style.cssText='background:#F0F0F0;border-radius:4px;width:680px;max-width:92%;max-height:88vh;display:flex;flex-direction:column;box-shadow:0 12px 48px rgba(0,0,0,0.3);font-family:Segoe UI,system-ui,sans-serif;overflow:hidden;';

  // Title bar
  dlg.innerHTML=`
    <div style="background:linear-gradient(180deg,#F8F8F8,#E8E8E8);border-bottom:1px solid #B0B0B0;padding:8px 12px;display:flex;align-items:center;justify-content:space-between;">
      <span style="font-size:12px;font-weight:600;color:#1E293B;">Foreign Key Properties - ${_escHtml(fkName)}</span>
      <button id="dmFKCloseBtn" style="border:none;background:none;font-size:16px;cursor:pointer;color:#64748B;padding:0 4px;">&times;</button>
    </div>
    <div style="display:flex;flex:1;overflow:hidden;">
      <!-- Left tabs -->
      <div style="width:170px;background:#FFFFFF;border-right:1px solid #D0D0D0;padding:8px 0;flex-shrink:0;">
        <div class="dmfk-tab dmfk-tab-active" data-tab="general" style="padding:5px 12px;font-size:11px;cursor:pointer;border-left:3px solid #2563EB;background:#EFF6FF;color:#1E293B;font-weight:600;">General</div>
        <div class="dmfk-tab" data-tab="columns" style="padding:5px 12px;font-size:11px;cursor:pointer;border-left:3px solid transparent;color:#475569;">Dependent Columns Constraint</div>
        <div class="dmfk-tab" data-tab="comments" style="padding:5px 12px;font-size:11px;cursor:pointer;border-left:3px solid transparent;color:#475569;">Comments</div>
        <div class="dmfk-tab" data-tab="notes" style="padding:5px 12px;font-size:11px;cursor:pointer;border-left:3px solid transparent;color:#475569;">Notes</div>
        <div class="dmfk-tab" data-tab="impact" style="padding:5px 12px;font-size:11px;cursor:pointer;border-left:3px solid transparent;color:#475569;">Impact Analysis</div>
        <div class="dmfk-tab" data-tab="properties" style="padding:5px 12px;font-size:11px;cursor:pointer;border-left:3px solid transparent;color:#475569;">Dynamic Properties</div>
        <div class="dmfk-tab" data-tab="userdefined" style="padding:5px 12px;font-size:11px;cursor:pointer;border-left:3px solid transparent;color:#475569;">User Defined Properties</div>
        <div class="dmfk-tab" data-tab="summary" style="padding:5px 12px;font-size:11px;cursor:pointer;border-left:3px solid transparent;color:#475569;">Summary</div>
      </div>
      <!-- Right content -->
      <div style="flex:1;padding:16px;overflow-y:auto;background:#FFFFFF;" id="dmFKTabContent">
        <div style="text-align:center;font-size:12px;font-weight:600;color:#475569;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #E2E8F0;">General</div>
        <div style="display:grid;grid-template-columns:140px 1fr 100px 1fr;gap:8px 12px;align-items:center;font-size:11px;">
          <label style="color:#475569;font-weight:600;">Name</label>
          <input id="dmFKName" class="inp" style="font-size:11px;padding:4px 8px;grid-column:span 1;" value="${_escHtml(fkName)}">
          <label style="color:#475569;font-weight:600;">Table</label>
          <input class="inp" style="font-size:11px;padding:4px 8px;background:#F1F5F9;" value="${_escHtml(fromLabel)}" readonly>

          <label style="color:#475569;font-weight:600;">PK / UK Index</label>
          <select id="dmFKPKIndex" class="inp" style="font-size:11px;padding:4px 8px;">
            <option value="${_escHtml(toLabel)}_PK">${_escHtml(toLabel)}_PK</option>
          </select>
          <label style="color:#475569;font-weight:600;">Delete Rule</label>
          <select id="dmFKDeleteRule" class="inp" style="font-size:11px;padding:4px 8px;">
            <option value="NO ACTION" selected>NO ACTION</option>
            <option value="CASCADE">CASCADE</option>
            <option value="SET NULL">SET NULL</option>
            <option value="SET DEFAULT">SET DEFAULT</option>
            <option value="RESTRICT">RESTRICT</option>
          </select>

          <label style="color:#475569;font-weight:600;">Source Table</label>
          <input class="inp" style="font-size:11px;padding:4px 8px;background:#F1F5F9;" value="${_escHtml(fromLabel)}" readonly>
          <label style="color:#475569;font-weight:600;">Target Table</label>
          <input class="inp" style="font-size:11px;padding:4px 8px;background:#F1F5F9;" value="${_escHtml(toLabel)}" readonly>

          <label style="color:#475569;font-weight:600;">Relationship Type</label>
          <select id="dmFKRelType" class="inp" style="font-size:11px;padding:4px 8px;">
            <option value="many-to-one" ${edge.label==='many-to-one'?'selected':''}>Many-to-One (*..1)</option>
            <option value="one-to-many" ${edge.label==='one-to-many'?'selected':''}>One-to-Many (1..*)</option>
            <option value="one-to-one" ${edge.label==='one-to-one'?'selected':''}>One-to-One (1..1)</option>
            <option value="many-to-many" ${edge.label==='many-to-many'?'selected':''}>Many-to-Many (*..*)</option>
          </select>
          <label style="color:#475569;font-weight:600;">Update Rule</label>
          <select id="dmFKUpdateRule" class="inp" style="font-size:11px;padding:4px 8px;">
            <option value="NO ACTION" selected>NO ACTION</option>
            <option value="CASCADE">CASCADE</option>
            <option value="SET NULL">SET NULL</option>
            <option value="RESTRICT">RESTRICT</option>
          </select>
        </div>

        <div style="margin-top:14px;display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;font-size:11px;">
          <label style="display:flex;align-items:center;gap:4px;color:#475569;"><input type="checkbox" id="dmFKMandatory" ${fkCols.some(c=>!c.is_nullable)?'checked':''}> Mandatory</label>
          <label style="display:flex;align-items:center;gap:4px;color:#475569;"><input type="checkbox" id="dmFKDeprecated"> Deprecated</label>
          <label style="display:flex;align-items:center;gap:4px;color:#475569;"><input type="checkbox" id="dmFKTransferable" checked> Transferable (Updatable)</label>
          <label style="display:flex;align-items:center;gap:4px;color:#475569;"><input type="checkbox" id="dmFKGenerateDDL" checked> Generate in DDL</label>
        </div>

        <div style="margin-top:16px;font-size:11px;font-weight:600;color:#475569;border-bottom:1px solid #E2E8F0;padding-bottom:4px;">Associated Columns</div>
        <table style="width:100%;border-collapse:collapse;font-size:11px;margin-top:6px;">
          <thead>
            <tr style="background:#F1F5F9;">
              <th style="text-align:left;padding:5px 8px;border:1px solid #E2E8F0;font-weight:600;color:#334155;">Referenced Column (PK)</th>
              <th style="text-align:left;padding:5px 8px;border:1px solid #E2E8F0;font-weight:600;color:#334155;">Column (FK)</th>
              <th style="text-align:center;padding:5px 8px;border:1px solid #E2E8F0;font-weight:600;color:#334155;">Mandatory</th>
            </tr>
          </thead>
          <tbody id="dmFKColsBody">
            ${fkCols.length?fkCols.map(c=>{
              const refCol=toCols.length?toCols[0].name:'—';
              return '<tr><td style="padding:4px 8px;border:1px solid #E2E8F0;">'+_escHtml(refCol)+'</td><td style="padding:4px 8px;border:1px solid #E2E8F0;">'+_escHtml(c.name)+'</td><td style="text-align:center;padding:4px 8px;border:1px solid #E2E8F0;"><input type="checkbox" '+(c.is_nullable?'':'checked')+'></td></tr>';
            }).join(''):'<tr><td style="padding:4px 8px;border:1px solid #E2E8F0;color:#94A3B8;" colspan="3">No FK columns mapped yet. Use "+ Add Mapping" below.</td></tr>'}
          </tbody>
        </table>
        <button id="dmFKAddMappingBtn" style="margin-top:6px;font-size:10px;padding:3px 10px;border:1px solid #3B82F6;border-radius:4px;background:#EFF6FF;color:#2563EB;cursor:pointer;">+ Add Mapping</button>
      </div>
    </div>
    <!-- Bottom buttons -->
    <div style="border-top:1px solid #B0B0B0;padding:10px 16px;display:flex;justify-content:flex-end;gap:8px;background:#F0F0F0;">
      <button id="dmFKOKBtn" style="padding:5px 24px;font-size:11px;border:1px solid #3B82F6;border-radius:4px;background:#2563EB;color:#fff;cursor:pointer;font-weight:600;">OK</button>
      <button id="dmFKApplyBtn" style="padding:5px 24px;font-size:11px;border:1px solid #3B82F6;border-radius:4px;background:#EFF6FF;color:#2563EB;cursor:pointer;font-weight:600;">Apply</button>
      <button id="dmFKCancelBtn" style="padding:5px 24px;font-size:11px;border:1px solid #CBD5E1;border-radius:4px;background:#fff;color:#475569;cursor:pointer;">Cancel</button>
    </div>
  `;

  overlay.appendChild(dlg);
  document.body.appendChild(overlay);

  // Tab switching logic
  dlg.querySelectorAll('.dmfk-tab').forEach(tab=>{
    tab.addEventListener('click',()=>{
      dlg.querySelectorAll('.dmfk-tab').forEach(t=>{t.style.borderLeftColor='transparent';t.style.background='';t.style.fontWeight='400';t.classList.remove('dmfk-tab-active');});
      tab.style.borderLeftColor='#2563EB';tab.style.background='#EFF6FF';tab.style.fontWeight='600';tab.classList.add('dmfk-tab-active');
      const tabName=tab.getAttribute('data-tab');
      _dmFKSwitchTab(tabName, edge, fromLabel, toLabel, fkName);
    });
  });

  // Add column mapping button
  document.getElementById('dmFKAddMappingBtn').addEventListener('click',()=>{
    const tbody=document.getElementById('dmFKColsBody');
    const fromOpts=fromCols.map(c=>'<option value="'+_escHtml(c.name)+'">'+_escHtml(c.name)+'</option>').join('');
    const toOpts=toCols.map(c=>'<option value="'+_escHtml(c.name)+'">'+_escHtml(c.name)+'</option>').join('');
    const row=document.createElement('tr');
    row.innerHTML='<td style="padding:4px 8px;border:1px solid #E2E8F0;"><select class="inp" style="font-size:10px;padding:2px 4px;">'+toOpts+'</select></td><td style="padding:4px 8px;border:1px solid #E2E8F0;"><select class="inp" style="font-size:10px;padding:2px 4px;">'+fromOpts+'</select></td><td style="text-align:center;padding:4px 8px;border:1px solid #E2E8F0;"><input type="checkbox" checked></td>';
    tbody.appendChild(row);
  });

  // Close handlers
  document.getElementById('dmFKCloseBtn').addEventListener('click',()=>overlay.remove());
  document.getElementById('dmFKCancelBtn').addEventListener('click',()=>overlay.remove());
  overlay.addEventListener('click',ev=>{if(ev.target===overlay)overlay.remove();});

  // Apply logic
  const applyFn=()=>{
    const newType=document.getElementById('dmFKRelType').value;
    if(newType!==edge.label){
      _dmApplyFKChanges(edgeIdx, edge, newType);
    }
  };
  document.getElementById('dmFKApplyBtn').addEventListener('click',applyFn);
  document.getElementById('dmFKOKBtn').addEventListener('click',()=>{applyFn();overlay.remove();});
}

function _dmFKSwitchTab(tabName, edge, fromLabel, toLabel, fkName){
  const content=document.getElementById('dmFKTabContent');
  if(!content)return;
  if(tabName==='general')return; // Already rendered as default
  if(tabName==='comments'){
    content.innerHTML='<div style="text-align:center;font-size:12px;font-weight:600;color:#475569;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #E2E8F0;">Comments</div><textarea id="dmFKComment" style="width:100%;height:200px;border:1px solid #CBD5E1;border-radius:4px;padding:10px;font-size:11px;font-family:Segoe UI,system-ui,sans-serif;resize:vertical;" placeholder="Add comments about this foreign key relationship...">'+(edge.comment||'')+'</textarea>';
  } else if(tabName==='notes'){
    content.innerHTML='<div style="text-align:center;font-size:12px;font-weight:600;color:#475569;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #E2E8F0;">Notes</div><textarea style="width:100%;height:200px;border:1px solid #CBD5E1;border-radius:4px;padding:10px;font-size:11px;font-family:Segoe UI,system-ui,sans-serif;resize:vertical;" placeholder="Add design notes...">'+(edge.notes||'')+'</textarea>';
  } else if(tabName==='summary'){
    content.innerHTML='<div style="text-align:center;font-size:12px;font-weight:600;color:#475569;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #E2E8F0;">Summary</div><div style="font-size:11px;color:#334155;line-height:1.8;"><table style="width:100%;border-collapse:collapse;"><tr><td style="padding:4px 8px;font-weight:600;border-bottom:1px solid #E2E8F0;width:140px;">FK Name:</td><td style="padding:4px 8px;border-bottom:1px solid #E2E8F0;">'+_escHtml(fkName)+'</td></tr><tr><td style="padding:4px 8px;font-weight:600;border-bottom:1px solid #E2E8F0;">Source Table:</td><td style="padding:4px 8px;border-bottom:1px solid #E2E8F0;">'+_escHtml(fromLabel)+'</td></tr><tr><td style="padding:4px 8px;font-weight:600;border-bottom:1px solid #E2E8F0;">Target Table:</td><td style="padding:4px 8px;border-bottom:1px solid #E2E8F0;">'+_escHtml(toLabel)+'</td></tr><tr><td style="padding:4px 8px;font-weight:600;border-bottom:1px solid #E2E8F0;">Relationship:</td><td style="padding:4px 8px;border-bottom:1px solid #E2E8F0;">'+_escHtml(edge.label||'many-to-one')+'</td></tr><tr><td style="padding:4px 8px;font-weight:600;border-bottom:1px solid #E2E8F0;">Via Column:</td><td style="padding:4px 8px;border-bottom:1px solid #E2E8F0;">'+_escHtml(edge.via_column||'—')+'</td></tr></table></div>';
  } else if(tabName==='impact'){
    content.innerHTML='<div style="text-align:center;font-size:12px;font-weight:600;color:#475569;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #E2E8F0;">Impact Analysis</div><div style="font-size:11px;color:#475569;padding:20px;text-align:center;background:#F8FAFC;border-radius:6px;"><p style="margin:0 0 8px;">Changing or removing this FK will affect:</p><ul style="text-align:left;list-style:disc;padding-left:20px;margin:8px 0;"><li>Table <b>'+_escHtml(fromLabel)+'</b> — column constraints</li><li>Table <b>'+_escHtml(toLabel)+'</b> — referenced PK</li><li>DDL generation output</li><li>ER diagram relationships</li></ul></div>';
  } else if(tabName==='columns'){
    content.innerHTML='<div style="text-align:center;font-size:12px;font-weight:600;color:#475569;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #E2E8F0;">Dependent Columns Constraint</div><div style="font-size:11px;color:#475569;padding:12px;background:#F8FAFC;border-radius:6px;"><p>Columns in <b>'+_escHtml(fromLabel)+'</b> that depend on this FK constraint:</p><ul style="margin:8px 0;padding-left:18px;">'+(edge.via_column?'<li><b>'+_escHtml(edge.via_column)+'</b> → references '+_escHtml(toLabel)+'</li>':'<li style="color:#94A3B8;">No dependent columns defined</li>')+'</ul></div>';
  } else {
    content.innerHTML='<div style="text-align:center;font-size:12px;font-weight:600;color:#475569;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #E2E8F0;">'+tabName.charAt(0).toUpperCase()+tabName.slice(1)+'</div><div style="padding:30px;text-align:center;color:#94A3B8;font-size:11px;">No data configured for this section.</div>';
  }
}

async function _dmApplyFKChanges(edgeIdx, edge, newType){
  if(!_dmModel||!_dmModel.relationships)return;
  // Use edge.from/to directly instead of index (more reliable)
  const fromId=edge.from, toId=edge.to;
  const rel=_dmModel.relationships.find(r=>r.from===fromId&&r.to===toId);
  if(!rel){toast('Relationship not found','terr');return;}
  // Preserve current node positions before re-render
  _dmPreservePositions();
  await _dmEdit({
    relationship_removes:[{from:fromId,to:toId}],
    relationship_adds:[{from:fromId,to:toId,type:newType,via_column:edge.via_column||rel.via_column||''}]
  },'Relationship updated to '+newType);
}

// Preserve node positions so re-render doesn't reset them
var _dmSavedPositions={};
function _dmPreservePositions(){
  if(!_dmErJson||!_dmErJson.nodes)return;
  _dmErJson.nodes.forEach(n=>{
    if(n.x!==undefined&&n.y!==undefined){
      _dmSavedPositions[n.id]={x:n.x,y:n.y};
    }
  });
}

function dmToggleNotation(){
  _dmNotation=_dmNotation==='crowsfoot'?'arrow':'crowsfoot';
  const btn=G('dmNotationBtn');
  if(btn) btn.textContent=_dmNotation==='crowsfoot'?'\u27c1 Crow\u2019s Foot':'\u2192 Arrow';
  if(_dmErJson) dmRenderER(_dmErJson);
}

// Dynamically resize SVG so scrollbars reflect the actual diagram extent
function _dmResizeSvg(){
  if(!_dmErJson||!_dmErJson.nodes||!_dmErJson.nodes.length) return;
  const svg=G('dmErSvg');
  const container=G('dmErContainer');
  if(!svg||!container)return;
  const nodeW=300, rowH=22, hdrH=28;
  let maxX=0, maxY=0;
  _dmErJson.nodes.forEach(n=>{
    const cols=(n.columns||[]).length;
    const h=hdrH+cols*rowH+40;
    const nx=(n.x||0)*_dmZoomLevel+_dmPanX+nodeW*_dmZoomLevel+60;
    const ny=(n.y||0)*_dmZoomLevel+_dmPanY+h*_dmZoomLevel+60;
    if(nx>maxX) maxX=nx;
    if(ny>maxY) maxY=ny;
  });
  const minW=container.clientWidth;
  const minH=container.clientHeight;
  svg.setAttribute('width', Math.max(Math.ceil(maxX), minW));
  svg.setAttribute('height', Math.max(Math.ceil(maxY), minH));
  _dmSyncTopScroll();
}

function dmAutoLayout(){
  if(!_dmErJson||!_dmErJson.nodes||!_dmErJson.nodes.length) return;
  // Clear saved positions to force re-layout
  _dmSavedPositions={};
  _dmErJson.nodes.forEach(n=>{n.x=undefined;n.y=undefined;n._autoLayout=true;});
  // Re-render with fresh layout then fit to viewport
  dmRenderER(_dmErJson);
  setTimeout(()=>dmFitView(), 150);
  toast('Layout optimized','tok');
}

// Sync top scrollbar with main container
function _dmSyncTopScroll(){
  const container=G('dmErContainer');
  const topScroll=G('dmErTopScroll');
  const topInner=G('dmErTopScrollInner');
  if(!container||!topScroll||!topInner)return;
  const svg=G('dmErSvg');
  topInner.style.width=svg.getAttribute('width')+'px';
  topInner.style.height='1px';
}
function _dmInitTopScroll(){
  const container=G('dmErContainer');
  const topScroll=G('dmErTopScroll');
  if(!container||!topScroll)return;
  // Sync: when user scrolls top bar, scroll main container
  topScroll.addEventListener('scroll',()=>{
    container.scrollLeft=topScroll.scrollLeft;
  });
  // Sync: when user scrolls main container, update top bar
  container.addEventListener('scroll',()=>{
    topScroll.scrollLeft=container.scrollLeft;
  });
}

function dmFitView(){
  if(!_dmErJson||!_dmErJson.nodes||!_dmErJson.nodes.length){
    // Fallback: use SVG group bounding box if nodes array empty
    const grp=G('dmErGroup');
    const svg=G('dmErSvg');
    const container=G('dmErContainer');
    if(!grp||!grp.children.length)return;
    try{
      const bbox=grp.getBBox();
      if(!bbox||bbox.width<10)return;
      const viewportW=container?container.clientWidth-16:900;
      const viewportH=container?container.clientHeight-16:900;
      const contentW=bbox.width+60;
      const contentH=bbox.height+60;
      // Use minimum readable scale of 0.55
      const scaleX=viewportW/contentW;
      const scaleY=viewportH/contentH;
      _dmZoomLevel=Math.min(scaleX, scaleY);
      _dmZoomLevel=Math.max(0.55, Math.min(1.0, _dmZoomLevel));
      // Size SVG to show content at readable scale
      const svgW=Math.max(viewportW, contentW*_dmZoomLevel);
      const svgH=Math.max(viewportH, contentH*_dmZoomLevel);
      svg.setAttribute('width', Math.ceil(svgW));
      svg.setAttribute('height', Math.ceil(svgH));
      _dmPanX=-(bbox.x-30)*_dmZoomLevel;
      _dmPanY=-(bbox.y-30)*_dmZoomLevel;
      grp.style.transition='transform 0.3s ease';
      grp.setAttribute('transform','translate('+_dmPanX+','+_dmPanY+') scale('+_dmZoomLevel+')');
      setTimeout(()=>{grp.style.transition='';}, 350);
      if(container){container.scrollTop=0;container.scrollLeft=0;}
      _dmSyncTopScroll();
    }catch(e){console.warn('dmFitView bbox fallback error',e);}
    return;
  }
  const svg=G('dmErSvg');
  const container=G('dmErContainer');
  const grp=G('dmErGroup');
  const nodeW=300, rowH=22, hdrH=28;
  const viewportW=container?container.clientWidth-16:900;
  const viewportH=container?container.clientHeight-16:900;

  // Calculate bounding box including metadata panels (start at 0,0) and all nodes
  let minX=0, minY=0, maxX=600, maxY=200;
  _dmErJson.nodes.forEach(n=>{
    const cols=(n.columns||[]).length;
    const pkCols=(n.columns||[]).filter(c=>c.is_pk).length;
    const fkCols=(n.columns||[]).filter(c=>c.fk_table).length;
    const constraintLines=(pkCols?1:0)+fkCols;
    const h=hdrH+cols*rowH+6+(constraintLines?(constraintLines*20+12):0)+4;
    if(n.x<minX)minX=n.x;
    if(n.y<minY)minY=n.y;
    if(n.x+nodeW>maxX)maxX=n.x+nodeW;
    if(n.y+h>maxY)maxY=n.y+h;
  });

  const pad=30;
  minX-=pad; minY-=pad; maxX+=pad; maxY+=pad;
  const contentW=maxX-minX;
  const contentH=maxY-minY;

  // Use minimum readable scale of 0.55 — scrollbars handle overflow
  const scaleX=viewportW/contentW;
  const scaleY=viewportH/contentH;
  _dmZoomLevel=Math.min(scaleX, scaleY);
  _dmZoomLevel=Math.max(0.55, Math.min(1.0, _dmZoomLevel));

  // Size SVG to show all content at chosen scale
  const svgW=Math.max(viewportW, contentW*_dmZoomLevel + 40);
  const svgH=Math.max(viewportH, contentH*_dmZoomLevel + 40);
  svg.setAttribute('width', Math.ceil(svgW));
  svg.setAttribute('height', Math.ceil(svgH));

  // Position content starting from top-left
  _dmPanX=-minX*_dmZoomLevel + 20;
  _dmPanY=-minY*_dmZoomLevel + 20;

  // Apply transform
  grp.style.transition='transform 0.3s ease';
  grp.setAttribute('transform','translate('+_dmPanX+','+_dmPanY+') scale('+_dmZoomLevel+')');
  setTimeout(()=>{grp.style.transition='';}, 350);

  // Scroll container to top-left
  if(container){container.scrollTop=0; container.scrollLeft=0;}
  _dmSyncTopScroll();
}

function dmUpdateEdges(er){
  const g=G('dmErGroup');
  _dmDrawEdges(er, g);
}

function dmZoom(factor){
  if(factor===0){_dmZoomLevel=1;_dmPanX=0;_dmPanY=0;}
  else{_dmZoomLevel*=factor;}
  _dmZoomLevel=Math.max(0.3,Math.min(3,_dmZoomLevel));
  G('dmErGroup').setAttribute('transform','translate('+_dmPanX+','+_dmPanY+') scale('+_dmZoomLevel+')');
}

// ── DDL Change Detection ───────────────────────────────────────────────────────────
async function dmDetectChanges(){
  if(!_dmModelId){toast('Generate a model first','terr');return;}
  toast('Scanning for schema drift...','tok');
  try{
    const cat=G('dmCatalog').value||'main', sch=G('dmSchema').value||'default';
    const r=await fetch('/api/v1/datamodel/detect-changes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({model_id:_dmModelId,catalog:cat,schema:sch})});
    const d=await r.json();
    if(d.success){
      _dmDetectedChanges=d.changes||[];
      if(d.has_changes){
        G('dmChangesAlert').style.display='';
        G('dmChangeCount').textContent=_dmDetectedChanges.length+' changes';
        G('dmChangesList').innerHTML=_dmDetectedChanges.map(c=>{
          const icon=c.type==='column_added'?'\u2795':c.type==='column_removed'?'\u2796':c.type==='type_changed'?'\ud83d\udd04':'\u26a0\ufe0f';
          return '<div style="padding:2px 0;">'+icon+' <b>'+c.table+'</b>: '+c.detail+'</div>';
        }).join('');
        toast(_dmDetectedChanges.length+' schema change(s) detected!','terr');
      } else {
        toast('\u2714 No schema drift detected - model is in sync','tok');
        G('dmChangesAlert').style.display='none';
      }
    }else{toast(d.error||'Detection failed','terr');}
  }catch(e){toast('Error: '+e.message,'terr');}
}

async function dmApplyDetectedChanges(){
  if(!_dmDetectedChanges.length)return;
  // Re-generate model to pick up live changes
  toast('Refreshing model from live schema...','tok');
  await dmGenerate();
  G('dmChangesAlert').style.display='none';
  _dmDetectedChanges=[];
  toast('Model refreshed with latest schema','tok');
}

// ── Relationship Suggestions ────────────────────────────────────────────────────────
async function dmSuggestRelationships(){
  if(!_dmModelId){toast('Generate a model first','terr');return;}
  toast('Analyzing column patterns for relationships...','tok');
  try{
    const r=await fetch('/api/v1/datamodel/suggest-relationships',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({model_id:_dmModelId})});
    const d=await r.json();
    if(d.success){
      _dmSuggestions=d.suggestions||[];
      if(_dmSuggestions.length){
        G('dmSuggestPanel').style.display='';
        G('dmSuggestList').innerHTML=_dmSuggestions.map((s,i)=>{
          const conf=Math.round(s.confidence*100);
          const confColor=conf>=85?'#10B981':conf>=70?'#F59E0B':'#94A3B8';
          return '<div style="display:flex;align-items:center;gap:8px;padding:3px 0;border-bottom:1px solid rgba(0,0,0,.05);">'+
            '<span style="font-size:10px;font-weight:700;color:'+confColor+';">'+conf+'%</span>'+
            '<span style="font-size:10px;"><b>'+s.from+'</b> \u2192 <b>'+s.to+'</b> ('+s.type+')</span>'+
            '<span style="font-size:9px;color:var(--t4);flex:1;">via '+s.via_column+'</span>'+
            '<button class="btn btn-ghost btn-xs" onclick="dmAcceptSuggestion('+i+')" style="font-size:9px;color:#10B981;padding:1px 6px;">\u2714 Add</button>'+
            '</div>';
        }).join('');
        toast(_dmSuggestions.length+' relationship suggestion(s) found','tok');
      } else {
        toast('No additional relationships suggested - model looks complete','tok');
        G('dmSuggestPanel').style.display='none';
      }
    }else{toast(d.error||'Suggestion failed','terr');}
  }catch(e){toast('Error: '+e.message,'terr');}
}

async function dmAcceptSuggestion(idx){
  const s=_dmSuggestions[idx];if(!s)return;
  await _dmEdit({relationship_adds:[{from:s.from,to:s.to,type:s.type}]},'Added: '+s.from+' \u2192 '+s.to);
  _dmSuggestions.splice(idx,1);
  if(_dmSuggestions.length===0) G('dmSuggestPanel').style.display='none';
  else dmSuggestRelationships(); // Refresh list
}

async function dmAcceptAllSuggestions(){
  if(!_dmSuggestions.length)return;
  const adds=_dmSuggestions.map(s=>({from:s.from,to:s.to,type:s.type}));
  await _dmEdit({relationship_adds:adds},'Added '+adds.length+' suggested relationships');
  _dmSuggestions=[];
  G('dmSuggestPanel').style.display='none';
}

// ── Table Details Rendering ─────────────────────────────────────────────────
function dmRenderDetails(d){
  // Facts
  const fDiv=G('dmFactsList');fDiv.innerHTML='';
  (d.facts||[]).forEach(f=>{ fDiv.innerHTML+=dmTableCard(f,'fact'); });
  // Dimensions
  const dDiv=G('dmDimsList');dDiv.innerHTML='';
  (d.dimensions||[]).forEach(dim=>{ dDiv.innerHTML+=dmTableCard(dim,'dimension'); });
  // Relationships with editable cardinality
  const rb=G('dmRelsBody');rb.innerHTML='';
  (d.relationships||[]).forEach((r,i)=>{
    const cardBg=r.type==='many-to-one'?'rgba(99,102,241,.1)':r.type==='one-to-many'?'rgba(16,185,129,.1)':r.type==='one-to-one'?'rgba(245,158,11,.1)':'rgba(239,68,68,.1)';
    const cardColor=r.type==='many-to-one'?'#6366F1':r.type==='one-to-many'?'#10B981':r.type==='one-to-one'?'#F59E0B':'#EF4444';
    const notation=r.type==='many-to-one'?'*..1':r.type==='one-to-many'?'1..*':r.type==='one-to-one'?'1..1':'*..*';
    const fkCol=r.via_column||'\u2014';
    rb.innerHTML+='<tr style="border-bottom:1px solid var(--border);">'+
      '<td style="padding:6px 10px;font-weight:600;font-size:11px;">'+r.from+'</td>'+
      '<td style="padding:6px 10px;font-weight:600;font-size:11px;">'+r.to+'</td>'+
      '<td style="padding:6px 10px;text-align:center;">'+
        '<select class="inp" onchange="dmChangeRelType('+i+',this.value)" style="font-size:10px;padding:2px 6px;background:'+cardBg+';color:'+cardColor+';font-weight:700;border:1px solid '+cardColor+'44;border-radius:8px;">'+
        '<option value="many-to-one"'+(r.type==='many-to-one'?' selected':'')+'>*..1 Many-to-One</option>'+
        '<option value="one-to-many"'+(r.type==='one-to-many'?' selected':'')+'>1..* One-to-Many</option>'+
        '<option value="one-to-one"'+(r.type==='one-to-one'?' selected':'')+'>1..1 One-to-One</option>'+
        '<option value="many-to-many"'+(r.type==='many-to-many'?' selected':'')+'>*..* Many-to-Many</option>'+
        '</select>'+
      '</td>'+
      '<td style="padding:6px 10px;text-align:center;font-size:10px;color:var(--t3);font-family:monospace;">'+fkCol+'</td>'+
      '<td style="padding:6px 10px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmRemoveRel('+i+')" style="color:#EF4444;font-size:10px;">\u2715</button></td>'+
      '</tr>';
  });
  // Views
  const vDiv=G('dmViewsList');
  if(vDiv){
    const views=d.views||[];
    if(!views.length){
      vDiv.innerHTML='<div style="font-size:10px;color:var(--t4);padding:8px;">No views in model. Use "Load Views" or "Create View" to add.</div>';
    } else {
      vDiv.innerHTML=views.map(v=>{
        const vnEsc=v.view_name.replace(/'/g,"\\'");
        return '<div style="background:rgba(245,158,11,.06);border:1px solid #F59E0B22;border-radius:8px;padding:10px;">'+
          '<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">'+
          '<span style="font-weight:700;font-size:12px;color:var(--t1);">\ud83d\udc41 '+v.view_name+'</span>'+
          '<span style="font-size:9px;padding:1px 6px;border:1px solid #F59E0B;color:#F59E0B;border-radius:10px;">VIEW</span>'+
          '<div style="margin-left:auto;display:flex;gap:4px;">'+
          '<button class="btn btn-ghost btn-xs" onclick="dmRemoveView(\''+vnEsc+'\')" style="font-size:9px;color:#EF4444;">\u2715 Remove</button>'+
          '</div></div>'+
          (v.comment?'<div style="font-size:10px;color:var(--t3);margin-bottom:4px;"><i>'+v.comment+'</i></div>':'')+
          '<pre style="font-size:9px;background:var(--bg2);padding:6px;border-radius:6px;max-height:80px;overflow:auto;white-space:pre-wrap;color:var(--t2);">'+
          (v.definition||'-- No definition').replace(/</g,'&lt;')+'</pre></div>';
      }).join('');
    }
  }
}

function dmTableCard(tbl,role){
  const color=role==='fact'?'#3B82F6':'#10B981';
  const bgColor=role==='fact'?'rgba(59,130,246,.06)':'rgba(16,185,129,.06)';
  const tn=tbl.table_name;
  const tnEsc=tn.replace(/'/g,"\\'");
  let html='<div style="background:'+bgColor+';border:1px solid '+color+'22;border-radius:8px;padding:10px;position:relative;" id="dmCard_'+tn+'">';
  // Header: table name + role badge + actions
  html+='<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;flex-wrap:wrap;">';
  html+='<span style="font-weight:700;font-size:12px;color:var(--t1);">'+tn+'</span>';
  html+='<button class="btn btn-ghost btn-xs" onclick="dmToggleRole(\''+tnEsc+'\',\''+(role==='fact'?'dimension':'fact')+'\')" style="font-size:9px;padding:1px 6px;border:1px solid '+color+';color:'+color+';border-radius:10px;cursor:pointer;" title="Toggle role">'+role.toUpperCase()+'</button>';
  if(tbl.comment){html+='<span style="font-size:9px;color:var(--t3);font-style:italic;" title="'+tbl.comment.replace(/"/g,'&quot;')+'">'+tbl.comment.substring(0,30)+(tbl.comment.length>30?'...':'')+'</span>';}
  html+='<div style="margin-left:auto;display:flex;gap:4px;">';
  html+='<button class="btn btn-ghost btn-xs" onclick="dmRenameTableDialog(\''+tnEsc+'\')" style="font-size:9px;color:var(--t3);" title="Rename table">\u270E Rename</button>';
  html+='<button class="btn btn-ghost btn-xs" onclick="dmRemoveTable(\''+tnEsc+'\')" style="font-size:9px;color:#EF4444;" title="Remove table">\u2715 Remove</button>';
  html+='</div></div>';
  // Column table
  html+='<table style="width:100%;font-size:10px;border-collapse:collapse;">';
  html+='<thead><tr style="background:rgba(0,0,0,.03);"><th style="padding:3px 4px;text-align:left;font-weight:600;color:var(--t3);">Column</th><th style="padding:3px 4px;text-align:left;font-weight:600;color:var(--t3);">Type</th><th style="padding:3px 4px;text-align:center;font-weight:600;color:var(--t3);">PK</th><th style="padding:3px 4px;text-align:center;font-weight:600;color:var(--t3);">UQ</th><th style="padding:3px 4px;text-align:center;font-weight:600;color:var(--t3);">FK</th><th style="padding:3px 4px;text-align:center;font-weight:600;color:var(--t3);">Null</th><th style="padding:3px 4px;text-align:left;font-weight:600;color:var(--t3);">Comment</th><th style="padding:3px 4px;text-align:center;font-weight:600;color:var(--t3);">Actions</th></tr></thead>';
  html+='<tbody>';
  (tbl.columns||[]).forEach(c=>{
    const cnEsc=c.name.replace(/'/g,"\\'");
    const pk=c.is_pk;
    const uq=c.is_unique;
    const fk=c.fk_table;
    const nl=c.is_nullable;
    const cmt=c.comment||'';
    html+='<tr style="border-bottom:1px solid rgba(0,0,0,.05);">';
    html+='<td style="padding:3px 4px;">'+(pk?'<span style="color:#F59E0B;" title="PK">\ud83d\udd11</span> ':'')+(fk?'<span style="color:#6366F1;" title="FK\u2192'+fk+'">\ud83d\udd17</span> ':'')+c.name+'</td>';
    html+='<td style="padding:3px 4px;color:var(--t3);">'+c.data_type+'</td>';
    html+='<td style="padding:3px 4px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmToggleColPK(\''+tnEsc+'\',\''+cnEsc+'\','+(!pk)+')" style="font-size:9px;padding:0 4px;color:'+(pk?'#F59E0B':'var(--t4)')+';">'+(pk?'\u2605':'\u2606')+'</button></td>';
    html+='<td style="padding:3px 4px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmToggleColUnique(\''+tnEsc+'\',\''+cnEsc+'\','+(!uq)+')" style="font-size:9px;padding:0 4px;color:'+(uq?'#8B5CF6':'var(--t4)')+';">'+(uq?'\ud83c\udfaf':'\u25cb')+'</button></td>';
    html+='<td style="padding:3px 4px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmERSetFK(\''+tnEsc+'\',\''+cnEsc+'\',\''+(fk||'').replace(/'/g,"\\'")+'\')" style="font-size:9px;padding:0 4px;color:'+(fk?'#6366F1':'var(--t4)')+';">'+(fk?fk:'\u2014')+'</button></td>';
    html+='<td style="padding:3px 4px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmToggleColNull(\''+tnEsc+'\',\''+cnEsc+'\','+(!nl)+')" style="font-size:9px;padding:0 4px;color:'+(nl?'#10B981':'var(--t4)')+';">'+(nl?'\u2714':'\u2718')+'</button></td>';
    html+='<td style="padding:3px 4px;font-size:9px;color:var(--t3);max-width:80px;overflow:hidden;text-overflow:ellipsis;" title="'+cmt.replace(/"/g,'&quot;')+'">'+cmt.substring(0,20)+(cmt.length>20?'...':'')+'</td>';
    html+='<td style="padding:3px 4px;text-align:center;display:flex;gap:2px;justify-content:center;">';
    html+='<button class="btn btn-ghost btn-xs" onclick="dmEditColDialog(\''+tnEsc+'\',\''+cnEsc+'\')" style="font-size:9px;padding:0 3px;color:var(--t3);" title="Edit">\u270E</button>';
    html+='<button class="btn btn-ghost btn-xs" onclick="dmRemoveCol(\''+tnEsc+'\',\''+cnEsc+'\')" style="font-size:9px;padding:0 3px;color:#EF4444;" title="Remove">\u2715</button>';
    html+='</td></tr>';
  });
  html+='</tbody></table>';
  // Add column button
  html+='<button class="btn btn-ghost btn-xs" onclick="dmAddColDialog(\''+tnEsc+'\')" style="margin-top:4px;font-size:9px;color:'+color+';border:1px dashed '+color+'44;width:100%;padding:3px;">+ Add Column</button>';
  html+='</div>';
  return html;
}

// ── Generic Edit API Helper ─────────────────────────────────────────────────
async function _dmEdit(edits, successMsg){
  if(!_dmModelId){toast('Generate a model first','terr');return;}
  // Save current positions before API call overwrites er_json
  if(_dmErJson&&_dmErJson.nodes){
    if(typeof _dmSavedPositions==='undefined')window._dmSavedPositions={};
    _dmErJson.nodes.forEach(n=>{if(n.x!==undefined&&n.y!==undefined)_dmSavedPositions[n.id]={x:n.x,y:n.y};});
  }
  try{
    const r=await fetch('/api/v1/datamodel/edit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
      model_id:_dmModelId, edits:edits,
      catalog:G('dmCatalog').value, schema:G('dmSchema').value
    })});
    const d=await r.json();
    if(d.success){
      _dmModel=d;_dmModelId=d.model_id;_dmErJson=d.er_json;_dmDdl=d.ddl;
      G('dmKpiFacts').textContent=d.facts.length;
      G('dmKpiDims').textContent=d.dimensions.length;
      G('dmKpiTables').textContent=d.facts.length+d.dimensions.length;
      G('dmKpiSchema').textContent=d.schema_type==='star'?'\u2b50 Star':'\u2744 Snowflake';
      G('dmSchemaTypeBadge').textContent=d.schema_type.toUpperCase()+' SCHEMA';
      G('dmSchemaTypeBadge').style.background=d.schema_type==='star'?'rgba(245,158,11,.15)':'rgba(59,130,246,.15)';
      G('dmSchemaTypeBadge').style.color=d.schema_type==='star'?'#F59E0B':'#3B82F6';
      dmRenderER(d.er_json);dmRenderDetails(d);G('dmDdlCode').textContent=d.ddl;
      if(typeof _dmRenderInsights==='function') _dmRenderInsights(d);
      if(successMsg)toast(successMsg,'tok');
    }else{toast(d.error||'Edit failed','terr');}
  }catch(e){toast('Edit failed: '+e.message,'terr');}
}

// ── Manual Edits ────────────────────────────────────────────────────────────
// ── Role Toggle ─────────────────────────────────────────────────────────────
async function dmToggleRole(tableName,newRole){
  await _dmEdit({role_changes:[{table_name:tableName,new_role:newRole}]},tableName+' moved to '+newRole);
}

// ── Relationship Remove / Add ───────────────────────────────────────────────
async function dmRemoveRel(idx){
  if(!_dmModel||!_dmModel.relationships)return;
  const rel=_dmModel.relationships[idx];if(!rel)return;
  await _dmEdit({relationship_removes:[{from:rel.from,to:rel.to}]},'Relationship removed');
}

async function dmChangeRelType(idx, newType){
  if(!_dmModel||!_dmModel.relationships)return;
  const rel=_dmModel.relationships[idx];if(!rel)return;
  if(rel.type===newType)return;
  await _dmEdit({
    relationship_removes:[{from:rel.from,to:rel.to}],
    relationship_adds:[{from:rel.from,to:rel.to,type:newType}]
  },'Cardinality changed to '+newType);
}

function dmShowAddRelDialog(){
  if(!_dmModel)return;
  const allTables=[..._dmModel.facts,..._dmModel.dimensions].map(t=>t.table_name);
  const fromOpts=allTables.map(t=>'<option value="'+t+'">'+t+'</option>').join('');
  const html='<div style="display:flex;gap:8px;align-items:end;margin-top:8px;padding:10px;background:var(--bg2);border-radius:8px;" id="dmAddRelRow">'+
    '<div><label style="font-size:10px;font-weight:600;">From</label><select class="inp" id="dmNewRelFrom" style="font-size:11px;">'+fromOpts+'</select></div>'+
    '<div><label style="font-size:10px;font-weight:600;">To</label><select class="inp" id="dmNewRelTo" style="font-size:11px;">'+fromOpts+'</select></div>'+
    '<div><label style="font-size:10px;font-weight:600;">Type</label><select class="inp" id="dmNewRelType" style="font-size:11px;"><option>many-to-one</option><option>one-to-one</option><option>many-to-many</option></select></div>'+
    '<button class="btn btn-primary btn-xs" onclick="dmAddRel()">Add</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="G(\'dmAddRelRow\').remove()">Cancel</button>'+
    '</div>';
  const existing=G('dmAddRelRow');if(existing)existing.remove();
  G('dmRelsBody').closest('div').insertAdjacentHTML('beforeend',html);
}

async function dmAddRel(){
  const from=G('dmNewRelFrom').value,to=G('dmNewRelTo').value,type=G('dmNewRelType').value;
  if(from===to){toast('From and To must be different','terr');return;}
  await _dmEdit({relationship_adds:[{from,to,type}]},'Relationship added');
  const row=G('dmAddRelRow');if(row)row.remove();
}

// ── Remove Table ────────────────────────────────────────────────────────────
async function dmRemoveTable(tableName){
  if(!(await uiConfirm('Remove table "'+tableName+'" from the model?',{danger:true})))return;
  await _dmEdit({table_removes:[tableName]},tableName+' removed');
}

// ── Rename Table Dialog ─────────────────────────────────────────────────────
function dmRenameTableDialog(tableName){
  const newName=prompt('New name for table "'+tableName+'":',tableName);
  if(!newName||newName===tableName)return;
  _dmEdit({table_renames:[{old_name:tableName,new_name:newName}]},tableName+' renamed to '+newName);
}

// ── Column Editing ──────────────────────────────────────────────────────────
function dmEditColDialog(tableName,colName){
  // Find current column data
  const allTbls=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])];
  const tbl=allTbls.find(t=>t.table_name===tableName);
  if(!tbl)return;
  const col=(tbl.columns||[]).find(c=>c.name===colName);
  if(!col)return;
  // Build a small inline dialog
  const dlgId='dmColEditDlg_'+tableName+'_'+colName;
  let existing=document.getElementById(dlgId);if(existing)existing.remove();
  const types=['STRING','INT','BIGINT','SMALLINT','TINYINT','DOUBLE','FLOAT','DECIMAL(18,2)','DECIMAL(10,2)','BOOLEAN','DATE','TIMESTAMP','BINARY','ARRAY<STRING>','MAP<STRING,STRING>'];
  const typeOpts=types.map(t=>'<option value="'+t+'"'+(col.data_type.toUpperCase()===t?' selected':'')+'>'+t+'</option>').join('');
  const card=document.getElementById('dmCard_'+tableName);
  if(!card)return;
  const html='<div id="'+dlgId+'" style="background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:8px;margin-top:4px;">'+
    '<div style="font-size:10px;font-weight:700;margin-bottom:4px;">Edit Column: '+colName+'</div>'+
    '<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:end;">'+
    '<div><label style="font-size:9px;">Name</label><input class="inp" id="'+dlgId+'_name" value="'+col.name+'" style="font-size:10px;width:100px;"></div>'+
    '<div><label style="font-size:9px;">Type</label><select class="inp" id="'+dlgId+'_type" style="font-size:10px;width:110px;">'+typeOpts+'</select></div>'+
    '<div><label style="font-size:9px;">Comment</label><input class="inp" id="'+dlgId+'_comment" value="'+(col.comment||'').replace(/"/g,'&quot;')+'" placeholder="Column comment" style="font-size:10px;width:120px;"></div>'+
    '<label style="font-size:9px;"><input type="checkbox" id="'+dlgId+'_unique" '+(col.is_unique?'checked':'')+'>UQ</label>'+
    '<button class="btn btn-primary btn-xs" onclick="dmSaveColEdit(\''+tableName.replace(/'/g,"\\'")+'\',\''+colName.replace(/'/g,"\\'")+'\',\''+dlgId+'\')">Save</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\''+dlgId+'\').remove()">Cancel</button>'+
    '</div></div>';
  card.insertAdjacentHTML('beforeend',html);
}

async function dmSaveColEdit(tableName,oldColName,dlgId){
  const newName=document.getElementById(dlgId+'_name').value.trim();
  const newType=document.getElementById(dlgId+'_type').value;
  const newComment=document.getElementById(dlgId+'_comment').value.trim();
  const newUnique=document.getElementById(dlgId+'_unique').checked;
  if(!newName){toast('Column name required','terr');return;}
  const edits={column_edits:[]};
  if(newName!==oldColName)edits.column_edits.push({table_name:tableName,column_name:oldColName,field:'name',value:newName});
  const allTbls=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])];
  const tbl=allTbls.find(t=>t.table_name===tableName);
  const col=tbl?(tbl.columns||[]).find(c=>c.name===oldColName):null;
  if(col&&col.data_type.toUpperCase()!==newType)edits.column_edits.push({table_name:tableName,column_name:newName||oldColName,field:'data_type',value:newType});
  if(col&&(col.comment||'')!==newComment)edits.column_edits.push({table_name:tableName,column_name:newName||oldColName,field:'comment',value:newComment});
  if(col&&!!col.is_unique!==newUnique)edits.column_edits.push({table_name:tableName,column_name:newName||oldColName,field:'is_unique',value:newUnique});
  if(edits.column_edits.length===0){document.getElementById(dlgId).remove();return;}
  await _dmEdit(edits,'Column updated');
}

async function dmToggleColPK(tableName,colName,newVal){
  await _dmEdit({column_edits:[{table_name:tableName,column_name:colName,field:'is_pk',value:newVal}]},'PK toggled');
}

async function dmToggleColUnique(tableName,colName,newVal){
  await _dmEdit({column_edits:[{table_name:tableName,column_name:colName,field:'is_unique',value:newVal}]},'Unique constraint toggled');
}

async function dmToggleColNull(tableName,colName,newVal){
  await _dmEdit({column_edits:[{table_name:tableName,column_name:colName,field:'is_nullable',value:newVal}]},'Nullable toggled');
}

async function dmRemoveCol(tableName,colName){
  if(!(await uiConfirm('Remove column "'+colName+'" from '+tableName+'?',{danger:true})))return;
  await _dmEdit({column_removes:[{table_name:tableName,column_name:colName}]},'Column removed');
}

// ── Add Column Dialog ───────────────────────────────────────────────────────
function dmAddColDialog(tableName){
  const dlgId='dmAddColDlg_'+tableName;
  let existing=document.getElementById(dlgId);if(existing)existing.remove();
  const card=document.getElementById('dmCard_'+tableName);if(!card)return;
  const types=['STRING','INT','BIGINT','SMALLINT','TINYINT','DOUBLE','FLOAT','DECIMAL(18,2)','DECIMAL(10,2)','BOOLEAN','DATE','TIMESTAMP','BINARY'];
  const typeOpts=types.map(t=>'<option value="'+t+'">'+t+'</option>').join('');
  const html='<div id="'+dlgId+'" style="background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:8px;margin-top:4px;">'+
    '<div style="font-size:10px;font-weight:700;margin-bottom:4px;">Add Column to '+tableName+'</div>'+
    '<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:end;">'+
    '<div><label style="font-size:9px;">Name</label><input class="inp" id="'+dlgId+'_name" placeholder="column_name" style="font-size:10px;width:100px;"></div>'+
    '<div><label style="font-size:9px;">Type</label><select class="inp" id="'+dlgId+'_type" style="font-size:10px;width:110px;">'+typeOpts+'</select></div>'+
    '<label style="font-size:9px;"><input type="checkbox" id="'+dlgId+'_pk"> PK</label>'+
    '<label style="font-size:9px;"><input type="checkbox" id="'+dlgId+'_uq"> UQ</label>'+
    '<label style="font-size:9px;"><input type="checkbox" id="'+dlgId+'_null" checked> Null</label>'+
    '<div><label style="font-size:9px;">Comment</label><input class="inp" id="'+dlgId+'_comment" placeholder="" style="font-size:9px;width:90px;"></div>'+
    '<button class="btn btn-primary btn-xs" onclick="dmSaveNewCol(\''+tableName.replace(/'/g,"\\'")+'\',\''+dlgId+'\')">Add</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\''+dlgId+'\').remove()">Cancel</button>'+
    '</div></div>';
  card.insertAdjacentHTML('beforeend',html);
}

async function dmSaveNewCol(tableName,dlgId){
  const name=document.getElementById(dlgId+'_name').value.trim();
  const dtype=document.getElementById(dlgId+'_type').value;
  const pk=document.getElementById(dlgId+'_pk').checked;
  const uq=document.getElementById(dlgId+'_uq').checked;
  const nl=document.getElementById(dlgId+'_null').checked;
  const comment=document.getElementById(dlgId+'_comment').value.trim();
  if(!name){toast('Column name required','terr');return;}
  await _dmEdit({column_adds:[{table_name:tableName,column:{name:name,data_type:dtype,is_pk:pk,is_unique:uq,is_nullable:nl,comment:comment}}]},'Column "'+name+'" added');
}

// ── Add New Table Dialog ────────────────────────────────────────────────────
function dmShowAddTableDialog(){
  if(!_dmModel){toast('Generate a model first','terr');return;}
  let dlg=G('dmAddTableDlg');if(dlg){dlg.remove();}
  const html='<div id="dmAddTableDlg" style="position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:9999;background:var(--bg1);border:2px solid #F59E0B;border-radius:12px;padding:20px;width:420px;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.3);">'+
    '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">'+
    '<span style="font-weight:700;font-size:14px;color:#F59E0B;">\u2795 Add New Table</span>'+
    '<button class="btn btn-ghost btn-xs" onclick="G(\'dmAddTableDlg\').remove()" style="font-size:14px;">\u2715</button></div>'+
    '<div style="display:flex;gap:8px;margin-bottom:8px;">'+
    '<div style="flex:1;"><label style="font-size:10px;font-weight:600;">Table Name</label><input class="inp" id="dmNewTblName" placeholder="my_table" style="font-size:11px;width:100%;"></div>'+
    '<div><label style="font-size:10px;font-weight:600;">Role</label><select class="inp" id="dmNewTblRole" style="font-size:11px;"><option value="fact">Fact</option><option value="dimension">Dimension</option></select></div>'+
    '</div>'+
    '<div style="font-size:10px;font-weight:700;margin-bottom:4px;">Columns</div>'+
    '<div id="dmNewTblCols" style="margin-bottom:6px;"></div>'+
    '<button class="btn btn-ghost btn-xs" onclick="dmNewTblAddColRow()" style="margin-bottom:10px;font-size:9px;border:1px dashed #F59E0B44;color:#F59E0B;width:100%;">+ Add Column</button>'+
    '<div style="display:flex;gap:8px;">'+
    '<button class="btn btn-primary btn-sm" onclick="dmSaveNewTable()" style="background:#F59E0B;border-color:#F59E0B;flex:1;">Create Table</button>'+
    '<button class="btn btn-ghost btn-sm" onclick="G(\'dmAddTableDlg\').remove()">Cancel</button></div></div>';
  document.body.insertAdjacentHTML('beforeend',html);
  // Add initial column row
  dmNewTblAddColRow();
}

let _dmNewTblColIdx=0;
function dmNewTblAddColRow(){
  const idx=_dmNewTblColIdx++;
  const types=['STRING','INT','BIGINT','SMALLINT','TINYINT','DOUBLE','FLOAT','DECIMAL(18,2)','DECIMAL(10,2)','BOOLEAN','DATE','TIMESTAMP','BINARY'];
  const typeOpts=types.map(t=>'<option value="'+t+'">'+t+'</option>').join('');
  const html='<div style="display:flex;gap:4px;align-items:center;margin-bottom:4px;" id="dmNewTblColRow_'+idx+'">'+
    '<input class="inp dmNewTblColName" placeholder="col_name" style="font-size:10px;flex:1;">'+
    '<select class="inp dmNewTblColType" style="font-size:10px;width:100px;">'+typeOpts+'</select>'+
    '<label style="font-size:8px;white-space:nowrap;"><input type="checkbox" class="dmNewTblColPK"> PK</label>'+
    '<label style="font-size:8px;white-space:nowrap;"><input type="checkbox" class="dmNewTblColUQ"> UQ</label>'+
    '<label style="font-size:8px;white-space:nowrap;"><input type="checkbox" class="dmNewTblColNull" checked> Null</label>'+
    '<input class="inp dmNewTblColComment" placeholder="comment" style="font-size:9px;width:80px;">'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\'dmNewTblColRow_'+idx+'\').remove()" style="color:#EF4444;font-size:10px;padding:0 3px;">\u2715</button>'+
    '</div>';
  G('dmNewTblCols').insertAdjacentHTML('beforeend',html);
}

async function dmSaveNewTable(){
  const name=G('dmNewTblName').value.trim();
  const role=G('dmNewTblRole').value;
  if(!name){toast('Table name required','terr');return;}
  const colRows=G('dmNewTblCols').children;
  const columns=[];
  for(let i=0;i<colRows.length;i++){
    const row=colRows[i];
    const cn=row.querySelector('.dmNewTblColName').value.trim();
    if(!cn)continue;
    columns.push({
      name:cn,
      data_type:row.querySelector('.dmNewTblColType').value,
      is_pk:row.querySelector('.dmNewTblColPK').checked,
      is_unique:row.querySelector('.dmNewTblColUQ').checked,
      is_nullable:row.querySelector('.dmNewTblColNull').checked,
      comment:row.querySelector('.dmNewTblColComment').value.trim()
    });
  }
  if(columns.length===0){toast('Add at least one column','terr');return;}
  await _dmEdit({table_adds:[{table_name:name,role:role,columns:columns}]},'Table "'+name+'" added');
  G('dmAddTableDlg').remove();
}

// ── Downloads ───────────────────────────────────────────────────────────────

// Helper: clone SVG with correct sizing so all content fits in the exported file
function _dmCloneForExport(){
  const svg=G('dmErSvg');
  const grp=G('dmErGroup');
  if(!svg||!grp)return null;
  // Get bounding box of all content at current transform
  const bbox=grp.getBBox();
  const pad=40;
  const exportW=Math.ceil(bbox.x+bbox.width+pad*2);
  const exportH=Math.ceil(bbox.y+bbox.height+pad*2);
  const clone=svg.cloneNode(true);
  const cloneGrp=clone.querySelector('#dmErGroup');
  // Reset transform: translate so content starts at (pad, pad), scale 1
  cloneGrp.setAttribute('transform','translate('+(pad-bbox.x)+','+(pad-bbox.y)+') scale(1)');
  // Set SVG dimensions to fit all content
  const totalW=Math.ceil(bbox.width+pad*2);
  const totalH=Math.ceil(bbox.height+pad*2);
  clone.setAttribute('width', totalW);
  clone.setAttribute('height', totalH);
  clone.setAttribute('viewBox', '0 0 '+totalW+' '+totalH);
  clone.style.minWidth='';clone.style.minHeight='';
  clone.setAttribute('xmlns','http://www.w3.org/2000/svg');
  return {clone, width:totalW, height:totalH};
}

function dmDownloadER(){
  const exp=_dmCloneForExport();
  if(!exp)return;
  const xml=new XMLSerializer().serializeToString(exp.clone);
  const svgBlob=new Blob(['<?xml version="1.0" encoding="UTF-8"?>'+xml],{type:'image/svg+xml'});
  const url=URL.createObjectURL(svgBlob);
  const img=new Image();
  img.onload=function(){
    const canvas=document.createElement('canvas');
    canvas.width=exp.width*2;canvas.height=exp.height*2;
    const ctx=canvas.getContext('2d');
    ctx.scale(2,2);ctx.fillStyle='#FFFFFF';ctx.fillRect(0,0,exp.width,exp.height);
    ctx.drawImage(img,0,0,exp.width,exp.height);
    canvas.toBlob(function(blob){
      const a=document.createElement('a');a.href=URL.createObjectURL(blob);
      a.download='data_model_er_diagram.png';a.click();URL.revokeObjectURL(a.href);
    },'image/png');
    URL.revokeObjectURL(url);
  };
  img.src=url;
}

function dmDownloadSVG(){
  const exp=_dmCloneForExport();
  if(!exp)return;
  const xml=new XMLSerializer().serializeToString(exp.clone);
  const blob=new Blob(['<?xml version="1.0" encoding="UTF-8"?>'+xml],{type:'image/svg+xml'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  a.download='data_model_er_diagram.svg';a.click();URL.revokeObjectURL(a.href);
}

function dmDownloadDDL(){
  if(!_dmDdl)return;
  const blob=new Blob([_dmDdl],{type:'text/sql'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  a.download='data_model_ddl.sql';a.click();URL.revokeObjectURL(a.href);
}

function dmCopyDDL(){
  if(!_dmDdl)return;
  navigator.clipboard.writeText(_dmDdl).then(()=>toast('DDL copied to clipboard','tok')).catch(()=>toast('Copy failed','terr'));
}

// ── Push to Azure DevOps ─────────────────────────────────────────────────────
async function dmPushToDevOps(){
  if(!_dmDdl && !_dmErJson){toast('Generate a model first','terr');return;}
  const org=(G('cfgDevOpsOrg')||{}).value||'';
  const project=(G('cfgDevOpsProject')||{}).value||'';
  const repo=(G('cfgDevOpsRepo')||{}).value||'';
  const branch=(G('cfgDevOpsBranch')||{}).value||'main';
  const reviewers=(G('cfgDevOpsReviewers')||{}).value||'';
  if(!org||!project||!repo){toast('Configure Azure DevOps in Settings first (org/project/repo)','terr');return;}
  // Capture ER diagram as PNG base64
  let erBase64='';
  try{
    const exp=_dmCloneForExport();
    if(exp){
      const xml=new XMLSerializer().serializeToString(exp.clone);
      const svgBlob=new Blob(['<?xml version="1.0" encoding="UTF-8"?>'+xml],{type:'image/svg+xml'});
      const url=URL.createObjectURL(svgBlob);
      const img=new Image();
      erBase64=await new Promise((resolve)=>{
        img.onload=function(){
          const canvas=document.createElement('canvas');
          canvas.width=exp.width*2;canvas.height=exp.height*2;
          const ctx=canvas.getContext('2d');
          ctx.scale(2,2);ctx.fillStyle='#FFFFFF';ctx.fillRect(0,0,exp.width,exp.height);
          ctx.drawImage(img,0,0,exp.width,exp.height);
          resolve(canvas.toDataURL('image/png'));
          URL.revokeObjectURL(url);
        };
        img.onerror=function(){resolve('');URL.revokeObjectURL(url);};
        img.src=url;
      });
    }
  }catch(e){console.warn('Could not capture ER as PNG:',e);}
  // Model name from catalog + schema
  const modelName=(G('dmCatalog').value||'model')+'_'+(G('dmSchema').value||'schema');

  // Show PR approval modal
  const modalHtml=`
    <div id="dmPrModal" style="position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.5);">
      <div style="background:#1e1e2e;border:1px solid #333;border-radius:12px;padding:28px 32px;width:480px;max-width:95vw;color:#e0e0e0;">
        <h3 style="margin:0 0 16px;color:#60a5fa;">Push to Azure DevOps</h3>
        <label style="font-size:12px;color:#9ca3af;">Commit Message</label>
        <input id="dmPrCommitMsg" class="input" style="width:100%;margin-bottom:12px;" value="Update data model: ${modelName}">
        <label style="font-size:12px;color:#9ca3af;">Push Mode</label>
        <select id="dmPrMode" class="input" style="width:100%;margin-bottom:12px;">
          <option value="pr" selected>Create Pull Request (requires approval)</option>
          <option value="direct">Direct Push (Admin only)</option>
        </select>
        <div id="dmPrReviewerSection">
          <label style="font-size:12px;color:#9ca3af;">Reviewers (comma-separated emails)</label>
          <input id="dmPrReviewers" class="input" style="width:100%;margin-bottom:12px;" placeholder="user1@company.com, user2@company.com" value="${reviewers}">
          <label style="font-size:12px;color:#9ca3af;display:flex;align-items:center;gap:6px;margin-bottom:12px;">
            <input type="checkbox" id="dmPrAutoComplete" checked> Auto-complete when approved
          </label>
        </div>
        <div style="display:flex;gap:10px;justify-content:flex-end;margin-top:16px;">
          <button class="btn btn-ghost" onclick="G('dmPrModal').remove()">Cancel</button>
          <button class="btn btn-primary" id="dmPrSubmitBtn" onclick="dmPrSubmit()">Create PR</button>
        </div>
      </div>
    </div>`;
  document.body.insertAdjacentHTML('beforeend',modalHtml);

  // Toggle reviewer section based on mode
  G('dmPrMode').onchange=function(){
    const sec=G('dmPrReviewerSection');
    const btn=G('dmPrSubmitBtn');
    if(this.value==='direct'){sec.style.display='none';btn.textContent='Push Directly';}
    else{sec.style.display='block';btn.textContent='Create PR';}
  };

  // Store data for submit
  window._dmPrPayload={org,project,repo,branch,modelName,erBase64};
}

async function dmPrSubmit(){
  const {org,project,repo,branch,modelName,erBase64}=window._dmPrPayload||{};
  const commitMsg=G('dmPrCommitMsg').value.trim()||'Update data model';
  const pushMode=G('dmPrMode').value;
  const reviewersStr=G('dmPrReviewers')?.value||'';
  const autoComplete=G('dmPrAutoComplete')?.checked!==false;
  const modal=G('dmPrModal');
  if(modal)modal.remove();

  toast(pushMode==='pr'?'Creating Pull Request…':'Pushing to DevOps…','tinfo');
  try{
    const r=await fetch('/api/v1/datamodel/push-devops',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        org,project,repo,branch,
        push_mode:pushMode,
        reviewers:reviewersStr,
        auto_complete:autoComplete,
        ddl:_dmDdl||'',
        er_image_base64:erBase64,
        model_json:_dmModel||null,
        commit_message:commitMsg,
        model_name:modelName,
      })});
    const d=await r.json();
    if(d.success){
      if(d.mode==='pr'){
        toast('PR #'+d.pr_id+' created! Awaiting approval.','tok',6000);
        if(d.pr_url){window.open(d.pr_url,'_blank');}
        // Show PR status badge
        _dmShowPrBadge(d.pr_id,d.pr_url,d.reviewers||[]);
      }else{
        toast('Pushed directly! Commit: '+(d.commit_id||'').substring(0,8),'tok',5000);
        if(d.url){window.open(d.url,'_blank');}
      }
    }else{
      toast(d.error||'Push failed','terr',6000);
    }
  }catch(e){toast('Error: '+e.message,'terr');}
}

function _dmShowPrBadge(prId,prUrl,reviewers){
  const existing=G('dmPrBadge');if(existing)existing.remove();
  const badge=document.createElement('div');
  badge.id='dmPrBadge';
  badge.style.cssText='position:fixed;bottom:20px;right:20px;background:#1e3a5f;border:1px solid #3b82f6;border-radius:10px;padding:14px 18px;color:#e0e0e0;z-index:9000;font-size:13px;max-width:320px;';
  badge.innerHTML=`
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
      <span style="width:8px;height:8px;border-radius:50%;background:#f59e0b;display:inline-block;"></span>
      <strong>PR #${prId} — Awaiting Approval</strong>
    </div>
    <div style="font-size:11px;color:#9ca3af;margin-bottom:8px;">Reviewers: ${reviewers.length?reviewers.join(', '):'(configured in DevOps)'}</div>
    <a href="${prUrl}" target="_blank" style="color:#60a5fa;font-size:12px;">View in Azure DevOps →</a>
    <button onclick="dmCheckPrStatus(${prId})" class="btn btn-ghost btn-xs" style="margin-left:8px;font-size:11px;">Check Status</button>
    <button onclick="G('dmPrBadge').remove()" style="position:absolute;top:6px;right:10px;background:none;border:none;color:#9ca3af;cursor:pointer;">✕</button>
  `;
  document.body.appendChild(badge);
}

async function dmCheckPrStatus(prId){
  try{
    const r=await fetch('/api/v1/datamodel/pr-status',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({pr_id:prId})});
    const d=await r.json();
    if(d.success){
      const statusMap={active:'⏳ Active',completed:'✅ Completed',abandoned:'❌ Abandoned'};
      const voteMap={10:'✅ Approved',5:'✅ Approved w/suggestions','-5':'⏳ Waiting','-10':'❌ Rejected','0':'— No vote'};
      let msg=statusMap[d.status]||d.status;
      if(d.reviewers&&d.reviewers.length){
        msg+='\\n'+d.reviewers.map(rv=>rv.name+': '+(voteMap[rv.vote]||'Pending')).join('\\n');
      }
      toast(msg,'tinfo',6000);
      // Update badge color
      const badge=G('dmPrBadge');
      if(badge){
        const dot=badge.querySelector('span');
        if(d.status==='completed'){dot.style.background='#16a34a';badge.querySelector('strong').textContent='PR #'+prId+' — Merged ✓';}
        else if(d.status==='abandoned'){dot.style.background='#dc2626';badge.querySelector('strong').textContent='PR #'+prId+' — Abandoned';}
      }
    }else{toast(d.error||'Status check failed','terr');}
  }catch(e){toast(e.message,'terr');}
}

// ── Test DevOps Connection ───────────────────────────────────────────────────
async function cfgTestDevOps(){
  const org=(G('cfgDevOpsOrg')||{}).value?.trim()||'';
  const project=(G('cfgDevOpsProject')||{}).value?.trim()||'';
  const repo=(G('cfgDevOpsRepo')||{}).value?.trim()||'';
  const status=G('cfgDevOpsStatus');
  if(!org||!project||!repo){toast('Fill in org, project, and repo','terr');return;}
  const btn=G('btnTestDevOps');btn.disabled=true;btn.textContent='Testing…';
  if(status)status.textContent='';
  try{
    const r=await fetch('/api/v1/datamodel/test-devops',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({org,project,repo})});
    const d=await r.json();
    if(d.success){
      if(status){status.style.color='#16a34a';status.textContent='✓ Connected — repo: '+d.repo_name+', default branch: '+d.default_branch;}
      toast('DevOps connection successful!','tok');
    }else{
      if(status){status.style.color='#dc2626';status.textContent='✕ '+d.error;}
      toast(d.error||'Connection failed','terr');
    }
  }catch(e){
    if(status){status.style.color='#dc2626';status.textContent='✕ '+e.message;}
    toast(e.message,'terr');
  }finally{btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Test Connection';}
}

// Init on load
dmInit();

/* ═══════════ ADMIN — USER MANAGEMENT ═══════════ */
window.adminRefresh=async function(){
  const tbody=G('adminUserTbody');
  if(!tbody)return;
  tbody.innerHTML='<tr><td colspan="4" style="text-align:center;padding:40px;color:var(--t4)">Loading…</td></tr>';
  try{
    const r=await fetch('/api/v1/admin/users');
    if(r.status===403){tbody.innerHTML='<tr><td colspan="4" style="text-align:center;padding:40px;color:#ef4444;">Admin access required.</td></tr>';return;}
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    const users=d.users||[];
    window._adminUsers=users;
    /* Stats */
    const sTotal=G('adminStatTotal'),sAdm=G('adminStatAdmins'),sDev=G('adminStatDevs'),sViw=G('adminStatViewers'),sCnt=G('adminUserCount');
    if(sTotal)sTotal.textContent=users.length;
    if(sAdm)sAdm.textContent=users.filter(u=>u.role==='Admin').length;
    if(sDev)sDev.textContent=users.filter(u=>u.role==='Developer').length;
    if(sViw)sViw.textContent=users.filter(u=>u.role==='Viewer').length;
    if(sCnt)sCnt.textContent=users.length;
    /* Table */
    if(!users.length){tbody.innerHTML='<tr><td colspan="4" style="text-align:center;padding:40px;color:var(--t4)">No users found.</td></tr>';return;}
    const roleBadge=r=>({Admin:'background:rgba(239,68,68,.1);color:#EF4444;border:1px solid rgba(239,68,68,.2)',Developer:'background:rgba(59,130,246,.1);color:#3B82F6;border:1px solid rgba(59,130,246,.2)',Viewer:'background:rgba(34,197,94,.1);color:#22C55E;border:1px solid rgba(34,197,94,.2)'}[r]||'background:var(--surface-2);color:var(--t3)');
    const avatar=u=>{const n=(u.display_name||u.username||'?');const i=n.split(' ').map(w=>w[0]).join('').slice(0,2).toUpperCase();const colors=['#3B82F6','#8B5CF6','#EC4899','#F59E0B','#10B981','#EF4444'];const c=colors[n.charCodeAt(0)%colors.length];return '<div style="width:32px;height:32px;border-radius:50%;background:'+c+';color:#fff;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;flex-shrink:0;">'+i+'</div>';};
    tbody.innerHTML=users.map(u=>`<tr style="border-bottom:1px solid var(--border);transition:background .15s;" onmouseenter="this.style.background='var(--surface-2)'" onmouseleave="this.style.background=''">
      <td style="padding:10px 18px;"><div style="display:flex;align-items:center;gap:10px;">${avatar(u)}<div><div style="font-weight:600;color:var(--t1);font-size:12px;">${_esc(u.username)}</div></div></div></td>
      <td style="padding:10px 18px;color:var(--t2);font-size:12px;">${_esc(u.display_name||'—')}</td>
      <td style="padding:10px 18px;"><span style="display:inline-block;padding:3px 12px;border-radius:9999px;font-size:11px;font-weight:600;${roleBadge(u.role)}">${_esc(u.role)}</span></td>
      <td style="padding:10px 18px;text-align:center;white-space:nowrap;">
        <button class="btn btn-ghost btn-xs" onclick="adminOpenEdit('${_esc(u.username)}','${_esc(u.display_name||'')}','${_esc(u.role)}')" title="Edit" style="padding:4px 8px;">
          <svg viewBox="0 0 24 24" style="width:13px;height:13px;"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
          Edit
        </button>
        <button class="btn btn-ghost btn-xs" onclick="adminResetPw('${_esc(u.username)}')" title="Reset Password" style="padding:4px 8px;">
          <svg viewBox="0 0 24 24" style="width:13px;height:13px;"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>
          Reset
        </button>
        <button class="btn btn-ghost btn-xs" style="color:#ef4444;padding:4px 8px;" onclick="adminDeleteUser('${_esc(u.username)}')" title="Delete">
          <svg viewBox="0 0 24 24" style="width:13px;height:13px;"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
          Delete
        </button>
      </td>
    </tr>`).join('');
  }catch(e){toast(e.message,'terr');}
};
window.adminFilterTable=function(){
  const q=(G('adminSearchInput')||{}).value||'';
  const lc=q.toLowerCase();
  const rows=G('adminUserTbody')?.querySelectorAll('tr')||[];
  rows.forEach(r=>{r.style.display=r.textContent.toLowerCase().includes(lc)?'':'none';});
};
function _esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}

window.adminCreateUser=async function(){
  const username=G('adminNewUser').value.trim();
  const display_name=G('adminNewDisplay').value.trim();
  const password=G('adminNewPass').value;
  const role=G('adminNewRole').value;
  if(!username||!password){toast('Username and password are required.','terr');return;}
  try{
    const r=await fetch('/api/v1/admin/users',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username,password,role,display_name})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Create failed');
    toast('User "'+username+'" created.','tok');
    G('adminNewUser').value='';G('adminNewDisplay').value='';G('adminNewPass').value='';G('adminNewRole').value='Viewer';
    adminRefresh();
  }catch(e){toast(e.message,'terr');}
};

window.adminOpenEdit=function(username,display_name,role){
  G('adminEditUsername').value=username;
  G('adminEditDisplay').value=display_name;
  G('adminEditRole').value=role;
  G('adminEditPass').value='';
  G('adminEditModal').style.display='flex';
};
window.adminCloseEditModal=function(){G('adminEditModal').style.display='none';};

window.adminSaveEdit=async function(){
  const username=G('adminEditUsername').value;
  const body={display_name:G('adminEditDisplay').value.trim(),role:G('adminEditRole').value};
  const pw=G('adminEditPass').value;
  if(pw)body.password=pw;
  try{
    const r=await fetch('/api/v1/admin/users/'+encodeURIComponent(username),{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Update failed');
    toast('User "'+username+'" updated.','tok');
    adminCloseEditModal();adminRefresh();
  }catch(e){toast(e.message,'terr');}
};

window.adminDeleteUser=async function(username){
  if(!(await uiConfirm('Delete user "'+username+'"? This cannot be undone.',{danger:true})))return;
  try{
    const r=await fetch('/api/v1/admin/users/'+encodeURIComponent(username),{method:'DELETE'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Delete failed');
    toast('User "'+username+'" deleted.','tok');adminRefresh();
  }catch(e){toast(e.message,'terr');}
};

window.adminResetPw=async function(username){
  const pw=prompt('Enter new password for "'+username+'" (min 6 chars):');
  if(!pw)return;
  try{
    const r=await fetch('/api/v1/admin/users/'+encodeURIComponent(username)+'/reset-password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({new_password:pw})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Reset failed');
    toast('Password reset for "'+username+'".','tok');
  }catch(e){toast(e.message,'terr');}
};

// ═════════════════════════════════════════════════════════════════════════════
// JOB SCHEDULER — Frontend Logic
// ═════════════════════════════════════════════════════════════════════════════

let _schSchedules = [];
let _schHistory = [];
let _schTables = [];
let _schEditId = null;

async function schLoadTables() {
  try {
    const r = await fetch('/api/v1/scheduler/tables');
    const d = await r.json();
    if (!d.success) { console.error('schLoadTables', d.error); return; }
    _schTables = (d.tables || []).map(t => ({...t, job_names: t.jobs ? t.jobs.map(j=>j.job_name) : (t.job_names||[]), schema: t.table_schema||t.schema||''}));
    const sel = G('schTableSelect');
    sel.innerHTML = '<option value="">— Select a table —</option>';
    _schTables.forEach(t => {
      const opt = document.createElement('option');
      opt.value = t.group_id;
      opt.textContent = t.table_name;
      sel.appendChild(opt);
    });
  } catch (e) { console.error('schLoadTables', e); }
}

function schOnTableChange() {
  const gid = G('schTableSelect').value;
  const hint = G('schTableJobs');
  if (!gid) { hint.textContent = ''; return; }
  const t = _schTables.find(x => x.group_id === gid);
  if (t && t.job_names.length) {
    hint.textContent = 'Jobs: ' + t.job_names.join(' → ');
  } else {
    hint.textContent = '';
  }
}

async function schRefresh() {
  try {
    const r = await fetch('/api/v1/scheduler/schedules');
    const d = await r.json();
    if (!d.success) { console.error('schRefresh', d.error); return; }
    _schSchedules = d.schedules || [];
    _schHistory = d.history || [];
    schRenderTable();
    schRenderHistory();
  } catch (e) { console.error('schRefresh', e); }
}

function schLoadJobs() {
  schLoadTables();
  schRefresh();
}

function schRenderTable() {
  const filter = (G('schFilterStatus') || {}).value || '';
  const tbody = G('schTableBody');
  let items = _schSchedules;
  if (filter) items = items.filter(s => s.status === filter);
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="padding:32px;text-align:center;color:var(--t4);">No schedules configured yet. Select a table and create one above.</td></tr>';
    return;
  }
  tbody.innerHTML = items.map(s => {
    const jobs = (s.job_names || []).join(' → ');
    const jobsTrunc = jobs.length > 40 ? jobs.substring(0, 37) + '...' : jobs;
    const sched = _schDescribeSchedule(s);
    const statusBadge = s.status === 'active'
      ? '<span style="background:#DCFCE7;color:#166534;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;">Active</span>'
      : '<span style="background:#FEF3C7;color:#92400E;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;">Paused</span>';
    const nextRun = s.next_run ? new Date(s.next_run).toLocaleString('en-US', {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '—';
    const lastRun = s.last_run ? new Date(s.last_run).toLocaleString('en-US', {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '—';
    return `<tr style="border-bottom:1px solid var(--border);">
      <td style="padding:8px 10px;font-weight:600;font-size:12px;">${s.table_name || '—'}</td>
      <td style="padding:8px 10px;font-size:11px;" title="${jobs}">${jobsTrunc || '—'}</td>
      <td style="padding:8px 10px;text-align:center;font-size:11px;text-transform:capitalize;">${s.type || '—'}</td>
      <td style="padding:8px 10px;font-size:11px;">${sched}</td>
      <td style="padding:8px 10px;text-align:center;">${statusBadge}</td>
      <td style="padding:8px 10px;font-size:11px;">${nextRun}</td>
      <td style="padding:8px 10px;font-size:11px;">${lastRun}</td>
      <td style="padding:8px 10px;text-align:center;white-space:nowrap;">
        <button class="btn btn-ghost btn-xs" onclick="schEdit('${s.schedule_id}')" title="Edit" style="padding:3px 6px;">✏️</button>
        <button class="btn btn-ghost btn-xs" onclick="schRunNow('${s.schedule_id}')" title="Run Now" style="padding:3px 6px;">▶</button>
        <button class="btn btn-ghost btn-xs" onclick="schToggle('${s.schedule_id}')" title="${s.status==='active'?'Pause':'Resume'}" style="padding:3px 6px;">${s.status==='active'?'⏸':'▶'}</button>
        <button class="btn btn-ghost btn-xs" onclick="schDelete('${s.schedule_id}')" title="Delete" style="padding:3px 6px;color:#dc2626;">🗑</button>
      </td>
    </tr>`;
  }).join('');
}

function schRenderHistory() {
  const tbody = G('schHistoryBody');
  if (!_schHistory.length) {
    tbody.innerHTML = '<tr><td colspan="6" style="padding:24px;text-align:center;color:var(--t4);">No execution history yet.</td></tr>';
    return;
  }
  tbody.innerHTML = _schHistory.slice(0, 50).map(h => {
    const ts = h.timestamp ? new Date(h.timestamp).toLocaleString('en-US', {month:'short',day:'numeric',year:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '—';
    const resultBadge = _schResultBadge(h.result);
    const jobsTrunc = (h.jobs || '').length > 40 ? (h.jobs || '').substring(0, 37) + '...' : (h.jobs || '—');
    const details = (h.details || '').length > 60 ? (h.details || '').substring(0, 57) + '...' : (h.details || '—');
    return `<tr style="border-bottom:1px solid var(--border);">
      <td style="padding:6px 10px;font-size:11px;">${ts}</td>
      <td style="padding:6px 10px;font-weight:600;font-size:11px;">${h.table_name || '—'}</td>
      <td style="padding:6px 10px;font-size:11px;" title="${h.jobs||''}">${jobsTrunc}</td>
      <td style="padding:6px 10px;text-align:center;font-size:10px;text-transform:capitalize;">${h.trigger || '—'}</td>
      <td style="padding:6px 10px;text-align:center;">${resultBadge}</td>
      <td style="padding:6px 10px;font-size:10px;color:var(--t3);" title="${h.details||''}">${details}</td>
    </tr>`;
  }).join('');
}

function _schResultBadge(result) {
  if (!result) return '—';
  const r = result.toLowerCase();
  if (r === 'success' || r === 'succeeded' || r === 'completed')
    return '<span style="background:#DCFCE7;color:#166534;padding:2px 6px;border-radius:8px;font-size:10px;font-weight:700;">Success</span>';
  if (r === 'failed' || r === 'error')
    return '<span style="background:#FEE2E2;color:#991B1B;padding:2px 6px;border-radius:8px;font-size:10px;font-weight:700;">Failed</span>';
  if (r === 'running')
    return '<span style="background:#DBEAFE;color:#1E40AF;padding:2px 6px;border-radius:8px;font-size:10px;font-weight:700;">Running</span>';
  return '<span style="background:#F3F4F6;color:#374151;padding:2px 6px;border-radius:8px;font-size:10px;font-weight:700;">'+result+'</span>';
}

function _schDescribeSchedule(s) {
  if (s.type === 'cron') return s.cron || '—';
  if (s.type === 'interval') return 'Every ' + (s.interval_value || 1) + ' ' + (s.interval_unit || 'hours');
  if (s.type === 'once') return 'Once at ' + (s.once_at ? new Date(s.once_at).toLocaleString() : '—');
  return '—';
}

function schToggleTypeFields() {
  const t = G('schType').value;
  G('schCronFields').style.display = t === 'cron' ? '' : 'none';
  G('schIntervalFields').style.display = t === 'interval' ? '' : 'none';
  G('schOnceFields').style.display = t === 'once' ? '' : 'none';
}

async function schCreateSchedule() {
  const gid = G('schTableSelect').value;
  if (!gid) { toast('Please select a table first.', 'terr'); return; }
  const tbl = _schTables.find(x => x.group_id === gid);
  const type = G('schType').value;
  const entry = {
    schedule_id: _schEditId || '',
    table_name: tbl ? tbl.table_name : '',
    table_schema: tbl ? tbl.schema : '',
    group_id: gid,
    job_names: tbl ? tbl.job_names : [],
    type: type,
    cron: type === 'cron' ? G('schCronExpr').value.trim() : null,
    interval_value: type === 'interval' ? parseInt(G('schIntervalValue').value) || 1 : null,
    interval_unit: type === 'interval' ? G('schIntervalUnit').value : null,
    once_at: type === 'once' ? G('schOnceAt').value : null,
    schedule_desc: _schBuildDesc(type),
    status: 'active',
  };
  entry.next_run = _schComputeNextRun(entry);
  try {
    const r = await fetch('/api/v1/scheduler/schedules', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(entry)
    });
    const d = await r.json();
    if (!d.success) { toast(d.error || 'Failed to save schedule', 'terr'); return; }
    toast(_schEditId ? 'Schedule updated!' : 'Schedule created!', 'tok');
    _schEditId = null;
    G('schCancelEditBtn').style.display = 'none';
    G('schSubmitLabel').textContent = 'Create Schedule';
    schRefresh();
  } catch (e) { toast(e.message, 'terr'); }
}

function _schBuildDesc(type) {
  if (type === 'cron') return G('schCronExpr').value.trim();
  if (type === 'interval') return 'Every ' + (G('schIntervalValue').value || 1) + ' ' + G('schIntervalUnit').value;
  if (type === 'once') return 'Once at ' + G('schOnceAt').value;
  return '';
}

function _schComputeNextRun(entry) {
  const now = new Date();
  if (entry.type === 'interval') {
    const ms = _schIntervalMs(entry.interval_value, entry.interval_unit);
    return new Date(now.getTime() + ms).toISOString();
  }
  if (entry.type === 'once') return entry.once_at ? new Date(entry.once_at).toISOString() : null;
  return new Date(now.getTime() + 3600000).toISOString();
}

function _schIntervalMs(val, unit) {
  const v = parseInt(val) || 1;
  if (unit === 'minutes') return v * 60000;
  if (unit === 'hours') return v * 3600000;
  if (unit === 'days') return v * 86400000;
  return v * 3600000;
}

function schEdit(scheduleId) {
  const s = _schSchedules.find(x => x.schedule_id === scheduleId);
  if (!s) return;
  _schEditId = scheduleId;
  G('schTableSelect').value = s.group_id || '';
  schOnTableChange();
  G('schType').value = s.type || 'cron';
  schToggleTypeFields();
  if (s.type === 'cron') G('schCronExpr').value = s.cron || '';
  if (s.type === 'interval') {
    G('schIntervalValue').value = s.interval_value || 1;
    G('schIntervalUnit').value = s.interval_unit || 'hours';
  }
  if (s.type === 'once') G('schOnceAt').value = s.once_at || '';
  G('schSubmitLabel').textContent = 'Update Schedule';
  G('schCancelEditBtn').style.display = '';
}

function schCancelEdit() {
  _schEditId = null;
  G('schSubmitLabel').textContent = 'Create Schedule';
  G('schCancelEditBtn').style.display = 'none';
  G('schTableSelect').value = '';
  schOnTableChange();
}

async function schToggle(scheduleId) {
  const s = _schSchedules.find(x => x.schedule_id === scheduleId);
  if (!s) return;
  const newStatus = s.status === 'active' ? 'paused' : 'active';
  try {
    const r = await fetch('/api/v1/scheduler/schedules/' + scheduleId, {
      method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({status: newStatus})
    });
    const d = await r.json();
    if (!d.success) { toast(d.error || 'Toggle failed', 'terr'); return; }
    toast('Schedule ' + (newStatus === 'active' ? 'resumed' : 'paused'), 'tok');
    schRefresh();
  } catch (e) { toast(e.message, 'terr'); }
}

async function schDelete(scheduleId) {
  if (!(await uiConfirm('Delete this schedule? This cannot be undone.',{danger:true}))) return;
  try {
    const r = await fetch('/api/v1/scheduler/schedules/' + scheduleId, {method: 'DELETE'});
    const d = await r.json();
    if (!d.success) { toast(d.error || 'Delete failed', 'terr'); return; }
    toast('Schedule deleted', 'tok');
    schRefresh();
  } catch (e) { toast(e.message, 'terr'); }
}

async function schRunNow(scheduleId) {
  if (!(await uiConfirm('Run this schedule now?',{okLabel:'Run Now'}))) return;
  try {
    const r = await fetch('/api/v1/scheduler/run-now/' + scheduleId, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({})
    });
    const d = await r.json();
    if (!d.success) { toast(d.error || 'Run failed', 'terr'); return; }
    toast('Schedule triggered!', 'tok');
    schRefresh();
  } catch (e) { toast(e.message, 'terr'); }
}

(function() {
  const sel = document.getElementById('schTableSelect');
  if (sel) sel.addEventListener('change', schOnTableChange);
})();

