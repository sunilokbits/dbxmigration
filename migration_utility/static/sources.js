/* Source catalogue helpers, driven by window.SOURCE_REGISTRY
   (served by /api/v1/sources/registry.js, loaded before main.js).
   Mirrors migration_utility/source_registry.py: a source_type says WHAT the
   system is; its connector says HOW the app talks to it. Built-in types are
   their own connector, so existing screens behave exactly as before. */
(function(){
  const REG = window.SOURCE_REGISTRY || {sources:[], methods:{}, categories:{}, jdbc_engines:{}, file_formats:['parquet']};
  const BY_KEY = {};
  REG.sources.forEach(s => { BY_KEY[s.key] = s; });
  const $ = id => document.getElementById(id);
  const esc = v => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

  function srcInfo(k){ return BY_KEY[k] || null; }
  function srcMethod(k, chosen){
    const s = BY_KEY[k];
    if(!s || !s.methods || !s.methods.length) return '';
    return s.methods.includes(chosen) ? chosen : s.methods[0];
  }
  function srcConnector(k, method, engine){
    const s = BY_KEY[k];
    if(!s || s.builtin) return k || 'sqlserver';
    const m = srcMethod(k, method);
    if(m === 'jdbc') return (engine && REG.jdbc_engines[engine]) ? engine : (s.engine || 'sqlserver');
    return (REG.methods[m] || {}).connector || 'api';
  }
  /* Connector for a source type using the current Settings selections. */
  function srcUi(k){
    return srcConnector(k, ($('cfgSrcMethod')||{}).value, ($('cfgSrcEngine')||{}).value);
  }
  /* Extra source_config keys an enterprise source needs (none for built-ins). */
  function srcRegistryExtras(k){
    const s = BY_KEY[k];
    if(!s || s.builtin) return {};
    const ex = {ingestion_method: srcMethod(k, ($('cfgSrcMethod')||{}).value)};
    const ui = srcUi(k);
    if(ex.ingestion_method === 'jdbc') ex.engine = ($('cfgSrcEngine')||{}).value || s.engine || 'sqlserver';
    if(ui === 'adls') ex.file_format = ($('cfgSrcFileFormat')||{}).value || 'parquet';
    if(ui === 'lakeflow') ex.objects = (($('cfgSrcObjects')||{}).value || '').trim();
    if(ui === 'api'){
      ex.api_auth_type = ($('cfgSrcApiAuthType')||{}).value || 'none';
      ex.api_key_header = (($('cfgSrcApiKeyHeader')||{}).value || '').trim();
    }
    return ex;
  }

  const READY = {
    ready:     {label:'Ready',              bg:'rgba(16,185,129,.12)', fg:'#047857'},
    configure: {label:'Configure & test',   bg:'rgba(59,130,246,.12)', fg:'#1D4ED8'},
    planned:   {label:'Planned',            bg:'rgba(148,163,184,.18)', fg:'#475569'},
  };
  function readinessOf(s, method){
    if(!s) return 'planned';
    if(typeof s.readiness === 'string') return s.readiness;
    return s.readiness[srcMethod(s.key, method)] || 'planned';
  }
  function badge(state){
    const r = READY[state] || READY.planned;
    return '<span style="display:inline-block;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:600;background:'+r.bg+';color:'+r.fg+';">'+r.label+'</span>';
  }

  /* Rebuild the Settings source picker grouped by category. */
  function cfgRenderSourceOptions(){
    const sel = $('cfgSrcType');
    if(!sel || !REG.sources.length) return;
    const cur = sel.value;
    const groups = {};
    REG.sources.forEach(s => { (groups[s.category] = groups[s.category] || []).push(s); });
    sel.innerHTML = Object.keys(REG.categories).filter(c => groups[c]).map(c =>
      '<optgroup label="'+esc(REG.categories[c])+'">' +
      groups[c].map(s => '<option value="'+esc(s.key)+'">'+esc(s.icon+' '+s.label)+'</option>').join('') +
      '</optgroup>').join('');
    sel.value = BY_KEY[cur] ? cur : (sel.options[0] || {}).value;
  }

  /* Ingestion-method / engine / format selects for the chosen source. */
  function cfgSyncMethodOptions(k){
    const s = BY_KEY[k];
    const mSel = $('cfgSrcMethod');
    if(mSel){
      // Keep the chosen method only while the same source stays selected.
      const prev = (mSel.dataset.src === k) ? mSel.value : '';
      mSel.dataset.src = k || '';
      const methods = (s && !s.builtin) ? s.methods : [];
      mSel.innerHTML = methods.map(m => '<option value="'+esc(m)+'">'+esc((REG.methods[m]||{}).label||m)+'</option>').join('');
      mSel.value = methods.includes(prev) ? prev : (methods[0] || '');
      mSel.disabled = methods.length < 2;
    }
    const eSel = $('cfgSrcEngine');
    if(eSel && !eSel.options.length){
      eSel.innerHTML = Object.entries(REG.jdbc_engines).map(([k2, l]) => '<option value="'+esc(k2)+'">'+esc(l)+'</option>').join('');
      if(s && s.engine) eSel.value = s.engine;
    }
    const fSel = $('cfgSrcFileFormat');
    if(fSel && !fSel.options.length){
      fSel.innerHTML = (REG.file_formats || ['parquet']).map(f => '<option value="'+esc(f)+'">'+esc(f.toUpperCase())+'</option>').join('');
    }
  }

  /* Restore enterprise-only fields from saved config (called by _populateConfig). */
  function cfgSyncSourceExtras(src){
    src = src || {};
    cfgSyncMethodOptions(src.source_type);
    if($('cfgSrcMethod') && src.ingestion_method) $('cfgSrcMethod').value = src.ingestion_method;
    if($('cfgSrcEngine') && src.engine) $('cfgSrcEngine').value = src.engine;
    if($('cfgSrcFileFormat') && src.file_format) $('cfgSrcFileFormat').value = src.file_format;
    if($('cfgSrcObjects')) $('cfgSrcObjects').value = src.objects || '';
  }

  /* RFP facts + readiness + secret strip under the source form. */
  function cfgRenderSourceInfo(k){
    const box = $('cfgSrcInfo');
    if(!box) return;
    const s = BY_KEY[k];
    if(!s || s.builtin && !s.rfp){ box.style.display = 'none'; box.innerHTML = ''; return; }
    const m = srcMethod(k, ($('cfgSrcMethod')||{}).value);
    const r = s.rfp;
    const facts = r ? [
      'RFP #'+r.no, r.objects+' objects', r.volume, 'Change '+r.change, 'CDC '+(r.cdc?'Yes':'No'),
      'Every '+r.frequency, 'Complexity '+r.complexity
    ].map(esc).join(' &nbsp;·&nbsp; ') : '';
    const how = s.builtin ? 'Built-in connector' : esc((REG.methods[m]||{}).label || m);
    const note = s.builtin ? '' : ' &nbsp;<span style="color:var(--t4);">Not yet verified against a live '+esc(s.label)+' system.</span>';
    box.innerHTML =
      '<div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;">'+badge(readinessOf(s, m))+
      '<b style="font-size:11px;">'+esc(s.icon+' '+s.label)+'</b><span style="font-size:11px;color:var(--t3);">'+how+'</span>'+
      '<span style="font-size:10px;color:var(--t4);margin-left:auto;">Secret: <code>'+esc(s.secret_key)+'</code></span></div>'+
      (facts ? '<div style="font-size:10.5px;color:var(--t3);margin-top:4px;">'+facts+'</div>' : '')+
      (r && r.ingestion ? '<div style="font-size:10.5px;color:var(--t4);margin-top:2px;">RFP ingestion: '+esc(r.ingestion)+note+'</div>' : '');
    box.style.display = '';
  }

  /* The 17 RFP systems as one neat table; clicking a row selects it. */
  function cfgRenderSourceCatalog(){
    const box = $('cfgSrcCatalog');
    if(!box) return;
    const rows = REG.sources.filter(s => s.rfp).sort((a, b) => a.rfp.no - b.rfp.no);
    const th = 'style="text-align:left;padding:6px 8px;font-size:10.5px;color:var(--t3);border-bottom:1px solid var(--b2);white-space:nowrap;"';
    const td = 'style="padding:6px 8px;font-size:11px;border-bottom:1px solid var(--b2);vertical-align:top;"';
    box.innerHTML =
      '<table style="width:100%;border-collapse:collapse;">'+
      '<thead><tr><th '+th+'>#</th><th '+th+'>System</th><th '+th+'>Objects</th><th '+th+'>Volume</th><th '+th+'>Change</th>'+
      '<th '+th+'>CDC</th><th '+th+'>Freq.</th><th '+th+'>Ingestion method</th><th '+th+'>Complexity</th><th '+th+'>Status</th></tr></thead><tbody>'+
      rows.map(s => {
        const r = s.rfp;
        return '<tr data-src="'+esc(s.key)+'" style="cursor:pointer;" title="Configure '+esc(s.label)+'">'+
          '<td '+td+'>'+r.no+'</td><td '+td+'><b>'+esc(s.icon+' '+s.label)+'</b></td><td '+td+'>'+esc(r.objects)+'</td>'+
          '<td '+td+'>'+esc(r.volume)+'</td><td '+td+'>'+esc(r.change)+'</td><td '+td+'>'+(r.cdc?'Yes':'No')+'</td>'+
          '<td '+td+'>'+esc(r.frequency)+'</td><td '+td+'>'+esc(r.ingestion)+'</td><td '+td+'>'+esc(r.complexity)+'</td>'+
          '<td '+td+'>'+badge(readinessOf(s, ''))+'</td></tr>';
      }).join('')+
      '</tbody></table>';
    box.querySelectorAll('tr[data-src]').forEach(tr => tr.addEventListener('click', () => {
      const sel = $('cfgSrcType');
      if(!sel) return;
      sel.value = tr.getAttribute('data-src');
      if(typeof window.cfgOnSrcTypeChange === 'function') window.cfgOnSrcTypeChange(sel);
      sel.scrollIntoView({behavior:'smooth', block:'center'});
    }));
    const cnt = $('cfgSrcCatalogCount');
    if(cnt) cnt.textContent = rows.length;
  }

  Object.assign(window, {srcInfo, srcMethod, srcConnector, srcUi, srcRegistryExtras, cfgRenderSourceOptions,
                         cfgSyncMethodOptions, cfgSyncSourceExtras, cfgRenderSourceInfo, cfgRenderSourceCatalog});

  // Build the picker before main.js applies the saved config, so a saved
  // enterprise source (e.g. "sap_ecc") has an option to select.
  function init(){
    cfgRenderSourceOptions();
    cfgSyncMethodOptions(($('cfgSrcType')||{}).value);
    cfgRenderSourceCatalog();
  }
  if($('cfgSrcType')) init();
  else document.addEventListener('DOMContentLoaded', init);
})();
