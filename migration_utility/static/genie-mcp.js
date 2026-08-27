/* ═══════════════ GENIE MCP (Genie One) INTEGRATION ═══════════════
   Adds MCP Genie One endpoint support to the existing Genie panel.
   Loaded AFTER supplementary.js — extends the existing Genie engine.
   ═══════════════════════════════════════════════════════════════════ */
(function(){
'use strict';

var _mcpEndpoints = [];
var _mcpSessionId = null;
var _currentMcp = null; // currently selected MCP endpoint

function $g(id){ return document.getElementById(id); }

/* ── Load MCP endpoints from backend ──────────────────────────────── */
function _loadMcpEndpoints(cb){
  fetch('/api/v1/genie/mcp/endpoints').then(function(r){return r.json();}).then(function(d){
    _mcpEndpoints = d.endpoints || [];
    if(cb) cb();
  }).catch(function(e){ console.warn('[MCP] load error:', e); if(cb) cb(); });
}

/* ── Inject MCP endpoints into the Genie Space dropdown (DISABLED) ── */
function _injectMcpOptions(){
  // MCP Genie One integration removed
  var sel = $g('genieSpaceSelect');
  if(!sel) return;
  var existing = sel.querySelector('optgroup[data-mcp]');
  if(existing) existing.remove();
}

/* ── Override genieSelectSpace / genieSpaceChanged to detect MCP ───── */
var _origSpaceChanged = window.genieSpaceChanged || window.genieSelectSpace;
window.genieSelectSpace = window.genieSpaceChanged = function(val){
  var sel = $g('genieSpaceSelect');
  var spId = val || (sel ? sel.value : '');
  if(spId && spId.indexOf('mcp::') === 0){
    // MCP endpoint selected
    var mcpId = spId.replace('mcp::', '');
    _currentMcp = _mcpEndpoints.find(function(ep){ return ep.id === mcpId; }) || {id: mcpId, name:'Genie One', endpoint_url:''};
    _mcpSessionId = null;
    // Update status
    var statusEl = $g('genieStatusLine') || $g('genieChatStatus');
    if(statusEl) statusEl.innerHTML = '<span class="genie-dot" style="background:#10B981;"></span>\u26A1 Genie One (MCP) connected';
    // Enable input
    var inp = $g('genieInput'), btn = $g('genieSendBtn');
    if(inp) inp.disabled = false;
    if(btn) btn.disabled = false;
    // Show welcome
    if(window.genieNewConversation) window.genieNewConversation();
    return;
  }
  // Not MCP — delegate to original handler
  _currentMcp = null;
  if(_origSpaceChanged) _origSpaceChanged(val);
};

/* ── Override genieSendMessage to route MCP queries ────────────────── */
var _origSendMessage = window.genieSendMessage;
window.genieSendMessage = function(){
  if(!_currentMcp){
    // Not MCP — use original Genie Space flow
    if(_origSendMessage) return _origSendMessage();
    return;
  }
  // MCP flow
  var inp = $g('genieInput');
  if(!inp) return;
  var text = (inp.value || '').trim();
  if(!text) return;
  inp.value = '';
  if(window.genieAutoResize) window.genieAutoResize(inp);
  inp.disabled = true;
  var sendBtn = $g('genieSendBtn');
  if(sendBtn) sendBtn.disabled = true;

  // Remove welcome
  var welcome = $g('genieWelcome') || $g('genieWelcomeScreen');
  if(welcome) welcome.remove();

  // Append user message
  _appendUserMsg(text);

  // Create bot placeholder
  var botId = 'mcpBot' + Date.now();
  _appendBotPlaceholder(botId);

  // Send to MCP backend
  var body = {
    content: text,
    endpoint_url: _currentMcp.endpoint_url || '',
    session_id: _mcpSessionId || ''
  };

  fetch('/api/v1/genie/mcp/query', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  }).then(function(r){ return r.json(); }).then(function(d){
    if(d.error){
      _renderBotError(botId, d.error);
    } else {
      // Cache session for reuse
      if(d.session_id) _mcpSessionId = d.session_id;
      // Render result
      var txt = (d.result && d.result.text) || '';
      var sql = (d.result && d.result.sql) || '';
      var data = (d.result && d.result.data) || null;
      _renderMcpBotComplete(botId, txt, sql, data);
    }
    _enableInput();
  }).catch(function(e){
    _renderBotError(botId, 'MCP request failed: ' + e.message);
    _enableInput();
  });
};

/* ── Add MCP endpoint (via + Add button extension) ────────────────── */
window.genieSaveMcpEndpoint = function(){
  var urlInput = $g('genieMcpUrl');
  var nameInput = $g('genieMcpName');
  var msgEl = $g('genieAddSpaceMsg');
  var url = urlInput ? urlInput.value.trim() : '';
  var name = nameInput ? nameInput.value.trim() : '';
  if(!url){
    if(msgEl){ msgEl.style.color='#EF4444'; msgEl.textContent='MCP endpoint URL is required'; }
    return;
  }
  if(msgEl){ msgEl.style.color='#3B82F6'; msgEl.textContent='Adding MCP endpoint...'; }
  fetch('/api/v1/genie/mcp/endpoints/save', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({endpoint_url: url, name: name || 'Genie One (MCP)'})
  }).then(function(r){return r.json();}).then(function(d){
    if(d.error){
      if(msgEl){ msgEl.style.color='#EF4444'; msgEl.textContent=d.error; }
      return;
    }
    if(msgEl){ msgEl.style.color='#10B981'; msgEl.textContent='\u2713 MCP endpoint added!'; }
    _loadMcpEndpoints(function(){
      _injectMcpOptions();
      var sel = $g('genieSpaceSelect');
      if(sel && d.endpoint) sel.value = 'mcp::' + d.endpoint.id;
      setTimeout(function(){
        var panel = $g('genieAddSpacePanel');
        if(panel) panel.style.display = 'none';
        window.genieSpaceChanged ? window.genieSpaceChanged() : window.genieSelectSpace();
      }, 800);
    });
  }).catch(function(e){
    if(msgEl){ msgEl.style.color='#EF4444'; msgEl.textContent='Error: '+e.message; }
  });
};

/* ── Toggle Add panel between Space ID and MCP mode ───────────────── */
window.genieToggleAddMode = function(mode){
  var spaceFields = $g('genieAddSpaceFields');
  var mcpFields = $g('genieAddMcpFields');
  var tabSpace = $g('genieAddTabSpace');
  var tabMcp = $g('genieAddTabMcp');
  if(mode === 'mcp'){
    if(spaceFields) spaceFields.style.display = 'none';
    if(mcpFields) mcpFields.style.display = 'block';
    if(tabSpace) tabSpace.classList.remove('active');
    if(tabMcp) tabMcp.classList.add('active');
  } else {
    if(spaceFields) spaceFields.style.display = 'block';
    if(mcpFields) mcpFields.style.display = 'none';
    if(tabSpace) tabSpace.classList.add('active');
    if(tabMcp) tabMcp.classList.remove('active');
  }
};

/* ── Helper: append user message ──────────────────────────────────── */
function _appendUserMsg(t){
  var initials = 'AD';
  if(window.__USER_NAME){
    initials = window.__USER_NAME.split(/[\s.]+/).map(function(w){return w[0];}).join('').toUpperCase().slice(0,2) || 'AD';
  }
  var list = $g('genieMsgList');
  if(!list) return;
  var d = document.createElement('div');
  d.innerHTML = '<div class="genie-msg user"><div class="genie-avatar">' + initials + '</div><div class="genie-bubble">' + _esc(t) + '</div></div>';
  while(d.firstChild) list.appendChild(d.firstChild);
  _scrollBottom();
}

/* ── Helper: bot placeholder ──────────────────────────────────────── */
function _appendBotPlaceholder(id){
  var list = $g('genieMsgList');
  if(!list) return;
  var dot = '<span style="width:5px;height:5px;border-radius:50%;background:#8B5CF6;display:inline-block;animation:genieThink .8s ease-in-out infinite ';
  var d = document.createElement('div');
  d.innerHTML = '<div class="genie-msg bot" id="' + id + '">' +
    '<div class="genie-avatar" style="background:linear-gradient(135deg,#6366F1,#8B5CF6);"><svg viewBox="0 0 24 24" fill="white"><path d="M13 2L4 14h7l-1 8 9-12h-7z"/></svg></div>' +
    '<div class="genie-msg-content"><div class="genie-msg-text" id="' + id + '_t" style="display:flex;align-items:center;gap:8px;color:#9CA3AF;font-size:12.5px;">' +
    '<span style="display:flex;gap:4px;align-items:center;">' +
    dot + '0s;"></span>' + dot + '.15s;"></span>' + dot + '.3s;"></span>' +
    '</span>Querying Genie One&hellip;</div></div></div>';
  while(d.firstChild) list.appendChild(d.firstChild);
  _scrollBottom();
}

/* ── Helper: render MCP bot complete ──────────────────────────────── */
function _renderMcpBotComplete(id, text, sql, resultData){
  var el = $g(id); if(!el) return;
  var c = el.querySelector('.genie-msg-content'); if(!c) return;
  var ts = new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
  var html = '';
  if(text) html += '<div class="genie-msg-text">' + _fmtText(text) + '</div>';
  if(sql){
    var sid = id + '_sql';
    html += '<div class="genie-sql-block">' +
      '<div class="genie-sql-hd"><div class="genie-sql-hd-left"><svg viewBox="0 0 24 24" style="width:13px;height:13px;stroke:#A5B4FC;fill:none;stroke-width:2;"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>Generated SQL</div>' +
      '<button class="genie-sql-copy" onclick="genieCopySql(\'' + sid + '\')">Copy</button></div>' +
      '<div class="genie-sql-body"><pre id="' + sid + '">' + _hlSql(sql) + '</pre></div></div>';
  }
  if(resultData && resultData.columns && resultData.rows){
    html += _renderMcpTable(resultData);
  }
  if(!text && !sql && !resultData) html = '<div class="genie-msg-text" style="color:var(--t3);">Genie One processed your request.</div>';
  html += '<div class="genie-msg-time" style="font-size:10px;color:#94A3AF;margin-top:6px;">' + ts + ' \u26A1 MCP</div>';
  c.innerHTML = html;
  _scrollBottom();
}

function _renderMcpTable(data){
  var cols = data.columns || [];
  var rows = data.rows || [];
  if(!cols.length) return '';
  var html = '<div class="genie-results-block">' +
    '<div class="genie-results-hd"><span style="font-weight:700;">' + rows.length + ' row' + (rows.length!==1?'s':'') + '</span></div>' +
    '<div class="genie-results-wrap"><table class="genie-table"><thead><tr>';
  cols.forEach(function(c){ html += '<th>' + _esc(String(c)) + '</th>'; });
  html += '</tr></thead><tbody>';
  rows.forEach(function(row){
    html += '<tr>';
    if(Array.isArray(row)) row.forEach(function(v){ html += '<td>' + _esc(v==null?'':String(v)) + '</td>'; });
    else cols.forEach(function(c){ html += '<td>' + _esc(row[c]==null?'':String(row[c])) + '</td>'; });
    html += '</tr>';
  });
  html += '</tbody></table></div></div>';
  return html;
}

/* ── Helper: render error ─────────────────────────────────────────── */
function _renderBotError(id, msg){
  var el = $g(id); if(!el) return;
  var c = el.querySelector('.genie-msg-content'); if(!c) return;
  c.innerHTML = '<div class="genie-error-bubble" style="background:#FEF2F2;border:1px solid #FECACA;border-radius:8px;padding:10px 14px;color:#991B1B;font-size:12px;">' +
    '<div style="display:flex;align-items:center;gap:6px;font-weight:700;margin-bottom:4px;">\u26A0\uFE0F MCP Error</div>' + _esc(msg) + '</div>';
  _scrollBottom();
}

function _enableInput(){
  var inp = $g('genieInput'), btn = $g('genieSendBtn');
  if(inp) inp.disabled = false;
  if(btn) btn.disabled = false;
  if(inp) inp.focus();
}
function _scrollBottom(){ var l=$g('genieMsgList'); if(l) requestAnimationFrame(function(){l.scrollTop=l.scrollHeight;}); }
function _esc(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/\n/g,'<br>'); }
function _fmtText(t){ return _esc(t).replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>').replace(/`(.+?)`/g,'<code style="font-family:monospace;background:rgba(0,0,0,.06);padding:1px 5px;border-radius:4px;">$1</code>'); }
function _hlSql(s){ return _esc(s).replace(/\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|ON|GROUP BY|ORDER BY|HAVING|LIMIT|UNION|ALL|DISTINCT|AS|INSERT|INTO|UPDATE|SET|DELETE|CREATE|TABLE|VIEW|WITH|CASE|WHEN|THEN|ELSE|END|AND|OR|NOT|IN|IS|NULL|LIKE|BETWEEN|EXISTS|COUNT|SUM|AVG|MIN|MAX)\b/gi,'<span class="genie-sql-kw">$1</span>'); }

/* ── Initialize: load MCP endpoints and inject into dropdown ──────── */
function _init(){
  // Wait for the Genie panel to exist
  var attempts = 0;
  var timer = setInterval(function(){
    attempts++;
    var sel = $g('genieSpaceSelect');
    if(sel || attempts > 30){
      clearInterval(timer);
      _loadMcpEndpoints(function(){
        _injectMcpOptions();
        _injectAddPanelMcpTab();
      });
    }
  }, 300);
}

/* ── Inject MCP tab into the Add Space panel ──────────────────────── */
function _injectAddPanelMcpTab(){
  var panel = $g('genieAddSpacePanel');
  if(!panel || panel.querySelector('[data-mcp-injected]')) return;

  // Wrap existing content
  var existingContent = panel.innerHTML;
  panel.innerHTML = '<div data-mcp-injected="1">' +
    '<div class="genie-add-tabs" style="display:flex;gap:0;margin-bottom:12px;border-bottom:1px solid #E2E8F0;">' +
      '<button id="genieAddTabSpace" class="genie-add-tab active" onclick="genieToggleAddMode(\'space\')" style="flex:1;padding:8px 12px;font-size:11px;font-weight:600;border:none;background:none;cursor:pointer;border-bottom:2px solid #6366F1;color:#6366F1;">Genie Space</button>' +
      '<button id="genieAddTabMcp" class="genie-add-tab" onclick="genieToggleAddMode(\'mcp\')" style="flex:1;padding:8px 12px;font-size:11px;font-weight:600;border:none;background:none;cursor:pointer;border-bottom:2px solid transparent;color:#64748B;">\u26A1 MCP Genie One</button>' +
    '</div>' +
    '<div id="genieAddSpaceFields">' + existingContent + '</div>' +
    '<div id="genieAddMcpFields" style="display:none;padding:8px 0;">' +
      '<div style="margin-bottom:8px;"><label style="font-size:11px;font-weight:600;color:#374151;display:block;margin-bottom:4px;">MCP Endpoint URL</label>' +
      '<input type="text" id="genieMcpUrl" placeholder="https://adb-xxx.azuredatabricks.net/api/2.0/mcp/genie" style="width:100%;padding:8px 10px;border:1px solid #D1D5DB;border-radius:6px;font-size:12px;box-sizing:border-box;" /></div>' +
      '<div style="margin-bottom:10px;"><label style="font-size:11px;font-weight:600;color:#374151;display:block;margin-bottom:4px;">Display Name (optional)</label>' +
      '<input type="text" id="genieMcpName" placeholder="Genie One" style="width:100%;padding:8px 10px;border:1px solid #D1D5DB;border-radius:6px;font-size:12px;box-sizing:border-box;" /></div>' +
      '<button onclick="genieSaveMcpEndpoint()" style="width:100%;padding:9px;background:linear-gradient(135deg,#6366F1,#8B5CF6);color:#fff;border:none;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;">\u26A1 Add MCP Endpoint</button>' +
      '<p style="margin:8px 0 0;font-size:10px;color:#6B7280;line-height:1.4;">Connects to Databricks Genie One via Model Context Protocol. Queries across all workspace data with Unity Catalog permissions enforced.</p>' +
    '</div>' +
  '</div>';
}

/* ═══════════════ CATALOG DISCOVERY & SQL EXECUTION ═══════════════ */

var _catalogCache = null;
var _sqlPanelVisible = false;

/* ── Fetch discovered catalogs ─────────────────────────────────── */
window.genieFetchCatalogs = function(cb){
  fetch('/api/v1/catalog/list').then(function(r){return r.json();}).then(function(d){
    _catalogCache = d.catalogs || [];
    if(cb) cb(_catalogCache);
  }).catch(function(e){ console.warn('[CatalogDiscovery] fetch error:', e); if(cb) cb([]); });
};

/* ── Trigger fresh catalog discovery ───────────────────────────── */
window.genieRefreshCatalogs = function(){
  var btn = $g('genieCatalogRefreshBtn');
  if(btn){ btn.disabled = true; btn.textContent = '\u21BB Scanning...'; }
  fetch('/api/v1/catalog/discover', {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'}).then(function(r){return r.json();}).then(function(d){
    // Poll status until done
    var pollCount = 0;
    var poller = setInterval(function(){
      pollCount++;
      if(pollCount > 60){ clearInterval(poller); if(btn){btn.disabled=false; btn.textContent='\u21BB Refresh';} return; }
      fetch('/api/v1/catalog/status').then(function(r){return r.json();}).then(function(s){
        if(!s.refresh_in_progress){
          clearInterval(poller);
          if(btn){btn.disabled=false; btn.textContent='\u21BB Refresh';}
          window.genieFetchCatalogs(function(cats){ _renderCatalogPanel(cats); });
          // Update status line
          var st = $g('genieCatalogStatus');
          if(st) st.textContent = 'Last refreshed: ' + (s.last_refreshed||'never') + ' | ' + (s.stats?s.stats.total_catalogs+' catalogs, '+s.stats.total_tables+' tables':'');
        }
      });
    }, 3000);
  }).catch(function(e){ if(btn){btn.disabled=false; btn.textContent='\u21BB Refresh';} });
};

/* ── Run SQL directly from Genie panel ─────────────────────────── */
window.genieRunSql = function(sql){
  if(!sql){
    var inp = $g('genieSqlInput');
    sql = inp ? inp.value.trim() : '';
  }
  if(!sql) return;

  // Show in chat
  _appendUserMsg('\ud83d\udcca SQL: ' + sql);
  var botId = 'sqlBot' + Date.now();
  _appendBotPlaceholder(botId);
  var plEl = $g(botId+'_t');
  if(plEl) plEl.innerHTML = plEl.innerHTML.replace('Querying Genie One', 'Executing SQL');

  fetch('/api/v1/sql/execute', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({sql: sql, max_rows: 200})
  }).then(function(r){return r.json();}).then(function(d){
    if(d.error){
      _renderBotError(botId, d.error);
    } else {
      var resultData = {columns: d.columns, rows: d.data};
      var txt = d.row_count + ' row' + (d.row_count!==1?'s':'') + ' returned' + (d.truncated?' (truncated)':'');
      _renderMcpBotComplete(botId, txt, sql, resultData);
    }
    _enableInput();
  }).catch(function(e){
    _renderBotError(botId, 'SQL execution failed: ' + e.message);
    _enableInput();
  });
};

/* ── Toggle SQL Panel ──────────────────────────────────────────── */
window.genieToggleSqlPanel = function(){
  var panel = $g('genieSqlPanel');
  if(!panel){
    _injectSqlPanel();
    panel = $g('genieSqlPanel');
  }
  _sqlPanelVisible = !_sqlPanelVisible;
  if(panel) panel.style.display = _sqlPanelVisible ? 'block' : 'none';
};

/* ── Toggle Catalog Browser ────────────────────────────────────── */
window.genieToggleCatalogBrowser = function(){
  var panel = $g('genieCatalogBrowser');
  if(!panel){
    _injectCatalogBrowser();
    panel = $g('genieCatalogBrowser');
  }
  var visible = panel.style.display !== 'none';
  panel.style.display = visible ? 'none' : 'block';
  if(!visible && !_catalogCache){
    window.genieFetchCatalogs(function(cats){ _renderCatalogPanel(cats); });
  }
};

/* ── Inject SQL Editor Panel ───────────────────────────────────── */
function _injectSqlPanel(){
  var container = $g('genieMsgList');
  if(!container) container = document.querySelector('.genie-body');
  if(!container) return;
  var parent = container.parentElement;
  var existing = $g('genieSqlPanel');
  if(existing) return;

  var d = document.createElement('div');
  d.id = 'genieSqlPanel';
  d.style.cssText = 'display:none;padding:8px 12px;border-top:1px solid #E2E8F0;background:#F8FAFC;';
  d.innerHTML =
    '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;">' +
      '<span style="font-size:11px;font-weight:700;color:#374151;">\ud83d\udcca Run SQL (Multi-Catalog)</span>' +
      '<span id="genieCatalogStatus" style="font-size:9px;color:#6B7280;margin-left:auto;"></span>' +
    '</div>' +
    '<textarea id="genieSqlInput" rows="3" placeholder="SELECT * FROM catalog.schema.table LIMIT 10" style="width:100%;padding:8px;border:1px solid #D1D5DB;border-radius:6px;font-family:monospace;font-size:11px;resize:vertical;box-sizing:border-box;background:#fff;"></textarea>' +
    '<div style="display:flex;gap:6px;margin-top:6px;">' +
      '<button onclick="genieRunSql()" style="flex:1;padding:7px;background:#10B981;color:#fff;border:none;border-radius:6px;font-size:11px;font-weight:600;cursor:pointer;">\u25B6 Run SQL</button>' +
      '<button onclick="genieToggleCatalogBrowser()" style="padding:7px 12px;background:#6366F1;color:#fff;border:none;border-radius:6px;font-size:11px;font-weight:600;cursor:pointer;">\ud83d\uddc2 Catalogs</button>' +
      '<button id="genieCatalogRefreshBtn" onclick="genieRefreshCatalogs()" style="padding:7px 12px;background:#F59E0B;color:#fff;border:none;border-radius:6px;font-size:11px;font-weight:600;cursor:pointer;">\u21BB Refresh</button>' +
    '</div>' +
    '<div id="genieCatalogBrowser" style="display:none;margin-top:8px;max-height:200px;overflow-y:auto;border:1px solid #E2E8F0;border-radius:6px;padding:8px;background:#fff;"></div>';
  parent.insertBefore(d, container.nextSibling);

  // Fetch discovery status
  fetch('/api/v1/catalog/status').then(function(r){return r.json();}).then(function(s){
    var st = $g('genieCatalogStatus');
    if(st) st.textContent = (s.last_refreshed ? 'Refreshed: '+s.last_refreshed.split('T')[0] : 'Not scanned yet') + (s.stats?' | '+s.stats.total_catalogs+'C/'+s.stats.total_schemas+'S/'+s.stats.total_tables+'T':'');
    // Auto-trigger first discovery if never run
    if(!s.last_refreshed && !s.refresh_in_progress) window.genieRefreshCatalogs();
  }).catch(function(){});
}

/* ── Inject Catalog Browser ────────────────────────────────────── */
function _injectCatalogBrowser(){
  // Already injected as part of SQL panel
}

function _renderCatalogPanel(catalogs){
  var panel = $g('genieCatalogBrowser');
  if(!panel) return;
  if(!catalogs || !catalogs.length){
    panel.innerHTML = '<p style="font-size:11px;color:#6B7280;margin:0;">No catalogs discovered yet. Click Refresh to scan.</p>';
    return;
  }
  var html = '<div style="font-size:11px;">';
  catalogs.forEach(function(cat){
    html += '<div style="margin-bottom:6px;">' +
      '<div style="font-weight:700;color:#1E293B;cursor:pointer;" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\'none\'?\'block\':\'none\'">\ud83d\uddc3 ' + cat.catalog + ' <span style="color:#6B7280;font-weight:400;">(' + cat.table_count + ' tables)</span></div>' +
      '<div style="display:none;margin-left:12px;margin-top:3px;">';
    if(cat.schemas && cat.schemas.length){
      cat.schemas.forEach(function(s){
        html += '<div style="color:#4B5563;cursor:pointer;padding:2px 0;" onclick="var i=$g(\'genieSqlInput\');if(i){i.value=\'SELECT * FROM `'+cat.catalog+'`.`'+s+'`. LIMIT 10\';i.focus();}">' +
          '\u2514 ' + s + '</div>';
      });
    }
    html += '</div></div>';
  });
  html += '</div>';
  panel.innerHTML = html;
}

/* ── Inject SQL Run button into Genie input area ───────────────── */
function _injectSqlButton(){
  var sendBtn = $g('genieSendBtn');
  if(!sendBtn) return;
  var existing = $g('genieSqlToggle');
  if(existing) return;
  var btn = document.createElement('button');
  btn.id = 'genieSqlToggle';
  btn.title = 'Toggle SQL Editor (Multi-Catalog)';
  btn.innerHTML = '\ud83d\udcca';
  btn.style.cssText = 'background:none;border:none;font-size:16px;cursor:pointer;padding:4px 6px;margin-right:2px;opacity:0.7;';
  btn.onmouseover = function(){ this.style.opacity='1'; };
  btn.onmouseout = function(){ this.style.opacity='0.7'; };
  btn.onclick = function(){ window.genieToggleSqlPanel(); };
  sendBtn.parentElement.insertBefore(btn, sendBtn);
}

// Boot
function _init(){
  var attempts = 0;
  var timer = setInterval(function(){
    attempts++;
    var sel = $g('genieSpaceSelect');
    if(sel || attempts > 30){
      clearInterval(timer);
      _loadMcpEndpoints(function(){
        _injectMcpOptions();
        _injectAddPanelMcpTab();
        _injectSqlButton();
      });
    }
  }, 300);
}

if(document.readyState === 'loading'){
  document.addEventListener('DOMContentLoaded', _init);
} else {
  setTimeout(_init, 500);
}

})();
