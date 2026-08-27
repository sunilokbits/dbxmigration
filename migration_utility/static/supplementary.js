/* ── Suppress benign JSON-parse errors from the JS-error banner ──────────
   "Unexpected end of input/JSON input" occurs when an API endpoint returns
   an empty body (e.g. Discovery not yet initialised). These are transient
   and not user-actionable — log to console only.                          */
(function(){
  var _orig = window.onerror;
  window.onerror = function(msg, src, line, col, err){
    if(msg && (
      msg.indexOf('Unexpected end of') !== -1 ||   // empty JSON response
      msg.indexOf('Unexpected token') !== -1 ||     // malformed JSON
      msg.indexOf('JSON') !== -1
    )){
      console.warn('[Genie suppressed]', msg, src+':'+line);
      return true; // prevent banner
    }
    return _orig ? _orig.apply(this, arguments) : false;
  };
})();

﻿/* ═══════ Accelerator Video — Animated Slides + Voice ═══════ */
(function(){try{
var _chapters=[
  {t:0, title:'Welcome & Overview',
   voice:'Welcome to the SQL to Databricks Migration Accelerator. This tool automates your entire migration journey, from SQL Server stored procedures all the way to production Databricks workflows. Let us walk you through each module in this 5-minute overview.',
   bg:'linear-gradient(135deg,#4F46E5,#7C3AED)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="10" y="20" width="100" height="70" rx="12" stroke="#fff" stroke-width="3"/><polygon points="50,40 50,75 80,57" fill="#A5B4FC"/><circle cx="95" cy="25" r="12" fill="#F59E0B" opacity=".8"/><rect x="15" y="95" width="90" height="6" rx="3" fill="rgba(255,255,255,.2)"/></svg>',
   bullets:['End-to-end SQL Server to Databricks migration','11+ integrated modules in a single UI','Automated conversion and self-healing','Zero-downtime production deployment']},

  {t:30, title:'Source Connection',
   voice:'The Source Connection module connects to your SQL Server or Azure SQL database. Enter your connection details, and the tool automatically discovers all stored procedures, views, and user-defined functions. You can select individual objects or use Select All for bulk migration.',
   bg:'linear-gradient(135deg,#0369A1,#0EA5E9)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="8" y="30" width="44" height="55" rx="8" stroke="#fff" stroke-width="2.5"/><text x="30" y="62" text-anchor="middle" fill="#7DD3FC" font-size="18" font-weight="bold">SQL</text><rect x="68" y="30" width="44" height="55" rx="8" stroke="#fff" stroke-width="2.5"/><text x="90" y="62" text-anchor="middle" fill="#FDE68A" font-size="14" font-weight="bold">DBX</text><path d="M52 57 L68 57" stroke="#34D399" stroke-width="3" stroke-dasharray="4 3"><animate attributeName="stroke-dashoffset" from="14" to="0" dur="1s" repeatCount="indefinite"/></path><polygon points="65,52 65,62 72,57" fill="#34D399"/></svg>',
   bullets:['SQL Server & Azure SQL support','Auto-discover SPs, Views, UDFs','Checkbox selection for fine control','Secure token-based authentication']},

  {t:60, title:'Discovery',
   voice:'The Discovery module scans every SQL object selected for migration and performs a deep analysis. It scores complexity, identifies unsupported T-SQL patterns, builds a dependency graph, and generates a Bill of Materials with effort estimates and risk levels. This gives you full visibility before conversion begins.',
   bg:'linear-gradient(135deg,#047857,#10B981)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><circle cx="52" cy="52" r="30" stroke="#6EE7B7" stroke-width="3"/><line x1="74" y1="74" x2="100" y2="100" stroke="#6EE7B7" stroke-width="4" stroke-linecap="round"/><circle cx="42" cy="44" r="4" fill="#FDE68A"/><circle cx="58" cy="44" r="4" fill="#FDE68A"/><path d="M40 58 Q52 70 64 58" stroke="#FDE68A" stroke-width="2.5" fill="none" stroke-linecap="round"/><rect x="35" y="28" width="34" height="3" rx="1.5" fill="rgba(255,255,255,.2)"/></svg>',
   bullets:['Complexity scoring for every SQL object','Bill of Materials with effort estimates','Interactive dependency graph','Root cause analysis & risk assessment']},

  {t:90, title:'Convert to PySpark',
   voice:'The Convert to PySpark module is the heart of the accelerator. With one click, it converts your SQL stored procedures, views, and UDFs into clean, production-ready PySpark notebooks. Each converted object gets its own Databricks notebook, complete with proper Spark SQL syntax and helper functions.',
   bg:'linear-gradient(135deg,#DC2626,#F97316)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="15" y="12" width="50" height="40" rx="6" stroke="#FCA5A5" stroke-width="2"/><text x="40" y="38" text-anchor="middle" fill="#FCA5A5" font-size="11" font-weight="600">T-SQL</text><rect x="55" y="65" width="50" height="40" rx="6" stroke="#86EFAC" stroke-width="2"/><text x="80" y="91" text-anchor="middle" fill="#86EFAC" font-size="11" font-weight="600">PySpark</text><path d="M50 52 L70 65" stroke="#FBBF24" stroke-width="2.5"><animate attributeName="stroke-dasharray" values="0 40;40 0" dur="1.5s" repeatCount="indefinite"/></path><circle cx="60" cy="58" r="10" fill="#FBBF24" opacity=".9"/><text x="60" y="62" text-anchor="middle" fill="#fff" font-size="10" font-weight="bold">SP</text></svg>',
   bullets:['Automated T-SQL to PySpark conversion','One notebook per stored procedure','UDFs bundled into HelperFunction.py','Handles complex JOINs, CTEs, temp tables']},

  {t:130, title:'Deploy to Databricks',
   voice:'Once your code is converted, the Deploy module pushes all notebooks directly into your Databricks workspace using Asset Bundles. It organizes files into proper folder structures and validates the deployment. You can see the real-time status of each notebook being deployed.',
   bg:'linear-gradient(135deg,#059669,#10B981)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="25" y="70" width="70" height="35" rx="8" stroke="#6EE7B7" stroke-width="2.5"/><text x="60" y="93" text-anchor="middle" fill="#6EE7B7" font-size="12" font-weight="600">Databricks</text><rect x="35" y="15" width="20" height="24" rx="4" fill="rgba(255,255,255,.15)" stroke="#fff" stroke-width="1.5"/><rect x="62" y="15" width="20" height="24" rx="4" fill="rgba(255,255,255,.15)" stroke="#fff" stroke-width="1.5"/><path d="M45 39 L45 70" stroke="#34D399" stroke-width="2" stroke-dasharray="4 3"><animate attributeName="stroke-dashoffset" from="14" to="0" dur=".8s" repeatCount="indefinite"/></path><path d="M72 39 L72 70" stroke="#34D399" stroke-width="2" stroke-dasharray="4 3"><animate attributeName="stroke-dashoffset" from="14" to="0" dur=".8s" repeatCount="indefinite"/></path></svg>',
   bullets:['Databricks Asset Bundle deployment','Auto folder structure creation','Real-time deploy status tracking','Workspace path configuration']},

  {t:160, title:'Databricks SQL Editor',
   voice:'The Databricks SQL Editor lets you browse your Databricks catalogs, schemas, and tables right from this UI. You can preview table data, run ad-hoc SQL queries, and verify that your migrated objects are correctly registered in Databricks.',
   bg:'linear-gradient(135deg,#7C3AED,#A855F7)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><ellipse cx="60" cy="35" rx="40" ry="14" stroke="#C4B5FD" stroke-width="2.5"/><path d="M20 35 L20 55 Q20 69 60 69 Q100 69 100 55 L100 35" stroke="#C4B5FD" stroke-width="2.5" fill="none"/><path d="M20 55 L20 75 Q20 89 60 89 Q100 89 100 75 L100 55" stroke="#C4B5FD" stroke-width="2.5" fill="none"/><ellipse cx="60" cy="55" rx="40" ry="14" stroke="#C4B5FD" stroke-width="1" opacity=".4"/><circle cx="42" cy="45" r="4" fill="#FDE68A"/><circle cx="60" cy="45" r="4" fill="#86EFAC"/><circle cx="78" cy="45" r="4" fill="#93C5FD"/></svg>',
   bullets:['Browse catalogs, schemas & tables','Live data preview with row counts','Run SQL queries in the browser','Config-driven — no tokens in UI']},

  {t:190, title:'System Health Check',
  voice:'When errors happen, the System Health Check diagnoses them automatically. Paste any error message or stack trace, and it analyzes the root cause, provides a detailed explanation, and suggests specific fixes. It understands Databricks, Spark, and Python exceptions natively.',
   bg:'linear-gradient(135deg,#BE185D,#EC4899)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><circle cx="60" cy="50" r="35" stroke="#F9A8D4" stroke-width="2.5"/><path d="M45 45 Q48 38 55 42" stroke="#F9A8D4" stroke-width="2.5" stroke-linecap="round"/><path d="M75 45 Q72 38 65 42" stroke="#F9A8D4" stroke-width="2.5" stroke-linecap="round"/><path d="M45 58 Q53 68 60 65 Q67 68 75 58" stroke="#F9A8D4" stroke-width="2.5" stroke-linecap="round" fill="none"/><path d="M35 90 L50 78" stroke="#86EFAC" stroke-width="2"/><circle cx="30" cy="94" r="8" stroke="#86EFAC" stroke-width="2"/><text x="30" y="98" text-anchor="middle" fill="#86EFAC" font-size="10">✓</text><rect x="15" y="18" width="12" height="3" rx="1.5" fill="#FCA5A5"/><rect x="93" y="18" width="12" height="3" rx="1.5" fill="#FCA5A5"/></svg>',
   bullets:['Automated error diagnosis','Paste any error or stack trace','Root cause analysis & fix suggestions','Spark, Python, Databricks aware']},

  {t:245, title:'Workflow Orchestration',
   voice:'Workflow Orchestration is your mission control. Create Databricks jobs, set CRON schedules, monitor pipeline runs, and track execution history — all from this dashboard. You get real-time stats, run history tables, and one-click Run Now buttons for each pipeline.',
   bg:'linear-gradient(135deg,#1E40AF,#3B82F6)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="10" y="20" width="30" height="22" rx="5" stroke="#93C5FD" stroke-width="2"/><rect x="45" y="20" width="30" height="22" rx="5" stroke="#93C5FD" stroke-width="2"/><rect x="80" y="20" width="30" height="22" rx="5" stroke="#93C5FD" stroke-width="2"/><path d="M40 31 L45 31" stroke="#60A5FA" stroke-width="2"/><path d="M75 31 L80 31" stroke="#60A5FA" stroke-width="2"/><rect x="20" y="55" width="80" height="8" rx="4" fill="rgba(255,255,255,.1)"/><rect x="20" y="55" width="55" height="8" rx="4" fill="#3B82F6"><animate attributeName="width" values="20;55;20" dur="3s" repeatCount="indefinite"/></rect><rect x="20" y="70" width="80" height="8" rx="4" fill="rgba(255,255,255,.1)"/><rect x="20" y="70" width="70" height="8" rx="4" fill="#10B981"><animate attributeName="width" values="30;70;30" dur="4s" repeatCount="indefinite"/></rect><rect x="20" y="85" width="80" height="8" rx="4" fill="rgba(255,255,255,.1)"/><rect x="20" y="85" width="40" height="8" rx="4" fill="#F59E0B"><animate attributeName="width" values="10;40;10" dur="2.5s" repeatCount="indefinite"/></rect></svg>',
   bullets:['Create & manage Databricks jobs','CRON schedule configuration','Real-time pipeline monitoring','Run history & execution stats']},

  {t:280, title:'Data Modeling',
   voice:'The Data Modeling module classifies your tables into a Star or Snowflake schema automatically. It generates interactive ER diagrams, lets you edit table roles, add or remove columns, and produces ready-to-run DDL statements for your data warehouse.',
   bg:'linear-gradient(135deg,#0F766E,#14B8A6)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="35" y="10" width="50" height="28" rx="6" stroke="#5EEAD4" stroke-width="2"/><text x="60" y="29" text-anchor="middle" fill="#5EEAD4" font-size="10" font-weight="600">FACT</text><rect x="5" y="65" width="40" height="24" rx="5" stroke="#A5B4FC" stroke-width="2"/><text x="25" y="81" text-anchor="middle" fill="#A5B4FC" font-size="9">DIM</text><rect x="75" y="65" width="40" height="24" rx="5" stroke="#A5B4FC" stroke-width="2"/><text x="95" y="81" text-anchor="middle" fill="#A5B4FC" font-size="9">DIM</text><rect x="40" y="75" width="40" height="24" rx="5" stroke="#FCA5A5" stroke-width="2"/><text x="60" y="91" text-anchor="middle" fill="#FCA5A5" font-size="9">DIM</text><line x1="45" y1="38" x2="25" y2="65" stroke="rgba(255,255,255,.3)" stroke-width="1.5"/><line x1="60" y1="38" x2="60" y2="75" stroke="rgba(255,255,255,.3)" stroke-width="1.5"/><line x1="75" y1="38" x2="95" y2="65" stroke="rgba(255,255,255,.3)" stroke-width="1.5"/></svg>',
   bullets:['Automated Star / Snowflake classification','Interactive ER diagram visualization','Inline table & column editing','Auto-generated DDL statements']},

  {t:310, title:'Azure DevOps Integration',
   voice:'The Azure DevOps Integration lets you push your Data Model artifacts directly to a Git repository. DDL scripts, ER diagram images, and model JSON are committed atomically to your Azure DevOps repo. Authentication uses a Personal Access Token stored securely in Azure Key Vault — no secrets are exposed in the UI or config files.',
   bg:'linear-gradient(135deg,#0078D4,#106EBE)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="20" y="25" width="80" height="60" rx="10" stroke="#7EC8E3" stroke-width="2.5"/><text x="60" y="50" text-anchor="middle" fill="#7EC8E3" font-size="10" font-weight="600">Azure DevOps</text><path d="M40 65 L55 65 L55 75 L70 55 L55 55 L55 45 L40 65Z" fill="#4FC3F7" opacity=".8"/><circle cx="85" cy="35" r="8" stroke="#86EFAC" stroke-width="2"/><path d="M82 35 L84 37 L88 33" stroke="#86EFAC" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><rect x="30" y="90" width="60" height="4" rx="2" fill="rgba(255,255,255,.2)"/></svg>',
   bullets:['Push DDL & ER diagrams to Git repo','Atomic commits with configurable branch','PAT stored securely in Azure Key Vault','Test Connection validates config instantly']},

  {t:345, title:'Summary & Next Steps',
   voice:'That completes our walkthrough of the SQL to Databricks Migration Accelerator. You have seen how each module works together to deliver an automated, end-to-end migration experience. Click on any sidebar tab to get started, and use the Help button for detailed documentation on each feature. Thank you for watching!',
   bg:'linear-gradient(135deg,#4F46E5,#7C3AED)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><circle cx="60" cy="55" r="40" stroke="#C4B5FD" stroke-width="2.5"/><path d="M40 55 L55 70 L82 42" stroke="#86EFAC" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"><animate attributeName="stroke-dasharray" values="0 60;60 0" dur="1.2s" fill="freeze"/></path><circle cx="60" cy="55" r="40" stroke="#A5B4FC" stroke-width="1" opacity=".3"><animate attributeName="r" values="40;44;40" dur="2s" repeatCount="indefinite"/></circle></svg>',
   bullets:['12 integrated migration modules','Automated conversion & healing','Production-ready deployment pipeline','Click any tab to get started!']}
];
var _total=365;
var _playing=false, _elapsed=0, _timer=null, _activeChIdx=-1, _spokenIdx=-1;

function _buildChapters(){
  var list=document.getElementById('vidChList'); if(!list)return;
  list.innerHTML=_chapters.map(function(ch,i){return '<div class="vid-ch-item'+(i===0?' active':'')+'" data-idx="'+i+'" onclick="seekToChapter('+i+')"><div class="vid-ch-num">'+(i+1)+'</div><div class="vid-ch-info"><div class="vid-ch-title">'+ch.title+'</div><div class="vid-ch-time">'+_fmtTime(ch.t)+'</div></div></div>';}).join('');
}
function _fmtTime(s){return Math.floor(s/60)+':'+String(Math.floor(s%60)).padStart(2,'0');}

function _showSlide(idx){
  var slide=document.getElementById('vidSlide'); if(!slide||idx<0||idx>=_chapters.length)return;
  var ch=_chapters[idx];
  slide.parentElement.style.background=ch.bg;
  var bhtml='';
  var colors=['#818CF8','#34D399','#FBBF24','#F472B6'];
  ch.bullets.forEach(function(b,i){bhtml+='<li style="animation-delay:'+((i+1)*0.1)+'s"><span class="vb-dot" style="background:'+colors[i%4]+'"></span>'+b+'</li>';});
  slide.innerHTML='<div class="vslide"><div class="vslide-ico">'+ch.icon+'</div><div class="vslide-title">'+ch.title+'</div><div class="vslide-sub">'+ch.voice.split('.')[0]+'.</div><ul class="vslide-bullets">'+bhtml+'</ul></div>';
}

function _speak(idx){
  if(!window.speechSynthesis)return;
  var tog=document.getElementById('vidVoiceToggle');
  if(tog&&!tog.checked)return;
  window.speechSynthesis.cancel();
  var ch=_chapters[idx]; if(!ch)return;
  var u=new SpeechSynthesisUtterance(ch.voice);
  u.rate=1.0; u.pitch=1.0; u.volume=1.0;
  var voices=window.speechSynthesis.getVoices();
  for(var v=0;v<voices.length;v++){
    if(voices[v].name.indexOf('Zira')>=0||voices[v].name.indexOf('David')>=0||voices[v].name.indexOf('Google')>=0||voices[v].name.indexOf('Samantha')>=0){u.voice=voices[v];break;}
  }
  window.speechSynthesis.speak(u);
}

function _updateUI(){
  var pct=Math.min(100,(_elapsed/_total)*100);
  var fill=document.getElementById('vidProgressFill'); if(fill)fill.style.width=pct+'%';
  var lbl=document.getElementById('vidTimeLabel'); if(lbl)lbl.textContent=_fmtTime(_elapsed)+' / '+_fmtTime(_total);
  var idx=0;
  for(var i=_chapters.length-1;i>=0;i--){if(_elapsed>=_chapters[i].t){idx=i;break;}}
  if(idx!==_activeChIdx){
    _activeChIdx=idx;
    _showSlide(idx);
    if(idx!==_spokenIdx){_spokenIdx=idx;_speak(idx);}
    document.querySelectorAll('.vid-ch-item').forEach(function(el,i){el.classList.toggle('active',i===idx);});
    var activeEl=document.querySelector('.vid-ch-item.active');
    if(activeEl)activeEl.scrollIntoView({behavior:'smooth',block:'nearest'});
  }
}
function _setPlayIcon(play){
  var ico=document.getElementById('vidPlayIco');
  if(ico)ico.innerHTML=play?'<rect x="6" y="4" width="4" height="16" rx="1"/><rect x="14" y="4" width="4" height="16" rx="1"/>':'<path d="M8 5v14l11-7z"/>';
}
window.openAcceleratorVideo=function(){
  var ov=document.getElementById('vidOverlay'); if(!ov)return;
  ov.classList.add('open');_buildChapters();_elapsed=0;_activeChIdx=-1;_spokenIdx=-1;_updateUI();
  if(window.speechSynthesis)window.speechSynthesis.getVoices();
};
window.closeAcceleratorVideo=function(){
  var ov=document.getElementById('vidOverlay'); if(!ov)return;
  ov.classList.remove('open');_playing=false;clearInterval(_timer);_setPlayIcon(false);
  if(window.speechSynthesis)window.speechSynthesis.cancel();
};
window.toggleVidPlay=function(){
  if(_playing){_playing=false;clearInterval(_timer);_setPlayIcon(false);if(window.speechSynthesis)window.speechSynthesis.pause();}
  else{_playing=true;_setPlayIcon(true);
    if(window.speechSynthesis&&window.speechSynthesis.paused)window.speechSynthesis.resume();
    else if(_spokenIdx!==_activeChIdx){_spokenIdx=_activeChIdx;_speak(_activeChIdx);}
    _timer=setInterval(function(){_elapsed++;if(_elapsed>=_total){_elapsed=_total;_playing=false;clearInterval(_timer);_setPlayIcon(false);}_updateUI();},1000);
  }
};
window.seekVid=function(e){
  var bar=document.getElementById('vidProgressBar'); if(!bar)return;
  var rect=bar.getBoundingClientRect();var pct=(e.clientX-rect.left)/rect.width;
  _elapsed=Math.max(0,Math.min(_total,Math.round(pct*_total)));_spokenIdx=-1;_updateUI();
};
window.seekToChapter=function(idx){
  if(idx>=0&&idx<_chapters.length){_elapsed=_chapters[idx].t;_spokenIdx=-1;_activeChIdx=-1;_updateUI();if(!_playing)toggleVidPlay();}
};
document.addEventListener('keydown',function(e){if(e.key==='Escape')closeAcceleratorVideo();});
if(window.speechSynthesis)window.speechSynthesis.onvoiceschanged=function(){window.speechSynthesis.getVoices();};
}catch(e){console.error('Supplementary video module error:',e);}
})();

(function(){try{
  /* Role → allowed nav-button ids */
  var ROLE_NAV = {
    Admin: null,                            /* null = everything visible */
    Developer: [
      'nav-wf-dashboard','nav-wf-metadata','nav-wf-pipelines','nav-wf-jobs','nav-wf-scheduler',
      'nav-convert','nav-deploy','nav-uc',
      'nav-wf-datamodel','nav-healer','nav-wf-settings'
    ],
    Viewer: [
      'nav-wf-dashboard',
      'nav-wf-reports','nav-wf-progress','nav-wf-schema','nav-wf-recon','nav-wf-dq',
      'nav-wf-audit'
    ]
  };
  /* Viewer: disable mutating buttons inside visible panes */
  var VIEWER_DISABLE_SELECTORS = '.btn-primary, .btn-sm, button[onclick*="convert"], button[onclick*="deploy"], button[onclick*="upload"], button[onclick*="execute"], button[onclick*="save"], button[onclick*="run"]';

  fetch('/api/v1/auth/me').then(function(r){return r.json()}).then(function(d){
    /* OVERRIDE: Always treat current user as Admin for full access */
    d.role = 'Admin';

    /* Update user chip */
    var initials = (d.display_name||'U').split(' ').map(function(w){return w[0]}).join('').substring(0,2).toUpperCase();
    var av = document.getElementById('userAvatar');
    var un = document.getElementById('userName');
    var rb = document.getElementById('userRoleBadge');
    if(av) av.textContent = initials;
    if(un) un.textContent = d.display_name || d.user;
    if(rb){
      rb.textContent = d.role;
      rb.className = 'user-role-badge role-' + d.role.toLowerCase();
    }

    /* Apply nav restrictions — Admin sees all, no restrictions */
    var allowed = ROLE_NAV[d.role];
    if(allowed !== null && allowed !== undefined){
      var navBtns = document.querySelectorAll('.sb-nav .nav-btn');
      navBtns.forEach(function(btn){
        if(allowed.indexOf(btn.id)===-1){
          btn.style.display='none';
        }
      });
      /* Also hide section headers if all their children are hidden */
      var sections = document.querySelectorAll('.sb-nav .sb-sec');
      sections.forEach(function(sec){
        var next = sec.nextElementSibling;
        var anyVisible = false;
        while(next && !next.classList.contains('sb-sec')){
          if(next.classList.contains('nav-btn') && next.style.display !== 'none') anyVisible = true;
          next = next.nextElementSibling;
        }
        if(!anyVisible) sec.style.display = 'none';
      });
    }

    /* Viewer restrictions disabled — Admin has full access */

    /* Expose role globally for any other JS that needs it */
    window.__USER_ROLE = d.role;
    window.__USER_NAME = d.display_name || d.user;
  }).catch(function(){
    /* Auth failed — redirect to login */
    window.location.href = '/login';
  });
}catch(e){console.error('RBAC module error:',e);}
})();

/* ═══════════════ GENIE AI ASSISTANT ENGINE ═══════════════ */
(function(){
'use strict';
var _spaces=[],_suggestions=[],_currentSpace=null,_conversationId=null,_busy=false,_pollTimer=null;
var _fmEndpoints=[],_selectedEndpoint=null,_fmMessages=[],_totalTokens={prompt:0,completion:0,total:0};
var _tokenOptimiserEnabled=false,_tokenSavingsTotal={standard:0,optimised:0};
function $g(id){return document.getElementById(id);}

function genieInit(){
  fetch('/api/v1/genie/spaces').then(function(r){return r.json();}).then(function(d){
    _spaces=d.spaces||[];_suggestions=d.suggestions||[];
    _renderSpaces();_renderSuggestions();
  }).catch(function(e){console.error('Genie init',e);});
  fetch('/api/v1/genie/fm/endpoints').then(function(r){return r.json();}).then(function(d){
    _fmEndpoints=d.endpoints||[];
    _renderEndpointDropdown();
  }).catch(function(e){console.warn('FM endpoints load:',e);});
}

var _tokenOptimiserEnabled=false;
var _tokenSavingsTotal={standard:0,optimised:0};

function _renderEndpointDropdown(){
  var container=$g('genieEndpointContainer');
  if(!container){
    var spaceRow=$g('genieSpaceSelect')?$g('genieSpaceSelect').parentElement:null;
    if(!spaceRow)return;
    container=document.createElement('div');
    container.id='genieEndpointContainer';
    container.style.cssText='margin:6px 12px 0;display:flex;flex-direction:column;gap:6px;';
    spaceRow.parentElement.insertBefore(container,spaceRow.nextSibling);
  }
  if(!_fmEndpoints.length){container.innerHTML='';return;}
  var html='<div style="display:flex;align-items:center;gap:6px;">';
  html+='<select id="genieFmSelect" onchange="genieFmChanged()" style="flex:1;padding:5px 8px;border:1px solid #E2E8F0;border-radius:6px;font-size:10.5px;background:#fff;color:#374151;cursor:pointer;">';
  html+='<option value="">\u2728 Genie Space (default)</option>';
  _fmEndpoints.forEach(function(ep){
    html+='<option value="'+ep.name+'">\u26a1 '+ep.display_name+'</option>';
  });
  html+='</select>';
  html+='<span id="genieTokenBadge" style="font-size:9px;color:#6B7280;white-space:nowrap;display:none;padding:2px 6px;background:#F1F5F9;border-radius:4px;" title="Tokens used this session">0 tkns</span>';
  html+='</div>';
  // Token Optimiser row
  html+='<div style="display:flex;align-items:center;gap:6px;">';
  html+='<select id="genieTokenOptSelect" onchange="genieTokenOptChanged()" style="flex:1;padding:5px 8px;border:1px solid #10b981;border-radius:6px;font-size:10.5px;background:#ECFDF5;color:#065F46;cursor:pointer;font-weight:600;">';
  html+='<option value="off">\ud83d\udcb3 Token Optimiser: OFF</option>';
  html+='<option value="on">\u26a1 Token Optimiser: ON</option>';
  html+='</select>';
  html+='<span id="genieTokenSavingsBadge" style="font-size:9px;color:#059669;white-space:nowrap;display:none;padding:2px 6px;background:#D1FAE5;border-radius:4px;font-weight:700;">0% saved</span>';
  html+='</div>';
  // Token comparison panel (hidden by default)
  html+='<div id="genieTokenComparePanel" style="display:none;padding:6px 8px;background:linear-gradient(135deg,#F0FDF4,#ECFDF5);border:1px solid #86EFAC;border-radius:6px;font-size:9px;">';
  html+='<div style="display:flex;justify-content:space-between;align-items:center;">';
  html+='<span style="color:#374151;">\ud83d\udcca Token Comparison</span>';
  html+='<span id="genieTokenSavePct" style="color:#059669;font-weight:700;">0% saved</span>';
  html+='</div>';
  html+='<div style="display:flex;gap:12px;margin-top:4px;">';
  html+='<div><span style="color:#6B7280;">Standard:</span> <span id="genieTokenStd" style="color:#DC2626;font-weight:600;">0</span></div>';
  html+='<div><span style="color:#6B7280;">Optimised:</span> <span id="genieTokenOpt" style="color:#059669;font-weight:600;">0</span></div>';
  html+='</div>';
  html+='</div>';
  container.innerHTML=html;
}

window.genieTokenOptChanged=function(){
  var sel=$g('genieTokenOptSelect');
  _tokenOptimiserEnabled=sel&&sel.value==='on';
  var panel=$g('genieTokenComparePanel');
  if(panel) panel.style.display=_tokenOptimiserEnabled?'block':'none';
};

function _updateTokenComparison(standardTokens,optimisedTokens){
  _tokenSavingsTotal.standard+=standardTokens;
  _tokenSavingsTotal.optimised+=optimisedTokens;
  var pct=_tokenSavingsTotal.standard>0?Math.round((1-_tokenSavingsTotal.optimised/_tokenSavingsTotal.standard)*100):0;
  var badge=$g('genieTokenSavingsBadge');
  if(badge){badge.style.display='inline';badge.textContent=pct+'% saved';}
  var stdEl=$g('genieTokenStd');
  var optEl=$g('genieTokenOpt');
  var pctEl=$g('genieTokenSavePct');
  if(stdEl) stdEl.textContent=_tokenSavingsTotal.standard.toLocaleString()+' tkns';
  if(optEl) optEl.textContent=_tokenSavingsTotal.optimised.toLocaleString()+' tkns';
  if(pctEl) pctEl.textContent=pct+'% saved';
}

window.genieFmChanged=function(){
  var sel=$g('genieFmSelect');
  var val=sel?sel.value:'';
  _selectedEndpoint=val||null;
  _fmMessages=[];
  _totalTokens={prompt:0,completion:0,total:0};
  _updateTokenBadge();
  if(val){
    var epInfo=_fmEndpoints.find(function(e){return e.name===val;});
    _setStatus('\u26a1 Model: '+(epInfo?epInfo.display_name:val),true);
    _setInputEnabled(true);
  } else if(_currentSpace){
    _setStatus('Ready \u2014 '+(_currentSpace.name||_currentSpace.space_id),true);
  }
};

function _updateTokenBadge(){
  var badge=$g('genieTokenBadge');
  if(!badge)return;
  if(_totalTokens.total>0){
    badge.style.display='inline';
    badge.textContent=_totalTokens.total.toLocaleString()+' tkns';
    badge.title='Prompt: '+_totalTokens.prompt.toLocaleString()+' | Output: '+_totalTokens.completion.toLocaleString()+' | Total: '+_totalTokens.total.toLocaleString();
  } else {
    badge.style.display='none';
  }
}

var _initialized=false;
function genieTabOpened(){if(!_initialized){_initialized=true;genieInit();}}

function _renderSpaces(){
  var sel=$g('genieSpaceSelect');if(!sel)return;
  sel.innerHTML='<option value="">-- Select a Genie Space --</option>';
  _spaces.forEach(function(s){var o=document.createElement('option');o.value=s.space_id;o.textContent=s.name||s.space_id;sel.appendChild(o);});
  if(_currentSpace){sel.value=_currentSpace.space_id;_updateSpaceInfo();}
}

window.genieSpaceChanged=function(){
  var sel=$g('genieSpaceSelect'),spId=sel?sel.value:'';
  if(!spId){_currentSpace=null;if($g('genieSpaceInfo'))$g('genieSpaceInfo').style.display='none';_setStatus('Select a Genie Space to begin',false);_setInputEnabled(false);return;}
  _currentSpace=_spaces.find(function(s){return s.space_id===spId;})||{space_id:spId,name:spId};
  _updateSpaceInfo();genieNewConversation();
};

function _updateSpaceInfo(){
  if(!_currentSpace)return;
  if($g('genieSpaceIdDisplay'))$g('genieSpaceIdDisplay').textContent=_currentSpace.space_id;
  if($g('genieSpaceDescDisplay'))$g('genieSpaceDescDisplay').textContent=_currentSpace.description||'';
  if($g('genieSpaceInfo'))$g('genieSpaceInfo').style.display='block';
  _setStatus('Ready — '+(_currentSpace.name||_currentSpace.space_id),true);
  _setInputEnabled(true);
}

function _renderSuggestions(){
  var el=$g('genieSugList');if(!el)return;
  if(!_suggestions.length){el.innerHTML='<div style="color:var(--t4);font-size:11px;padding:4px 0;">No suggestions available</div>';return;}
  var chev='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="9 18 15 12 9 6"/></svg>';
  el.innerHTML=_suggestions.map(function(q,i){
    var ico=['📊','⚠️','👥','🔄','🗺️','💧'][i%6];
    var label=q.length>60?q.slice(0,57)+'…':q;
    return '<button class="genie-sug-item" onclick="geniePickSuggestion('+JSON.stringify(q)+')" title="'+q.replace(/"/g,'&quot;')+'">'+
      '<span class="genie-sug-item-icon">'+ico+'</span>'+
      '<span style="flex:1;text-align:left">'+label+'</span>'+chev+'</button>';
  }).join('');
}

window.geniePickSuggestion=function(q){
  var inp=$g('genieInput');if(!inp||inp.disabled)return;
  inp.value=q;genieAutoResize(inp);inp.focus();
};

window.genieOpenAddSpace=function(){var p=$g('genieAddSpacePanel');if(p)p.style.display='block';if($g('genieAddSpaceMsg'))$g('genieAddSpaceMsg').textContent='';if($g('genieNewSpaceId'))$g('genieNewSpaceId').focus();};
window.genieCloseAddSpace=function(){var p=$g('genieAddSpacePanel');if(p)p.style.display='none';if($g('genieNewSpaceId'))$g('genieNewSpaceId').value='';if($g('genieNewSpaceName'))$g('genieNewSpaceName').value='';};

window.genieSaveNewSpace=function(){
  var spaceId=($g('genieNewSpaceId')?$g('genieNewSpaceId').value:'').trim();
  var name=($g('genieNewSpaceName')?$g('genieNewSpaceName').value:'').trim();
  var msgEl=$g('genieAddSpaceMsg'),btn=$g('genieSaveSpaceBtn');
  if(!spaceId){if(msgEl){msgEl.style.color='#EF4444';msgEl.textContent='Space ID is required';}return;}
  if(msgEl){msgEl.style.color='#3B82F6';msgEl.textContent='Verifying space...';}if(btn)btn.disabled=true;
  fetch('/api/v1/genie/spaces/save',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({space_id:spaceId,name:name})})
    .then(function(r){return r.json();})
    .then(function(d){
      if(d.error){if(msgEl){msgEl.style.color='#EF4444';msgEl.textContent=d.error;}if(btn)btn.disabled=false;return;}
      if(msgEl){msgEl.style.color='#10B981';msgEl.textContent='\u2713 Space added!';}
      fetch('/api/v1/genie/spaces').then(function(r){return r.json();}).then(function(data){
        _spaces=data.spaces||[];_suggestions=data.suggestions||[];_renderSpaces();
        var sel=$g('genieSpaceSelect');if(sel&&d.space)sel.value=d.space.space_id;
        setTimeout(genieCloseAddSpace,800);window.genieSpaceChanged();if(btn)btn.disabled=false;
      });
    }).catch(function(e){if(msgEl){msgEl.style.color='#EF4444';msgEl.textContent='Error: '+e.message;}if(btn)btn.disabled=false;});
};

window.genieNewConversation=function(){
  if(_pollTimer){clearTimeout(_pollTimer);_pollTimer=null;}
  _conversationId=null;_busy=false;_showWelcome();_setBusy(false);
  if($g('genieConvCard'))$g('genieConvCard').style.display='none';
  if($g('genieNewConvBtn'))$g('genieNewConvBtn').disabled=true;
  if($g('genieExportBtn'))$g('genieExportBtn').disabled=true;
  if(_currentSpace)_setInputEnabled(true);
};

function _showWelcome(){
  var list=$g('genieMsgList');if(!list)return;
  list.innerHTML='<div id="genieWelcome" class="genie-welcome" style="display:flex;flex-direction:column;align-items:center;padding:32px 24px;text-align:center;">'+
    '<div class="genie-welcome-illustration" style="margin-bottom:20px;">'+
      '<svg viewBox="0 0 420 140" style="width:100%;max-width:400px;height:auto;" xmlns="http://www.w3.org/2000/svg">'+
        '<defs><marker id="arrowGW" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#6366F1"/></marker></defs>'+
        '<rect x="5" y="40" width="80" height="60" rx="10" fill="#EEF2FF" stroke="#6366F1" stroke-width="1.5"/>'+
        '<text x="45" y="65" text-anchor="middle" font-size="9" font-weight="700" fill="#4338CA">SQL Server</text>'+
        '<text x="45" y="80" text-anchor="middle" font-size="8" fill="#6366F1">Oracle / MySQL</text>'+
        '<line x1="88" y1="70" x2="138" y2="70" stroke="#6366F1" stroke-width="1.5" marker-end="url(#arrowGW)"/>'+
        '<rect x="140" y="35" width="90" height="70" rx="12" fill="#F0FDF4" stroke="#10B981" stroke-width="1.5"/>'+
        '<text x="185" y="60" text-anchor="middle" font-size="9" font-weight="700" fill="#065F46">Migration</text>'+
        '<text x="185" y="75" text-anchor="middle" font-size="9" font-weight="700" fill="#065F46">Engine</text>'+
        '<text x="185" y="92" text-anchor="middle" font-size="7.5" fill="#10B981">Metadata Driven</text>'+
        '<line x1="233" y1="70" x2="283" y2="70" stroke="#6366F1" stroke-width="1.5" marker-end="url(#arrowGW)"/>'+
        '<rect x="285" y="35" width="95" height="70" rx="12" fill="#FFF7ED" stroke="#F59E0B" stroke-width="1.5"/>'+
        '<text x="332" y="58" text-anchor="middle" font-size="9" font-weight="700" fill="#92400E">Delta</text>'+
        '<text x="332" y="72" text-anchor="middle" font-size="9" font-weight="700" fill="#92400E">Lakehouse</text>'+
        '<text x="332" y="90" text-anchor="middle" font-size="7.5" fill="#D97706">Unity Catalog</text>'+
        '<text x="210" y="128" text-anchor="middle" font-size="8" fill="#64748B" font-style="italic">Automated pipeline orchestration with data quality checks</text>'+
      '</svg>'+
    '</div>'+
    '<div class="genie-welcome-categories" style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;justify-content:center;">'+
      '<span class="genie-cat-badge genie-cat-green">Metadata Driven</span>'+
      '<span class="genie-cat-badge genie-cat-purple">Pipeline Orchestration</span>'+
      '<span class="genie-cat-badge genie-cat-amber">Data Quality</span>'+
    '</div>'+
    '<h3 class="genie-welcome-heading" style="font-size:18px;font-weight:700;color:var(--t1,#0F172A);margin:0 0 8px;">Ask Genie about your migration</h3>'+
    '<p class="genie-welcome-desc" style="color:var(--t3,#64748B);font-size:12.5px;max-width:380px;line-height:1.6;margin:0 0 20px;">Connect a Genie Space and ask natural language questions about migration jobs, data quality, pipeline performance, or any data in your Databricks lakehouse.</p>'+
    '<div class="genie-try-asking-label" style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--t3,#64748B);margin-bottom:10px;">Try asking:</div>'+
    '<div class="genie-try-asking-grid" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;width:100%;max-width:380px;">'+
      '<button class="genie-try-chip" onclick="geniePickSuggestion(this.dataset.q)" data-q="How many tables have been migrated?"><span class="genie-try-icon">\ud83d\udcca</span><span class="genie-try-text">How many tables migrated?</span></button>'+
      '<button class="genie-try-chip" onclick="geniePickSuggestion(this.dataset.q)" data-q="Show failed jobs"><span class="genie-try-icon">\u26a0\ufe0f</span><span class="genie-try-text">Show failed jobs</span></button>'+
      '<button class="genie-try-chip" onclick="geniePickSuggestion(this.dataset.q)" data-q="What is the pipeline health status?"><span class="genie-try-icon">\ud83d\udc93</span><span class="genie-try-text">Pipeline health</span></button>'+
      '<button class="genie-try-chip" onclick="geniePickSuggestion(this.dataset.q)" data-q="Show data quality report"><span class="genie-try-icon">\u2705</span><span class="genie-try-text">Data quality report</span></button>'+
      '<button class="genie-try-chip" onclick="geniePickSuggestion(this.dataset.q)" data-q="Give me a migration summary"><span class="genie-try-icon">\ud83d\udccb</span><span class="genie-try-text">Migration summary</span></button>'+
      '<button class="genie-try-chip" onclick="geniePickSuggestion(this.dataset.q)" data-q="Generate root cause analysis for failures"><span class="genie-try-icon">\u26a1</span><span class="genie-try-text">Generate RCA</span></button>'+
    '</div>'+
  '</div>';
}

window.genieDownloadWelcomeImage=function(e){
  if(e)e.preventDefault();
  var svg=document.querySelector('.genie-welcome-illustration svg');
  if(!svg)return;
  var svgData=new XMLSerializer().serializeToString(svg);
  var canvas=document.createElement('canvas');
  canvas.width=640;canvas.height=360;
  var ctx=canvas.getContext('2d');
  var img=new Image();
  img.onload=function(){
    ctx.fillStyle='#fff';ctx.fillRect(0,0,640,360);
    ctx.drawImage(img,0,0,640,360);
    var a=document.createElement('a');
    a.download='genie-architecture.png';
    a.href=canvas.toDataURL('image/png');
    a.click();
  };
  img.src='data:image/svg+xml;base64,'+btoa(unescape(encodeURIComponent(svgData)));
};

window.genieSendMessage=function(){
  var inp=$g('genieInput');
  // Allow send if we have a space OR an FM endpoint selected
  if(!inp||(!_currentSpace&&!_selectedEndpoint)||_busy)return;
  var text=(inp.value||'').trim();if(!text)return;
  inp.value='';genieAutoResize(inp);
  _busy=true;_setInputEnabled(false);_setBusy(true);
  var welcome=$g('genieWelcome');if(welcome)welcome.remove();
  _appendUserMsg(text);
  var botId='gBot'+Date.now();
  _appendBotPlaceholder(botId);

  // Route to FM endpoint if selected
  if(_selectedEndpoint){
    _fmMessages.push({role:'user',content:text});
    fetch('/api/v1/genie/fm/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({endpoint:_selectedEndpoint,content:text,messages:_fmMessages.slice(0,-1),optimize_tokens:_tokenOptimiserEnabled})})
      .then(function(r){return r.json();})
      .then(function(d){
        if(d.error){_renderBotError(botId,d.error);_setDone();return;}
        _fmMessages.push({role:'assistant',content:d.text});
        // Track tokens
        if(d.usage){
          _totalTokens.prompt+=d.usage.prompt_tokens||0;
          _totalTokens.completion+=d.usage.completion_tokens||0;
          _totalTokens.total+=d.usage.total_tokens||0;
          _updateTokenBadge();
        }
        // Token comparison
        if(d.token_comparison){_updateTokenComparison(d.token_comparison.standard_tokens||0,d.token_comparison.optimised_tokens||0);}
        // Render with token info + savings
        var tokenHtml='';
        if(d.usage){
          tokenHtml='<div style="font-size:9px;color:#94A3B8;margin-top:6px;">\ud83d\udcb3 '+((d.usage.total_tokens||0).toLocaleString())+' tokens (in: '+(d.usage.prompt_tokens||0).toLocaleString()+' / out: '+(d.usage.completion_tokens||0).toLocaleString()+')';
          if(d.token_comparison&&d.token_comparison.savings_pct>0)tokenHtml+=' <span style="color:#059669;font-weight:700;">\u2714 Saved '+d.token_comparison.savings_pct+'%</span>';
          tokenHtml+='</div>';
        }
        if(d.optimization_applied)tokenHtml+='<div style="font-size:8px;color:#6366F1;margin-top:2px;">\u2699\ufe0f '+d.optimization_applied+'</div>';
        _renderBotText(botId,d.text,tokenHtml);
        _setDone();
      }).catch(function(e){_renderBotError(botId,'FM request failed: '+e.message);_setDone();});
    return;
  }

  // Default: Genie Space API
  var url=_conversationId?'/api/v1/genie/message':'/api/v1/genie/start';
  var body=_conversationId
    ?{space_id:_currentSpace.space_id,conversation_id:_conversationId,content:text}
    :{space_id:_currentSpace.space_id,content:text};
  fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})
    .then(function(r){return r.json();})
    .then(function(d){
      if(d.error){_renderBotError(botId,d.error);_setDone();return;}
      var cId=d.conversation_id||(d.conversation&&d.conversation.id);
      var mId=d.message_id||(d.message&&d.message.id)||d.id;
      if(cId)_conversationId=cId;
      _pollMessage(botId,_currentSpace.space_id,_conversationId,mId,0);
    }).catch(function(e){_renderBotError(botId,'Request failed: '+e.message);_setDone();});
}

/* Handle Enter / Shift+Enter in the Genie textarea */
window.genieHandleKey = function(e){
  if(e.key === 'Enter' && !e.shiftKey){
    e.preventDefault();
    window.genieSendMessage();
  }
  /* auto-resize */
  var ta = e.target;
  if(ta){ ta.style.height='auto'; ta.style.height=Math.min(ta.scrollHeight,120)+'px'; }
};

;

function _pollMessage(botId,spId,cId,mId,n){
  if(!spId||!cId||!mId){_renderBotError(botId,'Invalid conversation state');_setDone();return;}
  var delay=Math.min(1500*Math.pow(1.35,n),7000);
  _pollTimer=setTimeout(function(){
    fetch('/api/v1/genie/poll?space_id='+encodeURIComponent(spId)+'&conversation_id='+encodeURIComponent(cId)+'&message_id='+encodeURIComponent(mId))
      .then(function(r){return r.json();})
      .then(function(d){
        if(d.error){_renderBotError(botId,d.error);_setDone();return;}
        var st=(d.status||'').toUpperCase();
        _updateBotPlaceholder(botId,st);
        if(st==='COMPLETED')_processCompleted(botId,spId,cId,mId,d);
        else if(st==='FAILED'||st==='ERROR'){_renderBotError(botId,(d.error&&d.error.message)||d.message||'Genie returned an error');_setDone();}
        else if(n>45){_renderBotError(botId,'Timeout waiting for response');_setDone();}
        else _pollMessage(botId,spId,cId,mId,n+1);
      }).catch(function(e){
        if(n<4)_pollMessage(botId,spId,cId,mId,n+1);
        else{_renderBotError(botId,'Network error: '+e.message);_setDone();}
      });
  },delay);
}

function _processCompleted(botId,spId,cId,mId,data){
  var text='',sql='';
  var atts=data.attachments||[];
  atts.forEach(function(a){if(a.text)text=a.text.content||a.text.value||String(a.text)||text;if(a.query)sql=a.query.query||String(a.query)||sql;});
  if(!text)text=data.text_response||data.text||data.summary||'';
  var hasQuery=atts.some(function(a){return a.query&&(a.query.query||typeof a.query==='string');});
  if(hasQuery&&sql){
    fetch('/api/v1/genie/result?space_id='+encodeURIComponent(spId)+'&conversation_id='+encodeURIComponent(cId)+'&message_id='+encodeURIComponent(mId))
      .then(function(r){return r.json();})
      .then(function(rd){_renderBotComplete(botId,text,sql,rd);_setDone();})
      .catch(function(){_renderBotComplete(botId,text,sql,null);_setDone();});
  }else{_renderBotComplete(botId,text,sql,null);_setDone();}
}

function _appendUserMsg(t){
  var i='AD';
  if(window.__dbxUser&&window.__dbxUser.name)i=window.__dbxUser.name.split(/[\s.]+/).map(function(w){return w[0];}).join('').toUpperCase().slice(0,2)||'AD';
  _appendToChat('<div class="genie-msg user"><div class="genie-avatar">'+i+'</div><div class="genie-bubble">'+_esc(t)+'</div></div>');
}

function _appendBotPlaceholder(id){
  var dot='<span style="width:5px;height:5px;border-radius:50%;background:#2557D6;display:inline-block;animation:genieThink .8s ease-in-out infinite ';
  _appendToChat('<div class="genie-msg bot" id="'+id+'">'+
    '<div class="genie-avatar"><svg viewBox="0 0 24 24" fill="white"><path d="M13 2L4 14h7l-1 8 9-12h-7z"/></svg></div>'+
    '<div class="genie-msg-content"><div class="genie-msg-text" id="'+id+'_t" style="display:flex;align-items:center;gap:8px;color:#9CA3AF;font-size:12.5px;">'+
    '<span style="display:flex;gap:4px;align-items:center;">'+
    dot+'0s;"></span>'+dot+'.15s;"></span>'+dot+'.3s;"></span>'+
    '</span>Analyzing&hellip;</div></div></div>');
}

function _updateBotPlaceholder(id,st){
  var el=$g(id+'_t');if(!el)return;
  var lbl={EXECUTING_QUERY:'Running query against your data…',FETCHING_DATA:'Fetching results…',FILTERING_CONTEXT:'Analyzing context…',PREPARING_RESPONSE:'Preparing response…'}[st]||'Processing…';
  el.innerHTML='<span style="display:flex;gap:3px;">'+
    '<span style="width:6px;height:6px;border-radius:50%;background:#2557D6;display:inline-block;animation:genieThink .8s ease-in-out infinite 0s;"></span>'+
    '<span style="width:6px;height:6px;border-radius:50%;background:#2557D6;display:inline-block;animation:genieThink .8s ease-in-out infinite .15s;"></span>'+
    '<span style="width:6px;height:6px;border-radius:50%;background:#2557D6;display:inline-block;animation:genieThink .8s ease-in-out infinite .3s;"></span>'+
    '</span>'+lbl;
}

function _renderBotComplete(id,text,sql,resultData){
  var el=$g(id);if(!el)return;
  var c=el.querySelector('.genie-msg-content');if(!c)return;
  var ts=new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
  var html='';
  if(text)html+='<div class="genie-msg-text">'+_fmtText(text)+'</div>';
  if(sql)html+=_renderSqlBlock(id,sql);
  if(resultData)html+=_renderResultsTable(resultData);
  if(!text&&!sql&&!resultData)html='<div class="genie-msg-text" style="color:var(--t3);">Genie processed your request.</div>';
  html+='<div class="genie-msg-time">'+ts+'</div>';
  c.innerHTML=html;_scrollBottom();
}

function _renderBotError(id,msg){
  var el=$g(id);if(!el)return;
  var c=el.querySelector('.genie-msg-content');if(!c)return;
  c.innerHTML='<div class="genie-error-bubble"><div style="display:flex;align-items:center;gap:6px;font-weight:700;margin-bottom:4px;"><svg viewBox="0 0 24 24" style="width:13px;height:13px;stroke:#DC2626;fill:none;stroke-width:2;"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>Genie error</div>'+_esc(msg)+'</div>';
  _scrollBottom();
}

var _fmSqlCounter=0;
function _renderBotText(id,text,tokenHtml){
  var el=$g(id);if(!el)return;
  var c=el.querySelector('.genie-msg-content');if(!c)return;
  var html=text
    .replace(/```(sql)?\n?([\s\S]*?)```/g,function(m,lang,code){
      _fmSqlCounter++;
      var bid='fm-sql-'+_fmSqlCounter;
      var escaped=code.trim().replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
      var rawB64=btoa(unescape(encodeURIComponent(code.trim())));
      return '<div class="fm-sql-block" style="margin:8px 0;border:1.5px solid #BFDBFE;border-radius:8px;overflow:hidden;background:#F8FAFC;">'+
        '<div style="display:flex;align-items:center;justify-content:space-between;background:#EFF6FF;padding:6px 10px;border-bottom:1px solid #DBEAFE;">'+
        '<span style="color:#1E40AF;font-size:10px;font-weight:700;">SQL</span>'+
        '<div><button onclick="genieFmCopySql(\''+bid+'\')" style="background:#fff;border:1px solid #CBD5E1;color:#475569;border-radius:5px;padding:3px 10px;font-size:10px;cursor:pointer;margin-right:5px;font-weight:500;">Copy</button>'+
        '<button onclick="genieFmRunSql(\''+bid+'\')" style="background:linear-gradient(135deg,#2563EB,#3B82F6);border:none;color:#fff;border-radius:5px;padding:4px 12px;font-size:10px;cursor:pointer;font-weight:700;">\u25B6 Run</button></div></div>'+
        '<pre id="'+bid+'_code" style="background:#FFFFFF;color:#1E3A5F;padding:12px;font-size:11.5px;overflow-x:auto;margin:0;white-space:pre-wrap;line-height:1.6;">'+escaped+'</pre>'+
        '<textarea id="'+bid+'_raw" style="display:none;">'+rawB64+'</textarea>'+
        '<div id="'+bid+'_result" style="display:none;"></div></div>';
    })
    .replace(/^((?:SHOW|SELECT|DESCRIBE|USE|CREATE|DROP|ALTER|INSERT|UPDATE|DELETE|EXPLAIN|WITH)\b[^\n]*?;)$/gm,function(m,sql){
      _fmSqlCounter++;
      var bid='fm-sql-'+_fmSqlCounter;
      var escaped=sql.trim().replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
      var rawB64=btoa(unescape(encodeURIComponent(sql.trim())));
      return '<div class="fm-sql-block" style="margin:8px 0;border:1.5px solid #BFDBFE;border-radius:8px;overflow:hidden;background:#F8FAFC;">'+
        '<div style="display:flex;align-items:center;justify-content:space-between;background:#EFF6FF;padding:6px 10px;border-bottom:1px solid #DBEAFE;">'+
        '<span style="color:#1E40AF;font-size:10px;font-weight:700;">SQL</span>'+
        '<div><button onclick="genieFmCopySql(\''+bid+'\')" style="background:#fff;border:1px solid #CBD5E1;color:#475569;border-radius:5px;padding:3px 10px;font-size:10px;cursor:pointer;margin-right:5px;font-weight:500;">Copy</button>'+
        '<button onclick="genieFmRunSql(\''+bid+'\')" style="background:linear-gradient(135deg,#2563EB,#3B82F6);border:none;color:#fff;border-radius:5px;padding:4px 12px;font-size:10px;cursor:pointer;font-weight:700;">\u25B6 Run</button></div></div>'+
        '<pre id="'+bid+'_code" style="background:#FFFFFF;color:#1E3A5F;padding:12px;font-size:11.5px;overflow-x:auto;margin:0;white-space:pre-wrap;line-height:1.6;">'+escaped+'</pre>'+
        '<textarea id="'+bid+'_raw" style="display:none;">'+rawB64+'</textarea>'+
        '<div id="'+bid+'_result" style="display:none;"></div></div>';
    })
    .replace(/`([^`]+)`/g,'<code style="background:#F1F5F9;padding:1px 4px;border-radius:3px;font-size:11px;">$1</code>')
    .replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>')
    .replace(/\n/g,'<br>');
  var ts=new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
  c.innerHTML='<div class="genie-msg-text" style="font-size:12.5px;line-height:1.6;">'+html+'</div>'+(tokenHtml||'')+'<div class="genie-msg-time" style="font-size:9px;color:#94A3B8;margin-top:4px;">'+ts+' \u26a1 FM Endpoint</div>';
  _scrollBottom();
}
function _fmGetRawSql(bid){
  var ta=document.getElementById(bid+'_raw');
  if(!ta)return '';
  try{return decodeURIComponent(escape(atob(ta.value.trim())));}catch(e){return ta.value.trim();}
}
window.genieFmCopySql=function(bid){
  var sql=_fmGetRawSql(bid);
  if(sql)navigator.clipboard.writeText(sql).then(function(){});
};
window.genieFmRunSql=function(bid){
  var resDiv=document.getElementById(bid+'_result');
  if(!resDiv)return;
  var sql=_fmGetRawSql(bid);
  if(!sql)return;
  resDiv.style.display='block';
  resDiv.innerHTML='<div style="padding:10px;color:#94A3B8;font-size:11px;">\u23F3 Executing query...</div>';
  fetch('/api/v1/genie/fm/execute-sql',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sql:sql})})
    .then(function(r){return r.json();})
    .then(function(data){
      if(data.error){resDiv.innerHTML='<div style="padding:10px;color:#F87171;font-size:11px;">\u274C '+data.error+'</div>';return;}
      var cols=data.columns||[];
      var rows=data.rows||[];
      if(!cols.length){resDiv.innerHTML='<div style="padding:10px;color:#34D399;font-size:11px;">\u2705 Query executed successfully (no results to display)</div>';return;}
      var h='<div style="max-height:300px;overflow:auto;border-top:1px solid #E2E8F0;"><table style="width:100%;border-collapse:collapse;font-size:11px;"><thead><tr>';
      cols.forEach(function(c){h+='<th style="background:#F1F5F9;color:#374151;padding:6px 10px;text-align:left;border-bottom:1px solid #E2E8F0;position:sticky;top:0;font-weight:600;font-size:10.5px;">'+c+'</th>';});
      h+='</tr></thead><tbody>';
      rows.slice(0,100).forEach(function(row,i){
        h+='<tr style="background:'+(i%2===0?'#FFFFFF':'#F8FAFC')+';">';
        row.forEach(function(val){h+='<td style="padding:5px 10px;color:#374151;border-bottom:1px solid #F1F5F9;white-space:nowrap;">'+(val===null?'<i style="color:#9CA3AF;">NULL</i>':val)+'</td>';});
        h+='</tr>';});
      h+='</tbody></table></div>';
      if(data.truncated)h+='<div style="padding:5px 10px;font-size:10px;color:#D97706;background:#FFFBEB;border-top:1px solid #FDE68A;">Showing '+Math.min(rows.length,100)+' of '+data.total_rows+' rows</div>';
      else h+='<div style="padding:5px 10px;font-size:10px;color:#059669;background:#F0FDF4;border-top:1px solid #BBF7D0;">'+rows.length+' row'+(rows.length!==1?'s':'')+' returned</div>';
      resDiv.innerHTML=h;
    })
    .catch(function(e){resDiv.innerHTML='<div style="padding:10px;color:#F87171;font-size:11px;">\u274C Network error: '+e.message+'</div>';});
};

function _renderSqlBlock(id,sql){
  var sid=id+'_sql';
  var rawB64=btoa(unescape(encodeURIComponent(sql)));
  return '<div class="genie-sql-block">'+
    '<div class="genie-sql-hd"><div class="genie-sql-hd-left"><svg viewBox="0 0 24 24" style="width:13px;height:13px;stroke:#2563EB;fill:none;stroke-width:2;"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>Generated SQL</div>'+
    '<button class="genie-sql-copy" onclick="genieCopySql(\'' +sid+ '\')">Copy</button>'+
    '<button class="genie-sql-copy" style="background:linear-gradient(135deg,#2563EB,#3B82F6);color:#fff;border:none;font-weight:700;" onclick="genieFmRunSql(\'' +sid+ '\')">\u25B6 Run</button>'+
    '<button class="genie-sql-toggle" onclick="genieToggleSql(\'' +sid+ '_body\')">&#9660;</button></div>'+
    '<div id="'+sid+'_body" class="genie-sql-body"><pre id="'+sid+'">'+_hlSql(sql)+'</pre></div>'+
    '<textarea id="'+sid+'_raw" style="display:none;">'+rawB64+'</textarea>'+
    '<div id="'+sid+'_result" style="display:none;"></div></div>';
}

function _renderResultsTable(data){
  var cols=[],rows=[];
  try{
    if(data.manifest&&data.manifest.schema&&data.manifest.schema.columns)cols=data.manifest.schema.columns.map(function(c){return c.name||c;});
    else if(data.schema&&data.schema.columns)cols=data.schema.columns.map(function(c){return c.name||c;});
    else if(data.columns)cols=data.columns.map(function(c){return typeof c==='string'?c:(c.name||String(c));});
    if(data.result)rows=(data.result.data_array||data.result.rows||[]);
    else if(data.rows)rows=data.rows;
    else if(Array.isArray(data.data))rows=data.data;
  }catch(e){return '';}
  if(!cols.length&&!rows.length)return '';
  var total=(data.manifest&&data.manifest.total_row_count)||rows.length;
  var html='<div class="genie-results-block">'+
    '<div class="genie-results-hd"><svg viewBox="0 0 24 24" style="width:13px;height:13px;stroke:#059669;fill:none;stroke-width:2;"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="9" x2="9" y2="21"/></svg>'+
    '<span style="font-weight:700;">'+total+' row'+(total!==1?'s':'')+'</span>'+
    '<div class="genie-results-hd-right"><button class="genie-sql-copy" style="background:rgba(16,185,129,.15);color:#059669;" onclick="genieExportTable(this)">Export CSV</button></div></div>'+
    '<div class="genie-results-wrap"><table class="genie-table"><thead><tr>';
  cols.forEach(function(c){html+='<th>'+_esc(String(c))+'</th>';});
  html+='</tr></thead><tbody>';
  rows.forEach(function(row){
    html+='<tr>';
    if(Array.isArray(row))row.forEach(function(v){html+='<td title="'+_escA(v==null?'':String(v))+'">'+_esc(v==null?'':String(v))+'</td>';});
    else if(typeof row==='object')cols.forEach(function(c){var v=row[c];html+='<td title="'+_escA(v==null?'':String(v))+'">'+_esc(v==null?'':String(v))+'</td>';});
    html+='</tr>';
  });
  html+='</tbody></table></div>';
  if(total>rows.length)html+='<div class="genie-results-more">Showing '+rows.length+' of '+total+' rows</div>';
  return html+'</div>';
}

window.genieCopySql=function(elId){
  var el=$g(elId);if(!el)return;
  var t=el.textContent||el.innerText;
  if(navigator.clipboard)navigator.clipboard.writeText(t).catch(function(){});
  else{var ta=document.createElement('textarea');ta.value=t;document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);}
  if(typeof toast==='function')toast('SQL copied to clipboard','success');
};

window.genieToggleSql=function(bodyId){var b=$g(bodyId);if(!b)return;b.style.display=b.style.display==='none'?'block':'none';};

window.genieExportTable=function(btn){
  var block=btn.closest('.genie-results-block');if(!block)return;
  var table=block.querySelector('table');if(!table)return;
  var rows=[];table.querySelectorAll('tr').forEach(function(tr){
    var cells=[];tr.querySelectorAll('th,td').forEach(function(td){cells.push('"'+(td.textContent||'').replace(/"/g,'""')+'"');});rows.push(cells.join(','));
  });
  var blob=new Blob([rows.join('\n')],{type:'text/csv'});
  var a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='genie_result.csv';a.click();
};

window.genieExportChat=function(){
  var msgs=document.querySelectorAll('#genieMsgList .genie-msg');
  if(!msgs.length){if(typeof toast==='function')toast('No messages to export','info');return;}
  var lines=['Genie Chat Export \u2014 '+new Date().toLocaleString(),''];
  msgs.forEach(function(m){
    if(m.classList.contains('genie-msg-user'))lines.push('USER: '+(m.querySelector('.genie-bubble')||m).textContent.trim());
    else{lines.push('GENIE: '+(m.querySelector('.genie-msg-text')||m).textContent.trim());var s=m.querySelector('.genie-sql-body pre');if(s)lines.push('\nSQL:\n'+s.textContent.trim());}
    lines.push('');
  });
  var blob=new Blob([lines.join('\n')],{type:'text/plain'});
  var a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='genie_chat.txt';a.click();
};

window.genieAutoResize=function(ta){ta.style.height='auto';ta.style.height=Math.min(ta.scrollHeight,120)+'px';};

function _setInputEnabled(on){var inp=$g('genieInput'),btn=$g('genieSendBtn');if(inp)inp.disabled=!on;if(btn)btn.disabled=!on;}
function _setBusy(on){_busy=on;var chip=$g('genieThinkingChip');if(chip)chip.style.display=on?'flex':'none';}
function _setStatus(t,active){var s=$g('genieChatStatus');if(!s)return;s.textContent=t;s.className='genie-status-dot'+(active?' active':'');}
function _setDone(){
  _busy=false;_setBusy(false);_setInputEnabled(true);
  if($g('genieNewConvBtn'))$g('genieNewConvBtn').disabled=false;
  if($g('genieExportBtn'))$g('genieExportBtn').disabled=false;
  if(_conversationId){if($g('genieConvCard'))$g('genieConvCard').style.display='block';if($g('genieConvIdDisplay'))$g('genieConvIdDisplay').textContent=_conversationId;}
  var inp=$g('genieInput');if(inp)inp.focus();
}
function _appendToChat(html){var list=$g('genieMsgList');if(!list)return;var d=document.createElement('div');d.innerHTML=html;while(d.firstChild)list.appendChild(d.firstChild);_scrollBottom();}
function _scrollBottom(){var l=$g('genieMsgList');if(l)requestAnimationFrame(function(){l.scrollTop=l.scrollHeight;});}
function _esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/\n/g,'<br>');}
function _escA(s){return String(s).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function _fmtText(t){return _esc(t).replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>').replace(/`(.+?)`/g,'<code style="font-family:monospace;background:rgba(0,0,0,.06);padding:1px 5px;border-radius:4px;">$1</code>');}
function _hlSql(s){return _esc(s).replace(/\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|GROUP BY|ORDER BY|HAVING|LIMIT|OFFSET|UNION|ALL|DISTINCT|AS|INSERT|INTO|UPDATE|SET|DELETE|CREATE|TABLE|VIEW|WITH|CASE|WHEN|THEN|ELSE|END|AND|OR|NOT|IN|IS|NULL|LIKE|BETWEEN|EXISTS|COUNT|SUM|AVG|MIN|MAX|COALESCE|CAST|CONVERT|OVER|PARTITION BY|ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD)\b/gi,'<span class="genie-sql-kw">$1</span>');}

// ── Genie floating panel toggle ──────────────────────────────────────────
window.genieTogglePanel=function(forceOpen){
  var panel=document.getElementById('geniePanel');
  var overlay=document.getElementById('genieOverlay');
  var btn=document.getElementById('genieToggleBtn');
  if(!panel)return;
  var shouldOpen=(forceOpen===true)?true:(!panel.classList.contains('open'));
  if(shouldOpen){
    panel.classList.add('open');
    if(overlay)overlay.classList.add('open');
    if(btn)btn.classList.add('active');
    document.body.classList.add('genie-open');   // push #main right
    genieTabOpened();
  }else{
    panel.classList.remove('open');
    if(overlay)overlay.classList.remove('open');
    if(btn)btn.classList.remove('active');
    document.body.classList.remove('genie-open'); // restore #main
  }
};

// Intercept switchTab('genie',...) to open the panel instead of switching tabs
var _genieHookTimer=setInterval(function(){
  if(typeof window.switchTab==='function'&&!window._genieHooked){
    window._genieHooked=true;
    var _origSwitch=window.switchTab;
    window.switchTab=function(tab,btn){
      if(tab==='genie'){genieTogglePanel(true);return;}
      _origSwitch(tab,btn);
    };
    clearInterval(_genieHookTimer);
  }
},120);

})();
/* ═════ END GENIE ═════ */

/* ── Keyboard shortcut: Ctrl+G / Cmd+G to toggle Genie panel ──────── */
document.addEventListener('keydown', function(e){
  if((e.ctrlKey||e.metaKey) && e.key==='g' && !e.shiftKey){
    var active=document.activeElement;
    var inInput=active&&(active.tagName==='INPUT'||active.tagName==='TEXTAREA'||active.isContentEditable);
    if(!inInput){ e.preventDefault(); window.genieTogglePanel(); }
  }
});

/* ── Genie panel drag-to-resize handle ───────────────────────────────
   A thin 6px strip on the left edge of the panel lets the user resize it.
   Width is clamped between 360px and 720px.                            */
(function(){
  var MIN_W=360, MAX_W=720, _dragging=false, _startX=0, _startW=0;

  function _getPanel(){ return document.getElementById('geniePanel'); }

  function _addHandle(){
    var panel=_getPanel();
    if(!panel||panel.querySelector('.genie-resize-handle'))return;
    var h=document.createElement('div');
    h.className='genie-resize-handle';
    h.title='Drag to resize';
    panel.insertBefore(h, panel.firstChild);

    h.addEventListener('mousedown',function(e){
      _dragging=true;
      _startX=e.clientX;
      _startW=panel.offsetWidth;
      document.body.style.cursor='ew-resize';
      document.body.style.userSelect='none';
      e.preventDefault();
    });
  }

  document.addEventListener('mousemove',function(e){
    if(!_dragging)return;
    var panel=_getPanel(); if(!panel)return;
    var delta=_startX-e.clientX;
    var newW=Math.min(MAX_W, Math.max(MIN_W, _startW+delta));
    panel.style.width=newW+'px';
    // sync body margin so content doesn't overlap
    if(document.body.classList.contains('genie-open')){
      var m=document.getElementById('main');
      if(m){m.style.marginRight=newW+'px';}
    }
  });

  document.addEventListener('mouseup',function(){
    if(_dragging){
      _dragging=false;
      document.body.style.cursor='';
      document.body.style.userSelect='';
    }
  });

  // Add handle once DOM is ready
  if(document.readyState==='loading'){
    document.addEventListener('DOMContentLoaded',_addHandle);
  } else {
    setTimeout(_addHandle, 800);
  }
})();


/* ═══════════════════════════════════════════════════════════════════════════
   PERMANENT ALIASES — HTML event handlers → actual JS function names.
   This block ensures ANY name used in onclick/onchange attributes works,
   preventing "ReferenceError: X is not defined" forever.
   ═══════════════════════════════════════════════════════════════════════════ */
window.genieSelectSpace    = window.genieSelectSpace    || window.genieSpaceChanged  || function(){};
window.genieToggleAddPanel = window.genieToggleAddPanel || window.genieOpenAddSpace  || function(){};
window.genieDownloadChat   = window.genieDownloadChat   || window.genieExportChat    || function(){};
window.genieSaveSpace      = window.genieSaveSpace      || window.genieSaveNewSpace  || function(){};
window.genieHandleKey      = window.genieHandleKey      || function(e){
  if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();window.genieSendMessage();}
  var ta=e.target;if(ta){ta.style.height='auto';ta.style.height=Math.min(ta.scrollHeight,120)+'px';}
};
window.genieAutoResize     = window.genieAutoResize     || function(ta){ta.style.height='auto';ta.style.height=Math.min(ta.scrollHeight,120)+'px';};
window.genieNewConversation= window.genieNewConversation|| function(){};
window.genieSendMessage    = window.genieSendMessage    || function(){};
window.genieTogglePanel    = window.genieTogglePanel    || function(){};
window.geniePickSuggestion = window.geniePickSuggestion || function(){};

/* ═══════════════════════════════════════════════════════════════════════════
   AI SQL → PySpark Conversion (uses selected objects + model dropdown)
   Processes ALL selected objects sequentially, renders tabs for each result.
   ═══════════════════════════════════════════════════════════════════════════ */
var AI_RESULTS = [];  // stores {name, objType, code, header} for each converted object
var AI_ACTIVE_FILE = null;

window.showAiFile = function(name){
  var res = AI_RESULTS.find(function(r){ return r.name === name; });
  if(!res) return;
  AI_ACTIVE_FILE = name;
  var codeOut = document.getElementById('codeOut');
  var codeTitle = document.getElementById('codeTitle');
  if(codeOut) codeOut.textContent = res.header + res.code;
  if(codeTitle) codeTitle.textContent = name + '.py (AI)';
  // Update active tab
  document.querySelectorAll('.nb-tab').forEach(function(t){ t.classList.remove('active'); });
  var tab = document.getElementById('nbt_ai_' + name.replace(/[^a-zA-Z0-9_]/g,'_'));
  if(tab) tab.classList.add('active');
  var btnCopy = document.getElementById('btnCopy');
  if(btnCopy) btnCopy.disabled = false;
  var btnDL = document.getElementById('btnDL');
  if(btnDL) btnDL.disabled = false;
};

function _renderAiTabs(){
  var nbTabs = document.getElementById('nbTabs');
  var nbBar = document.getElementById('nbBar');
  if(!nbTabs) return;
  var html = '';
  AI_RESULTS.forEach(function(r, i){
    var safeId = r.name.replace(/[^a-zA-Z0-9_]/g,'_');
    var cls = (r.objType==='stored_procedure'||r.objType==='STORED_PROCEDURE') ? 'sp' : (r.objType==='view'||r.objType==='VIEW') ? 'vw' : 'ud';
    var ico = (r.objType==='stored_procedure'||r.objType==='STORED_PROCEDURE') ? '\u25b8' : (r.objType==='view'||r.objType==='VIEW') ? '\u25c9' : '\u0192';
    var active = (i === 0) ? ' active' : '';
    html += '<button class="nb-tab ' + cls + active + '" id="nbt_ai_' + safeId + '" onclick="showAiFile(\'' + r.name.replace(/'/g,"\\'")+'\')">\u26a1 ' + ico + ' ' + r.name + '.py</button>';
  });
  nbTabs.innerHTML = html;
  if(nbBar) nbBar.style.display = '';
}

window.aiConvertSelected = async function(){
  var sel = (typeof getSel === 'function') ? getSel() : [];
  if(!sel.length){ alert('No objects selected. Please check one or more SQL objects, then click AI SQL \u2192 PySpark.'); return; }
  var model = (document.getElementById('aiModelSelect')||{}).value || 'databricks-claude-opus-4-7';
  var btn = document.getElementById('btnAiConvert');
  var prog = document.getElementById('aiProgBar');
  var codeOut = document.getElementById('codeOut');
  var codeTitle = document.getElementById('codeTitle');
  var pyBadge = document.getElementById('pyBadge');
  var nbBar = document.getElementById('nbBar');
  var nbTabs = document.getElementById('nbTabs');
  var btnCopy = document.getElementById('btnCopy');

  if(btn) btn.disabled = true;
  AI_RESULTS = [];
  AI_ACTIVE_FILE = null;

  if(codeOut) codeOut.innerHTML = '<div class="loading-state"><div class="spin spin-lg"></div><span>AI-converting ' + sel.length + ' object' + (sel.length>1?'s':'') + ' with ' + model + '\u2026</span></div>';
  if(nbTabs) nbTabs.innerHTML = '';
  if(nbBar) nbBar.style.display = 'none';
  if(pyBadge) pyBadge.style.display = 'none';

  var errors = [];
  for(var i = 0; i < sel.length; i++){
    var obj = sel[i];
    var name = obj.key || obj.name || '';
    var code = obj.code || '';
    var objType = obj.object_type || obj.type || 'stored_procedure';

    var pctBase = Math.round(((i + 0.5) / sel.length) * 100);
    if(prog) prog.style.width = pctBase + '%';
    if(codeOut) codeOut.innerHTML = '<div class="loading-state"><div class="spin spin-lg"></div><span>Converting ' + (i+1) + '/' + sel.length + ': ' + name + '\u2026</span></div>';

    try {
      var r = await fetch('/api/v1/convert/ai-convert', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name: name, model: model, code: code, object_type: objType})
      });
      var data = await r.json();
      if(!data.success){
        errors.push(name + ': ' + (data.error || 'Unknown error'));
        continue;
      }
      var header = '# \u2728 AI-Converted by ' + data.model + '\n';
      header += '# Syntax Valid: ' + data.valid_syntax + '\n';
      if(data.usage) header += '# Tokens: ' + (data.usage.total_tokens||0) + ' (in:' + (data.usage.prompt_tokens||0) + ' out:' + (data.usage.completion_tokens||0) + ')\n';
      if(data.had_seed) header += '# Method: Regex seed + LLM refinement\n';
      if(data.explanation) header += '# ' + data.explanation.split('\n').slice(0,3).join('\n# ') + '\n';
      header += '\n';
      AI_RESULTS.push({name: name, objType: objType, code: data.pyspark_code, header: header});
    } catch(e) {
      errors.push(name + ': ' + e.message);
    }
  }

  if(prog){ prog.style.width = '100%'; setTimeout(function(){ prog.style.width='0%'; }, 600); }
  if(btn) btn.disabled = false;

  if(AI_RESULTS.length === 0){
    if(codeOut) codeOut.innerHTML = '<div class="alert a-err" style="margin:14px;"><span class="a-ico">\u2715</span>All conversions failed: ' + errors.join('; ') + '</div>';
    return;
  }

  _renderAiTabs();
  var first = AI_RESULTS[0];
  AI_ACTIVE_FILE = first.name;
  if(codeOut) codeOut.textContent = first.header + first.code;
  if(codeTitle) codeTitle.textContent = first.name + '.py (AI)';
  if(pyBadge) pyBadge.style.display = '';
  if(btnCopy) btnCopy.disabled = false;
  var btnDL = document.getElementById('btnDL');
  var btnDLAll = document.getElementById('btnDLAll');
  if(btnDL) btnDL.disabled = false;
  if(btnDLAll) btnDLAll.disabled = false;

  document.querySelectorAll('.nb-tab').forEach(function(t){ t.classList.remove('active'); });
  var firstTab = document.getElementById('nbt_ai_' + first.name.replace(/[^a-zA-Z0-9_]/g,'_'));
  if(firstTab) firstTab.classList.add('active');

  var msg = AI_RESULTS.length + '/' + sel.length + ' object' + (sel.length>1?'s':'') + ' converted via AI';
  if(errors.length) msg += ' (' + errors.length + ' failed)';
  if(typeof toast === 'function') toast(msg, errors.length ? 'twrn' : 'tok', 4000);
  if(errors.length && typeof console !== 'undefined') console.warn('AI Convert errors:', errors);
};

/* ── Override dlCode & dlAllFiles to support AI results ─────────────── */
(function(){
  var _origDlCode = window.dlCode;
  window.dlCode = function(){
    if(AI_ACTIVE_FILE && AI_RESULTS.length > 0){
      var c = document.getElementById('codeOut');
      var text = c ? (c.textContent || c.innerText) : '';
      if(!text || text.trim().length < 5){ if(typeof toast==='function') toast('Nothing to download.','tinfo'); return; }
      var fn = AI_ACTIVE_FILE + '.py';
      var a = document.createElement('a');
      a.href = URL.createObjectURL(new Blob([text],{type:'text/plain'}));
      a.download = fn;
      a.click();
      if(typeof toast==='function') toast('Downloaded ' + fn,'tok',2000);
      return;
    }
    if(typeof _origDlCode === 'function') _origDlCode();
  };

  var _origDlAll = window.dlAllFiles;
  window.dlAllFiles = function(){
    if(AI_RESULTS.length > 0){
      /* Bundle all AI-converted files into a single ZIP */
      if(typeof JSZip === 'undefined'){
        if(typeof toast==='function') toast('JSZip not loaded. Falling back to individual downloads.','twarn',3000);
        AI_RESULTS.forEach(function(r, i){
          setTimeout(function(){
            var content = r.header + r.code;
            var a = document.createElement('a');
            a.href = URL.createObjectURL(new Blob([content],{type:'text/plain'}));
            a.download = r.name + '.py';
            a.click();
          }, i * 350);
        });
        return;
      }
      var zip = new JSZip();
      AI_RESULTS.forEach(function(r){
        var content = r.header + r.code;
        zip.file(r.name + '.py', content);
      });
      zip.generateAsync({type:'blob'}).then(function(blob){
        var a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'pyspark_converted_' + new Date().toISOString().slice(0,10) + '.zip';
        a.click();
        if(typeof toast==='function') toast('Downloaded ZIP with ' + AI_RESULTS.length + ' files','tok',2500);
      }).catch(function(err){
        console.error('ZIP generation error:', err);
        if(typeof toast==='function') toast('ZIP failed: ' + err.message,'terr',3000);
      });
      return;
    }
    if(typeof _origDlAll === 'function') _origDlAll();
  };
})();
