/* ════════════════════════════════════════════════════════════════════
   PRE-FLIGHT READINESS CHECK — first-run gate (Databricks-themed)
   • First load (or first time in >24h, or last run had issues): a
     full-screen, opaque takeover blocks the dashboard entirely — no
     overlap/bleed-through — until every check has been evaluated.
   • If everything passes cleanly, the gate auto-dismisses and the
     dashboard fades in. From then on, refreshes within 24h skip the
     gate completely (dashboard loads normally, instantly).
   • Re-runnable anytime from Settings → Readiness Check, which opens
     a lighter "modal" variant on top of the already-visible dashboard.
   ════════════════════════════════════════════════════════════════════ */
(function(){
const PF_KEY = 'ms_preflight_v1';

function pfState(){ try{return JSON.parse(localStorage.getItem(PF_KEY)||'{}')}catch(e){return {}} }
function pfSave(s){ try{localStorage.setItem(PF_KEY, JSON.stringify(s))}catch(e){} }

/* The <head> guard script (index.html) already decided — before first
   paint — whether the dashboard should stay hidden, and stamped that
   decision onto <html data-pf-pending="1">. We just read it back. */
function pfIsPending(){ return document.documentElement.getAttribute('data-pf-pending') === '1'; }

/* Reveal the dashboard (no-op if it was already visible). */
function pfReleaseGate(){ document.documentElement.removeAttribute('data-pf-pending'); }

/* ── Styles (injected once) ── */
const CSS = `
#pfOverlay{position:fixed;inset:0;z-index:9999;display:none;align-items:center;justify-content:center;padding:24px}
#pfOverlay.show{display:flex}

/* Modal mode — manual re-run from Settings: dashboard stays visible, dimmed behind a blur */
#pfOverlay.pf-modal{background:rgba(11,32,38,.55);backdrop-filter:blur(3px);animation:pfFade .25s ease}

/* Gate mode — first load: solid Databricks-navy full-bleed takeover, nothing bleeds through */
#pfOverlay.pf-gate{
  background:
    radial-gradient(1100px 620px at 12% -8%, rgba(255,54,33,.20), transparent 60%),
    radial-gradient(900px 700px at 108% 108%, rgba(255,54,33,.14), transparent 55%),
    linear-gradient(160deg,#0B1720 0%,#0B2026 45%,#111B33 100%);
}
@keyframes pfFade{from{opacity:0}to{opacity:1}}
#pfModal{width:min(700px,94vw);max-height:88vh;background:var(--surface,#fff);border-radius:18px;box-shadow:0 24px 70px rgba(11,32,38,.45);display:flex;flex-direction:column;overflow:hidden;animation:pfPop .35s cubic-bezier(.2,.9,.3,1.2)}
@keyframes pfPop{from{opacity:0;transform:translateY(18px) scale(.97)}to{opacity:1;transform:none}}

#pfHead{background:linear-gradient(120deg,#0B2026 0%,#152B3B 55%,#1E2A52 100%);color:#fff;padding:22px 26px;display:flex;align-items:center;gap:14px;position:relative;overflow:hidden}
#pfHead::after{content:'';position:absolute;inset:0;background:linear-gradient(90deg,#FF3621,#FF7355 45%,transparent 90%);opacity:.10;pointer-events:none}
#pfHead .pf-shield{width:46px;height:46px;border-radius:12px;background:linear-gradient(135deg,#FF3621,#FF7355);display:flex;align-items:center;justify-content:center;flex-shrink:0;box-shadow:0 4px 14px rgba(255,54,33,.35)}
#pfHead h2{font-size:17px;font-weight:800;letter-spacing:-.01em}
#pfHead p{font-size:11.5px;color:#B8C7CC;margin-top:2px}
#pfBadge{margin-left:auto;font-size:11px;font-weight:800;padding:5px 13px;border-radius:99px;background:rgba(255,255,255,.14);white-space:nowrap}

/* Overall progress bar */
#pfProgWrap{height:4px;background:rgba(255,255,255,.14);position:relative;flex-shrink:0}
#pfProg{height:100%;width:0%;background:linear-gradient(90deg,#FF3621,#FF7355);transition:width .35s ease}

#pfBody{overflow-y:auto;padding:14px 22px;flex:1;background:var(--surface,#fff)}
.pf-cat-hd{font-size:9.5px;font-weight:800;letter-spacing:.1em;color:var(--t4,#94A3B8);text-transform:uppercase;padding:12px 6px 4px;}
.pf-cat-hd:first-child{padding-top:4px}
.pf-check{display:flex;gap:13px;padding:12px 6px;border-bottom:1px solid var(--surface-3,#F1F5F9);align-items:flex-start;opacity:.4;transform:translateY(4px);transition:opacity .35s ease,transform .35s ease}
.pf-check.live{opacity:1;transform:none}
.pf-ic{width:34px;height:34px;border-radius:10px;display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:15px;font-weight:800}
.pf-ic.wait{background:var(--surface-3,#F1F5F9);color:var(--t4,#94A3B8)}
.pf-ic.pass{background:#ECFDF5;color:#059669}
.pf-ic.warn{background:#FFFBEB;color:#D97706}
.pf-ic.fail{background:#FFF1EE;color:#FF3621}
.pf-ic .spin{width:16px;height:16px;border:2.5px solid #E2E8F0;border-top-color:#FF3621;border-radius:50%;animation:pfSpin .8s linear infinite}
@keyframes pfSpin{to{transform:rotate(360deg)}}
.pf-name{font-size:13px;font-weight:700;color:var(--t1,#0F172A)}
.pf-detail{font-size:11.5px;color:var(--t3,#64748B);margin-top:3px;line-height:1.5}
.pf-hint{font-size:11px;color:#92400E;background:var(--amber-light,#FFFBEB);border-radius:8px;padding:6px 10px;margin-top:6px;display:inline-block}
.pf-ms{margin-left:auto;font-size:10px;color:var(--t4,#94A3B8);font-weight:600;flex-shrink:0;padding-top:3px}
#pfFoot{padding:14px 22px;border-top:1px solid var(--border,#E2E8F0);display:flex;align-items:center;gap:10px;background:var(--surface-2,#F8FAFC)}
#pfSummary{font-size:12px;color:var(--t3,#64748B);font-weight:600;flex:1}
.pf-btn{border:none;cursor:pointer;font-family:inherit;font-weight:700;font-size:12.5px;border-radius:9px;padding:9px 16px;transition:.15s}
.pf-btn-primary{background:linear-gradient(135deg,#FF3621,#FF7355);color:#fff}
.pf-btn-primary:hover{filter:brightness(1.08);box-shadow:0 4px 14px rgba(255,54,33,.3)}
.pf-btn-ghost{background:#fff;color:var(--t3,#64748B);border:1px solid var(--border,#E2E8F0)}
.pf-btn-ghost:hover{border-color:#FF3621;color:#FF3621}

/* Dashboard reveal transition (paired with the <head> guard in index.html) */
#app{transition:opacity .4s ease}
html[data-pf-pending="1"] #app{opacity:0;visibility:hidden}
`;
if(!document.getElementById('pfStyles')){
  const st=document.createElement('style');st.id='pfStyles';st.textContent=CSS;document.head.appendChild(st);
}

const ICONS = {
  pass:'<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><path d="M20 6L9 17l-5-5"/></svg>',
  warn:'<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6"><path d="M12 9v4M12 17h.01"/><circle cx="12" cy="12" r="9"/></svg>',
  fail:'<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><path d="M18 6L6 18M6 6l12 12"/></svg>'
};

function pfBuildSkeleton(checks){
  const body=document.getElementById('pfBody');
  let lastCat=null, html='';
  checks.forEach((c,i)=>{
    const cat=(c.category||'').toUpperCase();
    if(cat!==lastCat){ html+=`<div class="pf-cat-hd">${cat}</div>`; lastCat=cat; }
    html+=`
    <div class="pf-check" id="pfChk${i}">
      <div class="pf-ic wait" id="pfIc${i}"><div class="spin"></div></div>
      <div style="flex:1;min-width:0">
        <div class="pf-name">${c.name}</div>
        <div class="pf-detail" id="pfDt${i}">Checking…</div>
        <div id="pfHint${i}"></div>
      </div>
      <div class="pf-ms" id="pfMs${i}"></div>
    </div>`;
  });
  body.innerHTML = html;
}

function pfFillRow(i,r,total){
  const ic=document.getElementById('pfIc'+i), dt=document.getElementById('pfDt'+i),
        hint=document.getElementById('pfHint'+i), ms=document.getElementById('pfMs'+i);
  ic.className='pf-ic '+r.status; ic.innerHTML=ICONS[r.status]||ICONS.pass;
  dt.textContent=r.detail||'';
  hint.innerHTML=r.hint?`<span class="pf-hint">💡 ${r.hint}</span>`:'';
  ms.textContent=r.ms!=null?(r.ms/1000).toFixed(1)+'s':'';
  document.getElementById('pfChk'+i).classList.add('live');
  const prog=document.getElementById('pfProg');
  if(prog) prog.style.width = Math.round(((i+1)/total)*100)+'%';
  const badge=document.getElementById('pfBadge');
  if(badge) badge.textContent = `Checking ${i+1} of ${total}…`;
}

function pfSetBadge(summary){
  const b=document.getElementById('pfBadge');
  if(summary.fail>0){ b.style.background='rgba(255,54,33,.28)'; b.textContent='✕ '+summary.fail+' failed'; }
  else if(summary.warn>0){ b.style.background='rgba(245,158,11,.28)'; b.textContent='⚠ '+summary.warn+' warning'+(summary.warn>1?'s':''); }
  else { b.style.background='rgba(16,185,129,.3)'; b.textContent='✓ All checks passed'; }
  document.getElementById('pfSummary').innerHTML =
    `<b style="color:var(--green,#059669)">${summary.pass} passed</b> · `+
    `<b style="color:var(--amber,#D97706)">${summary.warn} warning${summary.warn===1?'':'s'}</b> · `+
    `<b style="color:#FF3621">${summary.fail} failed</b> · completed in ${(summary.duration_ms/1000).toFixed(1)}s`;
}

let pfRunning=false;
async function pfRun(isGate){
  if(pfRunning) return; pfRunning=true;
  openPfOverlay(isGate);
  document.getElementById('pfBody').innerHTML='<div style="text-align:center;padding:34px;color:var(--t4);font-size:12.5px;">Starting environment checks…</div>';
  document.getElementById('pfBadge').textContent='Starting…';
  document.getElementById('pfSummary').textContent='Validating access, components and dependencies…';
  document.getElementById('pfEnter').style.display='none';
  const prog=document.getElementById('pfProg'); if(prog) prog.style.width='0%';
  try{
    const r=await fetch('/api/v1/preflight/run');
    const d=await r.json();
    if(!d.success){
      document.getElementById('pfBody').innerHTML='<div style="padding:26px;color:#FF3621;font-size:12.5px;">'+(d.error||'Pre-flight failed')+'</div>';
      pfRunning=false;
      const s=pfState(); s.ts=Date.now(); s.result='issues'; pfSave(s);
      return;
    }
    pfBuildSkeleton(d.checks);
    for(let i=0;i<d.checks.length;i++){
      await new Promise(res=>setTimeout(res,220));           // progressive reveal
      pfFillRow(i,d.checks[i],d.checks.length);
    }
    pfSetBadge(d.summary);
    const allClear = d.summary.ready && d.summary.warn===0 && d.summary.fail===0;
    const s=pfState(); s.ts=Date.now(); s.result = allClear ? 'pass' : 'issues'; delete s.skipped; pfSave(s);
    const enter=document.getElementById('pfEnter');
    enter.style.display='';
    if(allClear){
      enter.textContent='Start Using App'; enter.className='pf-btn pf-btn-primary';
      setTimeout(()=>{ if(document.getElementById('pfOverlay').classList.contains('show')) closePfOverlay(); },2000);
    } else if(d.summary.ready){
      enter.textContent='Continue Anyway'; enter.className='pf-btn pf-btn-primary';
    } else {
      enter.textContent='Continue Anyway (not ready)'; enter.className='pf-btn pf-btn-ghost';
    }
  }catch(e){
    document.getElementById('pfBody').innerHTML='<div style="padding:26px;color:#FF3621;font-size:12.5px;">Pre-flight error: '+e.message+'</div>';
    const s=pfState(); s.ts=Date.now(); s.result='issues'; pfSave(s);
  }
  pfRunning=false;
}

function openPfOverlay(isGate){
  const ov=document.getElementById('pfOverlay');
  ov.classList.toggle('pf-gate', !!isGate);
  ov.classList.toggle('pf-modal', !isGate);
  ov.classList.add('show');
}
function closePfOverlay(){
  document.getElementById('pfOverlay').classList.remove('show');
  pfReleaseGate();
}
function pfSkip(){
  const s=pfState(); s.ts=Date.now(); s.skipped=Date.now(); pfSave(s);
  closePfOverlay();
  if(typeof showToast==='function') showToast('Readiness check skipped — run it anytime from Settings','info');
}

/* ── Build overlay DOM once ── */
function pfEnsureDom(){
  if(document.getElementById('pfOverlay')) return;
  const ov=document.createElement('div'); ov.id='pfOverlay';
  ov.innerHTML=`
    <div id="pfModal" role="dialog" aria-modal="true">
      <div id="pfHead">
        <div class="pf-shield">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="M9 12l2 2 4-4"/></svg>
        </div>
        <div>
          <h2>Environment Readiness Check</h2>
          <p>Validating Databricks access, Azure components &amp; dependencies before you start</p>
        </div>
        <span id="pfBadge">Running…</span>
      </div>
      <div id="pfProgWrap"><div id="pfProg"></div></div>
      <div id="pfBody"></div>
      <div id="pfFoot">
        <span id="pfSummary">Validating…</span>
        <button class="pf-btn pf-btn-ghost" onclick="pfRun(document.getElementById('pfOverlay').classList.contains('pf-gate'))">↻ Re-run</button>
        <button class="pf-btn pf-btn-ghost" onclick="pfSkip()">Skip for now</button>
        <button class="pf-btn pf-btn-primary" id="pfEnter" style="display:none" onclick="closePfOverlay()">Continue</button>
      </div>
    </div>`;
  document.body.appendChild(ov);
  ov.addEventListener('click',e=>{ if(e.target===ov){ /* click outside = no accidental skip */ } });
}

/* ── Public API ── */
window.openPreflight = function(){ pfEnsureDom(); pfRun(false); };   // manual re-run from Settings — modal mode
window.closePfOverlay = closePfOverlay;
window.pfRun = pfRun;
window.pfSkip = pfSkip;

/* ── Auto-run as a blocking gate whenever the <head> guard left the
   dashboard hidden (first load, stale >24h, or last run had issues) ── */
if(typeof document!=='undefined'){
  const boot=()=>{
    if(pfIsPending()){ pfEnsureDom(); pfRun(true); }
    else { pfReleaseGate(); }   // defensive — nothing to release, but keeps state consistent
  };
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',boot);
  else boot();
}
})();
