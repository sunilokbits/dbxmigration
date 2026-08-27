(function(){
  var diag = document.getElementById('jsDiag');
  var fns = ['testSourceConn','loadFromSource','convertSelected','switchTab','toast','toggleSrcConn','previewSource'];
  var missing = fns.filter(function(f){ return typeof window[f] !== 'function'; });

  if(missing.length > 0){
    /* Main script block FAILED to parse/execute */
    if(diag){
      diag.style.background = '#fee2e2';
      diag.style.color = '#b91c1c';
      diag.style.border = '2px solid #f87171';
      diag.textContent = 'SCRIPT PARSE ERROR — Missing: ' + missing.join(', ');
    }
    /* Also add a big visible banner */
    var banner = document.createElement('div');
    banner.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:99999;background:#dc2626;color:#fff;padding:16px 24px;font:14px/1.5 sans-serif;text-align:center;box-shadow:0 4px 12px rgba(0,0,0,.3);';
    banner.innerHTML = '<b>JavaScript Error:</b> The main script failed to load. Functions missing: ' + missing.join(', ') + '<br>Please open this page in Chrome/Edge and check the console (F12) for the exact error.';
    document.body.appendChild(banner);
  } else {
    /* Main script loaded OK — wire up fallback event listeners */
    if(diag){
      diag.style.background = '#d1fae5';
      diag.style.color = '#065f46';
      diag.style.border = '1.5px solid #34d399';
      diag.textContent = 'JS OK — All ' + fns.length + ' core functions loaded';
      setTimeout(function(){ diag.style.opacity = '0'; diag.style.transition = 'opacity 1s'; setTimeout(function(){ diag.remove(); }, 1200); }, 4000);
    }

    /* Fallback: add event listeners in case inline onclick doesn't fire */
    var btnMap = {
      'btnTestSrc': function(){ testSourceConn(); },
      'btnLoadSrc': function(){ loadFromSource(); },
      'btnConvert': function(){ convertSelected(); }
    };
    Object.keys(btnMap).forEach(function(id){
      var el = document.getElementById(id);
      if(el){
        el.addEventListener('click', btnMap[id]);
      }
    });

    /* Also add a global click logger for debugging */
    document.addEventListener('click', function(e){
      var btn = e.target.closest('.btn');
      if(btn){
        console.log('[ClickDebug] Button clicked:', btn.id || btn.textContent.trim().substring(0,30), 'disabled:', btn.disabled, 'onclick:', btn.getAttribute('onclick'));
      }
    }, true);
  }
  window.__jsPhase = 'complete';
})();
