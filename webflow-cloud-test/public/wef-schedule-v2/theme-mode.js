(() => {
  const key = 'ringstatus-schedule-theme';
  let button,bound=false;
  const moon = '<path d="M20.9 13A9 9 0 0 1 11 3.1 9 9 0 1 0 20.9 13Z"/>';
  const sun = '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M2 12h2M20 12h2M5 5l1.4 1.4M17.6 17.6 19 19M5 19l1.4-1.4M17.6 6.4 19 5"/>';
  function apply(mode) {
    const light = mode === 'light';
    document.documentElement.dataset.theme = light ? 'light' : 'dark';
    const label = light ? 'Light mode; switch to Dark mode' : 'Dark mode; switch to Light mode';
    document.querySelector('meta[name="theme-color"]').content = light ? '#ffffff' : '#191919';
    if(!button)return;
    button.setAttribute('aria-label', label);
    button.title = label;
    button.setAttribute('aria-pressed', String(light));
    button.querySelector('svg').innerHTML = light ? sun : moon;
  }
  let mode = 'dark';
  try { mode = localStorage.getItem(key) || mode; } catch {}
  apply(mode);
  function bind(){
  if(bound)return;button=document.querySelector('.v2-theme-toggle');if(!button)return;bound=true;apply(mode);
  button.addEventListener('click', () => {
    const next = document.documentElement.dataset.theme === 'light' ? 'dark' : 'light';
    apply(next);
    try { localStorage.setItem(key, next); } catch {}
  });
  }
  document.addEventListener('rs-schedule-ready',bind,{once:true});
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',bind,{once:true});else bind();
})();
