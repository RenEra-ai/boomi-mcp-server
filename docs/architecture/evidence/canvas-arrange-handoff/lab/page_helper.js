// Installed into the signed-in Boomi Build page with chrome-devtools-mcp evaluate_script
// (pass "() => { <this file> }"). Opens each lab process, clicks Arrange, answers
// "Don't Save" to the unsaved-changes prompt, waits for "Calculating step positions",
// then Save & Close. Runs in the background; poll window.afl2.status().
// Set window.aflAccountId = '<Boomi account id>' before queueing.
const vis = e => e && e.offsetParent !== null;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const byText = (sel, txt) => [...document.querySelectorAll(sel)].filter(e => vis(e) && e.textContent.trim() === txt);
async function waitFor(fn, ms, step = 500) { const t = Date.now(); while (Date.now() - t < ms) { const v = fn(); if (v) return v; await sleep(step); } return null; }
const leafText = re => [...document.querySelectorAll('*')].some(e => vis(e) && e.children.length === 0 && re.test(e.textContent));
const S = window.aflState = window.aflState || {};
const errs = window.aflErrors = window.aflErrors || [];
// GWT runs in its own iframe and logs SEVERE errors through that frame's console
for (const w of [window, ...[...document.querySelectorAll('iframe')].map(f => { try { return f.contentWindow; } catch (e) { return null; } })]) {
  try {
    if (!w || !w.console || w.console.error.__afl) continue;
    const orig = w.console.error;
    w.console.error = function (...a) { errs.push({ t: Date.now(), m: a.map(String).join(' ').slice(0, 300) }); return orig.apply(this, a); };
    w.console.error.__afl = true;
  } catch (e) { /* cross-origin frame */ }
}
async function run(id, name, expected) {
  const st = S[name] = { phase: 'open', t0: Date.now(), log: [] };
  try {
    if (![...document.querySelectorAll('.gwt-TabLayoutPanelTab-selected')].some(e => e.textContent.trim() === name))
      location.hash = `build;accountId=${window.aflAccountId};components=${id}`;
    const title = await waitFor(() => {
      if (leafText(/navigate away from the build screen/)) { const y = byText('button', 'Yes')[0]; if (y) { st.log.push('navYes'); y.click(); } }
      return [...document.querySelectorAll('.gwt-TabLayoutPanelTab-selected')].find(e => e.textContent.trim() === name);
    }, 90000);
    if (!title) throw 'tab not open';
    const arrange = await waitFor(() => [...document.querySelectorAll('a[data-locator="link-arrange"]')].find(vis), 90000);
    if (!arrange) throw 'no arrange';
    // Arrange walks the rendered shape widgets: wait until the whole canvas is built
    const shown = () => [...document.querySelectorAll('.shapeLabel')].filter(vis).length;
    const full = await waitFor(() => shown() >= expected, 180000, 1000);
    if (!full) throw `only ${shown()} of ${expected} shapes rendered`;
    await sleep(5000);
    const tClick = Date.now();
    st.phase = 'arrange'; arrange.click();
    const prompt = await waitFor(() => byText('button', "Don't Save")[0], 8000);
    if (prompt) { st.log.push('dontSave'); prompt.click(); }
    st.phase = 'calculating';
    await sleep(1000);
    const done = await waitFor(() => {
      const e = errs.find(x => x.t > tClick && /SEVERE/.test(x.m));
      if (e) throw 'arrange crashed: ' + e.m.slice(0, 160);
      return !leafText(/Calculating step positions/);
    }, 600000, 1000);
    if (!done) throw 'calc timeout';
    st.calcMs = Date.now() - tClick;
    await sleep(1500);
    const save = byText('button', 'Save & Close')[0];
    if (!save) throw 'no save&close';
    st.phase = 'saving'; save.click();
    const ok = await waitFor(() => leafText(new RegExp(name.replace(/[()]/g, '.') + ' saved successfully')), 120000);
    if (!ok) throw 'no save confirmation';
    await waitFor(() => ![...document.querySelectorAll('.gwt-TabLayoutPanelTab')].some(e => e.textContent.trim() === name), 20000);
    st.phase = 'done';
  } catch (e) { st.phase = 'error'; st.error = String(e); }
  st.ms = Date.now() - st.t0;
}
async function queue(list, stopOnError = false) {
  if (!window.aflAccountId) throw 'set window.aflAccountId first';
  S.__queue = 'running';
  for (const [id, n, cnt] of list) { await run(id, n, cnt); if (stopOnError && S[n].phase !== 'done') break; if (S[n].phase === 'error') break; }
  S.__queue = 'finished';
}
window.afl2 = { run, queue, status: () => Object.fromEntries(Object.entries(S).map(([k, v]) => [k, typeof v === 'object' ? [v.phase, v.error || '', v.log.join(','), v.calcMs, v.ms] : v])) };
return 'ok';
