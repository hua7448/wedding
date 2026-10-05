/* ============================================================
   婚礼抽奖 · 数据层（双模式）
   ------------------------------------------------------------
   服务器模式（推荐）：页面由 server.py 托管时自动启用
     - 数据集中在服务器 SQLite（data.db + data_backup.json）
     - 任意手机/电脑登记、抽奖、兑奖实时同步
     - 大屏通过 SSE（/api/events）实时接收指令，跨设备零延迟
   本地模式（兜底）：直接双击 html / 静态托管时启用
     - localStorage 存储 + BroadcastChannel 跨标签页同步
     - 大屏与控制台需在同一浏览器不同标签页打开
   两模式对页面暴露完全相同的异步 API，页面无需关心当前模式。
   ============================================================ */
window.WL = (function () {
  const K = {
    guests: 'wl_guests', winners: 'wl_winners', config: 'wl_config',
    meta: 'wl_meta', msg: 'wl_msg', fp: 'wl_fp', fpRand: 'wl_fp_rand',
    pass: 'wl_pass',   // sessionStorage：管理密码
  };

  /* ---------------- 基础读写（本地模式用） ---------------- */
  function read(key, fallback) {
    try { const v = localStorage.getItem(key); return v ? JSON.parse(v) : fallback; }
    catch (e) { return fallback; }
  }
  function write(key, val) { localStorage.setItem(key, JSON.stringify(val)); }

  /* ---------------- 模式探测 ---------------- */
  let _mode = 'local';
  const ready = (async function detect() {
    try {
      const ctl = new AbortController();
      const to = setTimeout(() => ctl.abort(), 2500);
      const r = await fetch('api/ping', { cache: 'no-store', signal: ctl.signal });
      clearTimeout(to);
      if (r.ok) { const j = await r.json(); if (j && j.ok) return (_mode = 'server'); }
    } catch (e) { /* 静态托管 → 本地模式 */ }
    return (_mode = 'local');
  })();

  /* ---------------- 管理密码 ---------------- */
  function setPass(p) { try { sessionStorage.setItem(K.pass, p); } catch (e) {} }
  function getPass() { try { return sessionStorage.getItem(K.pass) || ''; } catch (e) { return ''; } }

  async function api(path, opts) {
    opts = opts || {};
    opts.headers = Object.assign({ 'Content-Type': 'application/json' }, opts.headers || {});
    if (getPass()) opts.headers['X-Admin-Pass'] = getPass();
    const r = await fetch('api/' + path, opts);
    let j = null;
    try { j = await r.json(); } catch (e) {}
    if (!r.ok) {
      const err = new Error((j && j.error) || ('请求失败 ' + r.status));
      err.payload = j; err.status = r.status;
      throw err;
    }
    return j;
  }

  /* ---------------- 消息订阅（大屏/控制台实时联动） ---------------- */
  const bus = ('BroadcastChannel' in window) ? new BroadcastChannel('wl_bus') : null;
  const handlers = [];
  let lastMsgId = null;
  function onMessage(cb) { handlers.push(cb); }
  function dispatch(msg) {
    if (!msg) return;
    if (msg._id) { if (msg._id === lastMsgId) return; lastMsgId = msg._id; }
    handlers.forEach(cb => { try { cb(msg); } catch (e) { console.error(e); } });
  }
  function sendLocal(msg) {   // 本地模式广播
    msg._id = Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    if (bus) { try { bus.postMessage(msg); } catch (e) {} }
    try { localStorage.setItem(K.msg, JSON.stringify(msg)); } catch (e) {}
    dispatch(msg);
  }
  if (bus) bus.onmessage = (ev) => dispatch(ev.data);
  window.addEventListener('storage', (ev) => {
    if (ev.key === K.msg && ev.newValue) { try { dispatch(JSON.parse(ev.newValue)); } catch (e) {} }
    if (ev.key === K.guests || ev.key === K.winners || ev.key === K.config) {
      handlers.forEach(cb => { try { cb({ t: 'data', key: ev.key }); } catch (e) {} });
    }
  });
  ready.then(m => {          // 服务器模式：订阅 SSE 事件流（自动重连）
    if (m !== 'server') return;
    const es = new EventSource('api/events');
    es.onmessage = (ev) => { try { dispatch(JSON.parse(ev.data)); } catch (e) {} };
  });

  /* ---------------- 设备指纹（含随机分量，杜绝同型号撞车） ---------------- */
  function fnv1a(str) {
    let h = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0;
    }
    return ('0000000' + h.toString(16)).slice(-8);
  }
  function fingerprint() {
    let fp = localStorage.getItem(K.fp);
    if (fp) return fp;
    let rand = localStorage.getItem(K.fpRand);
    if (!rand) {
      rand = (crypto.randomUUID ? crypto.randomUUID() :
        Date.now().toString(36) + Math.random().toString(36).slice(2));
      localStorage.setItem(K.fpRand, rand);
    }
    let canvasSig = '';
    try {
      const c = document.createElement('canvas');
      c.width = 200; c.height = 40;
      const ctx = c.getContext('2d');
      ctx.textBaseline = 'top'; ctx.font = '14px Arial';
      ctx.fillStyle = '#B98E4E'; ctx.fillRect(2, 2, 60, 20);
      ctx.fillStyle = '#3B2E22'; ctx.fillText('囍Huang&Nie♥088', 4, 6);
      canvasSig = c.toDataURL().slice(-128);
    } catch (e) {}
    const raw = [
      navigator.userAgent, navigator.language,
      screen.width + 'x' + screen.height + 'x' + screen.colorDepth,
      new Date().getTimezoneOffset(), navigator.hardwareConcurrency || '',
      navigator.platform || '', canvasSig, rand,
    ].join('|');
    fp = fnv1a(raw) + fnv1a(raw + ':salt');
    localStorage.setItem(K.fp, fp);
    return fp;
  }

  /* ---------------- 配置 ---------------- */
  const DEFAULT_CONFIG = {
    groom: '黄继安', bride: '聂玮婷',
    groomEn: 'Huang Ji’an', brideEn: 'Nie Weiting',
    date: '2026.10.25', venue: '', title: '婚礼幸运抽奖',
    adminPass: 'hn2026',
    prizes: [
      { id: 'p_san', name: '三等奖', count: 4, perRound: 2, desc: '' },
      { id: 'p_er',  name: '二等奖', count: 3, perRound: 2, desc: '' },
      { id: 'p_yi',  name: '一等奖', count: 2, perRound: 1, desc: '' },
      { id: 'p_te',  name: '特等奖', count: 1, perRound: 1, desc: '' },
    ],
  };
  function localConfig() {
    const c = read(K.config, null);
    if (!c) { write(K.config, DEFAULT_CONFIG); return JSON.parse(JSON.stringify(DEFAULT_CONFIG)); }
    return Object.assign(JSON.parse(JSON.stringify(DEFAULT_CONFIG)), c);
  }
  async function getConfig() {
    await ready;
    if (_mode === 'server') { try { return await api('config'); } catch (e) { /* fallthrough */ } }
    return localConfig();
  }
  async function saveConfig(cfg) {
    await ready;
    if (_mode === 'server') { await api('config', { method: 'POST', body: JSON.stringify(cfg) }); return; }
    write(K.config, cfg); sendLocal({ t: 'data', key: K.config });
  }
  async function checkPass(pass) {
    await ready;
    if (_mode === 'server') {
      try { await api('auth', { method: 'POST', body: JSON.stringify({ password: pass }) }); return true; }
      catch (e) { return false; }
    }
    return pass === localConfig().adminPass;
  }

  /* ---------------- 宾客 / 号码 ---------------- */
  const CODE_CHARS = '23456789ABCDEFGHJKMNPQRSTUVWXYZ';
  function randCode(n) {
    const buf = new Uint8Array(n); crypto.getRandomValues(buf);
    let s = ''; for (let i = 0; i < n; i++) s += CODE_CHARS[buf[i] % CODE_CHARS.length];
    return s;
  }
  function pad3(n) { return ('00' + n).slice(-3); }

  async function lookup(fp) {
    await ready;
    if (_mode === 'server') {
      const j = await api('lookup', { method: 'POST', body: JSON.stringify({ fp }) });
      return j.guest || null;
    }
    return read(K.guests, []).find(g => g.fp === fp) || null;
  }

  async function register(name, phone, wish) {
    name = (name || '').trim(); phone = (phone || '').trim(); wish = (wish || '').trim();
    if (!name) return { error: '请填写您的姓名' };
    if (name.length > 12) return { error: '姓名最多 12 个字' };
    if (!/^1\d{10}$/.test(phone)) return { error: '请填写 11 位手机号' };
    const fp = fingerprint();
    await ready;
    if (_mode === 'server') {
      try {
        return await api('register', { method: 'POST', body: JSON.stringify({ name, phone, wish, fp }) });
      } catch (e) { return { error: e.message || '登记失败，请稍后再试' }; }
    }
    // ---- 本地模式 ----
    const guests = read(K.guests, []);
    const byFp = guests.find(g => g.fp === fp);
    if (byFp) return { guest: byFp, existed: true };
    const byPhone = guests.find(g => g.phone === phone);
    if (byPhone) return { error: '该手机号已领取过号码券（No.' + byPhone.num + '）' };
    const meta = read(K.meta, { next: 1 });
    const guest = { num: pad3(meta.next), name, phone, wish, fp, code: randCode(4), ts: Date.now() };
    meta.next += 1; write(K.meta, meta);
    guests.push(guest); write(K.guests, guests);
    sendLocal({ t: 'data', key: K.guests });
    return { guest, existed: false };
  }

  /* ---------------- 控制台快照 ---------------- */
  function localState() {
    const winners = read(K.winners, []);
    const won = new Set(winners.filter(w => w.status !== 'void').map(w => w.num));
    const guests = read(K.guests, []);
    const poolArr = guests.filter(g => !won.has(g.num));
    const drawn = {};
    winners.forEach(w => { if (w.status !== 'void') drawn[w.prizeId] = (drawn[w.prizeId] || 0) + 1; });
    return { guests: guests.length, pool: poolArr.length,
             poolNames: poolArr.map(g => g.name),
             guestList: guests.map(g => ({ num: g.num, name: g.name, phone: g.phone, wish: g.wish || '', ts: g.ts })),
             winners, prizeDrawn: drawn, config: localConfig() };
  }
  async function state() {
    await ready;
    if (_mode === 'server') return await api('state');
    return localState();
  }

  /* ---------------- 抽奖 ---------------- */
  /** 广播大屏指令（roll / idle），服务端会原样推给所有大屏 */
  async function command(msg) {
    await ready;
    if (_mode === 'server') { await api('command', { method: 'POST', body: JSON.stringify(msg) }); return; }
    sendLocal(msg);
  }

  /** 定格抽奖：服务端原子抽取并写入；本地模式等效实现 */
  async function draw(prizeId, count, roundLabel) {
    await ready;
    if (_mode === 'server') {
      try {
        return await api('draw', { method: 'POST',
          body: JSON.stringify({ prizeId, count, round: roundLabel }) });
      } catch (e) { return { error: e.message || '抽奖失败' }; }
    }
    // ---- 本地模式 ----
    const cfg = localConfig();
    const prize = cfg.prizes.find(p => p.id === prizeId);
    if (!prize) return { error: '奖项不存在' };
    const st = localState();
    const wonNums = new Set(st.winners.filter(w => w.status !== 'void').map(w => w.num));
    const poolArr = read(K.guests, []).filter(g => !wonNums.has(g.num));
    if (poolArr.length < count) return { error: '号码池人数不足（当前 ' + poolArr.length + ' 人）' };
    const picks = [], used = new Set();
    while (picks.length < count) {
      const buf = new Uint32Array(1); crypto.getRandomValues(buf);
      const i = buf[0] % poolArr.length;
      if (used.has(i)) continue; used.add(i); picks.push(poolArr[i]);
    }
    const winners = read(K.winners, []);
    const now = Date.now(); const ws = [];
    picks.forEach((g, i) => {
      const w = { id: 'w' + now + '_' + i + '_' + randCode(3),
        prizeId: prize.id, prizeName: prize.name, prizeDesc: prize.desc || '',
        round: roundLabel, num: g.num, name: g.name, phone: g.phone, code: g.code,
        ts: now + i, status: 'win' };
      winners.push(w); ws.push(w);
    });
    write(K.winners, winners);
    sendLocal({ t: 'stop', prizeId: prize.id, prizeName: prize.name, prizeDesc: prize.desc || '',
      round: roundLabel, count, winners: ws.map(w => ({ num: w.num, name: w.name })) });
    return { winners: ws };
  }

  async function setWinnerStatus(id, status) {
    await ready;
    if (_mode === 'server') {
      await api('winner-status', { method: 'POST', body: JSON.stringify({ id, status }) });
      return;
    }
    const winners = read(K.winners, []);
    const w = winners.find(x => x.id === id);
    if (w) { w.status = status; write(K.winners, winners); sendLocal({ t: 'data', key: K.winners }); }
  }

  /* ---------------- 重置 / 导出 ---------------- */
  async function resetAll() {
    await ready;
    if (_mode === 'server') { await api('reset', { method: 'POST', body: '{}' }); return; }
    localStorage.removeItem(K.guests); localStorage.removeItem(K.winners); localStorage.removeItem(K.meta);
    sendLocal({ t: 'data', key: 'reset' });
  }
  /** 服务器模式：直接给导出链接（浏览器下载）；本地模式：返回 null 走 downloadCSV */
  function exportUrl(kind) {   // kind: 'winners' | 'guests'
    if (_mode !== 'server') return null;
    return 'api/export/' + kind + '.csv?pass=' + encodeURIComponent(getPass());
  }
  function downloadCSV(filename, rows) {
    const csv = '﻿' + rows.map(r => r.map(c => '"' + String(c == null ? '' : c).replace(/"/g, '""') + '"').join(',')).join('\r\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob); a.download = filename;
    document.body.appendChild(a); a.click();
    setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 500);
  }

  return {
    ready, mode: () => _mode,
    setPass, getPass, checkPass,
    onMessage, fingerprint,
    getConfig, saveConfig,
    lookup, register,
    state, command, draw, setWinnerStatus,
    resetAll, exportUrl, downloadCSV, pad3,
  };
})();
