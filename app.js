/* Видеочат 18+ — клиент на GitHub Pages (без сервера).
 * Сигнализация: публичный MQTT-брокер через WSS (HiveMQ / EMQX).
 * Медиа: P2P WebRTC (STUN). Комнаты: код в ссылке ?room=XXX.
 */
'use strict';
const $ = (s) => document.querySelector(s);

const el = {
  gate: $('#agegate'), app: $('#app'), home: $('#home'), call: $('#call'),
  agechk: $('#agechk'), agebtn: $('#agebtn'), stat: $('#stat'),
  createbtn: $('#createbtn'), roomlinkbox: $('#roomlink-box'), roomlink: $('#roomlink'), copybtn: $('#copybtn'),
  joinname: $('#joinname'), joinbtn: $('#joinbtn'),
  remote: $('#remote'), local: $('#local'), status: $('#callstatus'),
  micbtn: $('#micbtn'), cambtn: $('#cambtn'), sharebtn: $('#sharebtn'), leavebtn: $('#leavebtn'),
  chatbox: $('#chatbox'), chatform: $('#chatform'), chatinput: $('#chatinput'),
};

const BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt',
];
const TP = 'vc18'; // префикс топиков
const MAX_ROOM = 4;
const PEER_TTL = 75000;     // peer "умер", если не было presence 75с (у брокера бывают "чёрные дыры" ~30с)
const ALIVE_EVERY = 7000;  // шлём presence раз в 7с
const STUN = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
  { urls: 'stun:stun.cloudflare.com:3478' },
  { urls: 'stun:stun.nextcloud.com:443' },
];
const pcConfig = { iceServers: STUN, iceCandidatePoolSize: 4 };

const myId = Math.random().toString(36).slice(2, 10);
let client = null, brokerIdx = 0, reconnectTimer = null, aliveTimer = null, sweepTimer = null;
let currentBroker = null;
let room = null;
const peers = new Map(); // id -> {lastSeen, pc, cands:[]}
let activePeer = null, stream = null, joined = false;

// ---------- утилиты ----------
const rndCode = () => Math.random().toString(36).slice(2, 8).replace(/[^a-z0-9]/g, 'x');
const roomLink = (r) => location.origin + location.pathname + '?room=' + r;
const setStat = (s) => { el.stat.textContent = s || ''; };
const sysMsg = (t) => { addMsg('sys', t); el.status.textContent = t; el.status.style.display = ''; };
function addMsg(kind, text, who) {
  const d = document.createElement('div');
  d.className = 'msg ' + kind;
  if (kind === 'sys') d.textContent = text;
  else {
    if (who && who !== myId) { const b = document.createElement('b'); b.textContent = (who.slice(0, 6) + ': '); d.appendChild(b); }
    d.appendChild(document.createTextNode(text));
  }
  el.chatbox.appendChild(d);
  while (el.chatbox.children.length > 200) el.chatbox.firstChild.remove();
  el.chatbox.scrollTop = el.chatbox.scrollHeight;
}
function showCall(label) {
  el.home.classList.add('hidden');
  el.call.classList.remove('hidden');
  el.status.textContent = label || '';
  el.status.style.display = '';
  setTimeout(() => { if (el.status.textContent === (label || '')) el.status.style.display = 'none'; }, 4000);
}
function showHome() {
  el.call.classList.add('hidden');
  el.home.classList.remove('hidden');
}

// ---------- 18+ гейт ----------
function passGate() {
  try { localStorage.setItem('vc_age_ok', '1'); } catch {}
  document.cookie = 'age18=1; path=/; max-age=31536000; SameSite=Lax';
  el.gate.classList.add('hidden');
  el.app.classList.remove('hidden');
  autoJoinFromUrl();
}
if (localStorage.getItem('vc_age_ok') === '1') { passGate(); }
else {
  el.gate.classList.remove('hidden');
  el.agechk.addEventListener('change', () => { el.agebtn.disabled = !el.agechk.checked; });
  el.agebtn.addEventListener('click', () => { if (el.agechk.checked) passGate(); });
}

// ---------- MQTT ----------
function connectMqtt() {
  const uri = BROKERS[brokerIdx % BROKERS.length];
  currentBroker = uri;
  setStat('подключение… (' + uri.replace('wss://', '').split('/')[0] + ')');
  let c;
  try {
    c = mqtt.connect(uri, {
      clientId: myId + '-' + Math.random().toString(16).slice(2, 8),
      clean: true, keepalive: 20, reconnectPeriod: 0, connectTimeout: 12000,
    });
  } catch (e) { return failBroker(); }
  client = c;
  let connected = false;
  // если брокер не дал connect за 16с — считаем его мёртвым и идём дальше
  const watch = setTimeout(() => {
    if (!connected) { console.log('[mqtt] no connect from ' + uri); try { c.end(true); } catch {} failBroker(); }
  }, 16000);
  c.on('connect', () => {
    connected = true; clearTimeout(watch);
    if (!room) return;
    const t = TP + '/' + room;
    c.subscribe([t + '/p', t + '/s', t + '/c'], { qos: 1 }, (err) => { if (err) console.warn('sub', err); });
    pub(t + '/p', { id: myId, t: 'join', ts: Date.now() }, 1);
    joined = true;
    setStat('комната «' + room + '»');
        startAliveLoop();
        if (!sweepTimer) sweepTimer = setInterval(sweepPeers, 5000);
        maybeStartCall();
        // после переподключения брокера: если offer создан, но ответ не пришёл — повторяем offer
        if (activePeer) {
          const r2 = peers.get(activePeer);
          if (r2 && r2.pc && !r2.pc.closed && r2.pc.localDescription && !r2.pc.remoteDescription) {
            pubSdp(activePeer, r2.pc.localDescription);
          }
        }
      });
  c.on('message', (topic, payload) => {
    let m; try { m = JSON.parse(payload.toString()); } catch { return; }
    if (topic.endsWith('/p')) onPresence(m);
    else if (topic.endsWith('/s')) onSignalMsg(m);
    else if (topic.endsWith('/c')) onChat(m);
  });
  c.on('error', (e) => { console.warn('mqtt err', e && e.message); });
  c.on('close', () => {
    if (!room) return;               // ушли из комнаты — не переподключаемся
    if (!connected) return;          // не дожил до connect — это обработал watchdog/failBroker
    console.log('[mqtt] closed from ' + uri + ', reconnecting same broker');
    // публичный EMQX "резает" коннекты раз в ~25с — это норма: переподключаемся к тому же брокеру
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => { reconnectTimer = null; connectMqtt(); }, 1200);
  });
}
function failBroker() {
  if (reconnectTimer) return;
  brokerIdx++;
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectMqtt(); }, 1500);
}
const pub = (topic, obj, qos) => { if (client && client.connected) { try { client.publish(topic, JSON.stringify(obj), { qos: qos || 0 }); } catch {} } };

function startAliveLoop() {
  if (aliveTimer) clearInterval(aliveTimer);
  aliveTimer = setInterval(() => { if (room) pub(TP + '/' + room + '/p', { id: myId, t: 'alive', ts: Date.now() }); }, ALIVE_EVERY);
}

// ---------- presence ----------
function onPresence(m) {
  if (!m || !m.id || m.id === myId) return;
  console.log('[VC] presence from=' + m.id.slice(0, 6) + ' t=' + m.t);
  if (m.t === 'leave') { dropPeer(m.id, 'left'); return; }
  if (m.t !== 'join' && m.t !== 'alive') return;
  const isNew = !peers.has(m.id);
  const rec = peers.get(m.id) || { lastSeen: 0, pc: null, cands: [] };
  rec.lastSeen = Date.now();
  peers.set(m.id, rec);
  if (isNew) {
    sysMsg('В комнату вошёл ' + m.id.slice(0, 6));
    if (peers.size >= MAX_ROOM) sysMsg('Комната заполнена, новые участники не смогут войти.');
  }
  maybeStartCall();
}
function dropPeer(id, why) {
  console.log('[VC] dropPeer ' + id.slice(0, 6) + ' reason=' + why);
  const rec = peers.get(id);
  if (!rec) return;
  if (rec.pc) { try { rec.pc.close(); } catch {} }
  peers.delete(id);
  if (activePeer === id) {
    activePeer = null;
    el.remote.srcObject = null;
    sysMsg(why === 'left' ? 'Собеседник ушёл.' : 'Собеседник отключился.');
    maybeStartCall();
  }
}
function sweepPeers() {
  const now = Date.now();
  for (const [id, rec] of [...peers]) {
    if (now - rec.lastSeen > PEER_TTL) dropPeer(id, 'gone');
  }
  if (!(el.stat.textContent || '').startsWith('соединение')) {
    setStat('комната «' + room + '»: ' + (peers.size + 1) + ' чел');
  }
}
// 1-на-1: зовём первого, если ещё не с кем-то в эфире
function maybeStartCall() {
  if (!joined || activePeer || !peers.size) return;
  const peer = [...peers.keys()][0];
  startCallWith(peer, myId < peer);
}

// ---------- WebRTC ----------
async function ensureMedia() {
  if (stream) return stream;
  stream = await navigator.mediaDevices.getUserMedia({ video: { width: { ideal: 1280 }, height: { ideal: 720 } }, audio: true });
  el.local.srcObject = stream;
  return stream;
}
function makePC(peer, onTrack) {
  const c = new RTCPeerConnection(pcConfig);
  c.__peer = peer;
  if (stream) for (const t of stream.getTracks()) c.addTrack(t, stream);
  c.ontrack = (e) => { onTrack && onTrack(e); };
  c.onicecandidate = (e) => {
    if (e.candidate) pub(TP + '/' + room + '/s', { from: myId, to: peer, data: JSON.stringify({ cand: e.candidate }) });
  };
  c.onconnectionstatechange = () => { setStat('соединение: ' + c.connectionState); };
  return c;
}
async function startCallWith(peer, initiator) {
  console.log('[VC] startCall ' + peer.slice(0, 6) + ' init=' + initiator + ' peers=' + peers.size + ' active=' + (activePeer || 'null'));
  if (peers.size >= MAX_ROOM) return;
  activePeer = peer;
  showCall('Подключение к ' + peer.slice(0, 6) + '…');
  try { await ensureMedia(); } catch (e) {
    activePeer = null;
    sysMsg('Нет доступа к камере/микрофону: ' + e.message);
    return;
  }
    const rec = peers.get(peer) || { lastSeen: Date.now(), pc: null, cands: [] };
    peers.set(peer, rec);
    // не пересоздаём живой PC (гонка: offer мог уже обработаться на старом PC)
    if (rec.pc && !rec.pc.closed && (rec.pc.localDescription || rec.pc.remoteDescription)) {
      // эфир уже идёт — не трогаем
    } else {
      if (rec.pc) { try { rec.pc.close(); } catch {} }
      rec.pc = makePC(peer, (e) => { if (peer === activePeer) el.remote.srcObject = e.streams[0] || e.track; });
    }
        if (initiator && rec.pc && !rec.pc.localDescription) {
          try {
            const off = await rec.pc.createOffer();
            await rec.pc.setLocalDescription(off);
            pubSdp(peer, rec.pc.localDescription);
          } catch (e) { console.warn('offer', e); }
        }
      }
    // Повторная отправка SDP: публичный брокер "рвёт" коннекты на ~20-30с,
    // поэтому каждое SDP-событие повторяем 4 раза (0/2.5/6/12с).
    function pubSdp(peer, sdp) {
      const msg = { from: myId, to: peer, data: JSON.stringify({ sdp }) };
      const send = () => pub(TP + '/' + room + '/s', msg, 1);
      send();
      setTimeout(send, 2500);
      setTimeout(send, 6000);
      setTimeout(send, 12000);
    }
    function onSignalMsg(m) {
      if (!m || m.to !== myId || !m.from) return;
      const from = m.from;
      let s; try { s = JSON.parse(m.data); } catch { return; }
      let rec = peers.get(from);
      if (!rec) return; // не из комнаты
      const peer = from;
      Promise.resolve((async () => {
        if (!rec.pc || rec.pc.closed) {
          try { await ensureMedia(); } catch {}
          rec.pc = makePC(peer, (e) => { if (peer === activePeer) el.remote.srcObject = e.streams[0] || e.track; });
        }
        let pc = rec.pc;
        if (s.sdp) {
          if (peer !== activePeer) {
            // переключаем эфир на этого собеседника
            if (activePeer && peers.get(activePeer) && peers.get(activePeer).pc) { try { peers.get(activePeer).pc.close(); } catch {} }
            activePeer = peer;
            showCall('Собеседник: ' + peer.slice(0, 6));
          }
          if (s.sdp.type === 'offer') {
            // уже соединены и получили дубликат offer — просто подтверждаем ответом
            if (pc.remoteDescription && pc.remoteDescription.type === 'offer' &&
                pc.localDescription && pc.localDescription.type === 'answer' &&
                (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed')) {
              pubSdp(peer, pc.localDescription);
              return;
            }
            // negotiation есть, но соединения нет — начинаем заново (свежие кандидаты)
            if (pc.remoteDescription || pc.localDescription) {
              try { pc.close(); } catch {}
              try { await ensureMedia(); } catch {}
              rec.pc = makePC(peer, (e) => { if (peer === activePeer) el.remote.srcObject = e.streams[0] || e.track; });
              pc = rec.pc;
              rec.cands = [];
            }
            await pc.setRemoteDescription(new RTCSessionDescription(s.sdp));
            for (const cand of rec.cands || []) { try { await pc.addIceCandidate(new RTCIceCandidate(cand)); } catch {} }
            rec.cands = [];
            const an = await pc.createAnswer();
            await pc.setLocalDescription(an);
            pubSdp(peer, pc.localDescription);
          } else if (s.sdp.type === 'answer') {
            if (pc.remoteDescription && pc.remoteDescription.type === 'answer') return; // дубликат
            await pc.setRemoteDescription(new RTCSessionDescription(s.sdp));
            for (const cand of rec.cands || []) { try { await pc.addIceCandidate(new RTCIceCandidate(cand)); } catch {} }
            rec.cands = [];
          }
        } else if (s.cand) {
          if (!pc.remoteDescription) (rec.cands = rec.cands || []).push(s.cand);
          else { try { await pc.addIceCandidate(new RTCIceCandidate(s.cand)); } catch {} }
        }
      })()).catch((e) => console.warn('signal err', e));
    }

// ---------- чат ----------
function onChat(m) {
  if (!m || !m.text) return;
  addMsg(m.from === myId ? 'out' : 'in', m.text, m.from);
}
el.chatform.addEventListener('submit', (e) => {
  e.preventDefault();
  const t = el.chatinput.value.trim();
  if (!t || !room) return;
  pub(TP + '/' + room + '/c', { from: myId, text: t });
  el.chatinput.value = '';
});

// ---------- кнопки ----------
el.createbtn.addEventListener('click', () => {
  const code = rndCode();
  enterRoom(code, true);
});
el.copybtn.addEventListener('click', () => {
  el.roomlink.select();
  try { navigator.clipboard.writeText(el.roomlink.value); } catch { document.execCommand('copy'); }
  el.copybtn.textContent = '✓';
  setTimeout(() => { el.copybtn.textContent = 'Копировать'; }, 1500);
});
el.sharebtn.addEventListener('click', () => {
  if (!room) return;
  const link = roomLink(room);
  try { navigator.clipboard.writeText(link); } catch {}
  el.status.textContent = 'Ссылка скопирована';
  el.status.style.display = '';
});
el.joinbtn.addEventListener('click', () => {
  const code = el.joinname.value.trim().toLowerCase().replace(/[^a-z0-9]/g, '');
  if (code.length < 3) { alert('Введите код комнаты (3+ символов).'); return; }
  enterRoom(code, false);
});
el.leavebtn.addEventListener('click', () => { leaveRoom(); });
el.micbtn.addEventListener('click', () => { if (stream) for (const t of stream.getAudioTracks()) t.enabled = !t.enabled; });
el.cambtn.addEventListener('click', () => { if (stream) for (const t of stream.getVideoTracks()) t.enabled = !t.enabled; });

function enterRoom(code, created) {
  room = code;
  el.roomlink.value = roomLink(code);
  el.roomlinkbox.classList.remove('hidden');
  showCall('Соединение с комнатой…');
  connectMqtt();
  if (created) sysMsg('Комната создана. Пришлите ссылку собеседнику.');
}
function leaveRoom() {
  if (room) pub(TP + '/' + room + '/p', { id: myId, t: 'leave' }, 1);
  try { if (client) client.end(true); } catch {}
  client = null;
  if (aliveTimer) { clearInterval(aliveTimer); aliveTimer = null; }
  if (sweepTimer) { clearInterval(sweepTimer); sweepTimer = null; }
  for (const rec of peers.values()) { if (rec.pc) { try { rec.pc.close(); } catch {} } }
  peers.clear();
  activePeer = null;
  el.remote.srcObject = null;
  setStat('');
  room = null;
  joined = false;
  brokerIdx = 0;
  showHome();
}
function autoJoinFromUrl() {
  const m = location.search.match(/[?&]room=([a-zA-Z0-9]+)/);
  if (m) enterRoom(m[1].toLowerCase(), false);
}
window.addEventListener('beforeunload', () => { if (room) leaveRoom(); });
