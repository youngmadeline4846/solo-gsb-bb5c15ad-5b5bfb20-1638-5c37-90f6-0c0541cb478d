'use strict';

const assert = require('assert');
const http = require('http');
const path = require('path');
const { spawn } = require('child_process');
const WebSocket = require('ws');

const TEST_PORT = process.env.TEST_PORT || '3099';
const BASE = `http://127.0.0.1:${TEST_PORT}`;
// 测试用：把限时发言的 1「分钟」压缩为 50ms，便于快速验证到期自动结束
const SPEAK_TIME_UNIT_MS = 50;
// 测试用：点名确认时限压缩为 800ms，便于快速验证超时接力
const ROLLCALL_TIMEOUT_MS = 800;
// 测试用：议题投票时长下限压缩为 1 秒，便于快速验证到期结算
const TOPIC_MIN_SECONDS = 1;
// 测试用：把发言配额的 1「分钟」压缩为 50ms，便于快速验证额度耗尽自动结束
const QUOTA_TIME_UNIT_MS = 50;
let failures = 0;
let serverProc = null;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function test(name, fn) {
  return Promise.resolve()
    .then(fn)
    .then(() => console.log(`  ✅ ${name}`))
    .catch((err) => {
      failures++;
      console.error(`  ❌ ${name}: ${err.message}`);
      console.error(err.stack);
    });
}

function postJSON(path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      BASE + path,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) }
      },
      (res) => {
        let buf = '';
        res.on('data', (c) => (buf += c));
        res.on('end', () => {
          try {
            resolve({ status: res.statusCode, body: JSON.parse(buf) });
          } catch (e) {
            reject(e);
          }
        });
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

class Client {
  constructor() {
    this.ws = new WebSocket('ws://127.0.0.1:'+TEST_PORT+'/ws');
    this.queue = [];
    this.snapshot = null;
    this.lastError = null;
    this.joined = null;
    this.ws.on('message', (raw) => {
      const msg = JSON.parse(raw);
      if (msg.type === 'snapshot') this.snapshot = msg.snapshot;
      if (msg.type === 'joined') {
        this.joined = msg;
        this.snapshot = msg.snapshot;
      }
      if (msg.type === 'error') this.lastError = msg;
      this.queue.push(msg);
    });
  }
  open() {
    return new Promise((res, rej) => {
      this.ws.on('open', res);
      this.ws.on('error', rej);
    });
  }
  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }
  close() {
    return new Promise((res) => {
      this.ws.on('close', res);
      this.ws.close();
    });
  }
  waitFor(fn, timeout = 2000, name = new Error().stack.split('\n')[2].trim()) {
    return new Promise((resolve, reject) => {
      const t = setInterval(() => {
        try {
          if (fn(this)) {
            clearInterval(t);
            clearTimeout(to);
            resolve();
          }
        } catch { /* keep waiting */ }
      }, 20);
      const to = setTimeout(() => {
        clearInterval(t);
        reject(new Error(`timeout at: ${name}`));
      }, timeout);
    });
  }
  get v() {
    return this.snapshot ? this.snapshot.version : null;
  }
}

// ---------------------------------------------------------------------------
// 安全撤销测试辅助：独立房间，避免干扰主流程
// ---------------------------------------------------------------------------

async function createRoom(nick) {
  const created = await postJSON('/api/rooms', { nick: nick || '主持' });
  assert.strictEqual(created.status, 200);
  return created.body;
}

async function hostJoin(created, nick) {
  const c = new Client();
  await c.open();
  c.send({
    type: 'join',
    room: created.room,
    nick: nick || '主持',
    participantId: created.participantId,
    hostToken: created.hostToken
  });
  await c.waitFor((x) => x.joined && x.joined.you.host === true);
  return c;
}

async function guestJoin(code, nick) {
  const c = new Client();
  await c.open();
  c.send({ type: 'join', room: code, nick });
  await c.waitFor((x) => x.joined && x.joined.you.nick === nick);
  return c;
}

/** 主持人执行一条版本化命令并等待版本推进 */
async function hostCmd(host, cmd) {
  const v = host.v;
  host.lastError = null;
  host.send(Object.assign({ version: host.v }, cmd));
  await host.waitFor((c) => c.v === v + 1 || (c.lastError && c.lastError.code === 'STALE_VERSION'),
    2000, `hostCmd ${cmd.type} v=${v}`);
}

/**
 * 等待房间版本稳定：host 的 join 提交会在 guest join 之后再广播一个快照，
 * guest 需等到该快照到达，否则其首条命令会被判为 STALE_VERSION。
 */
async function waitStable(host, clients, ms = 150) {
  const v = host.v;
  await new Promise((r) => setTimeout(r, ms));
  for (const c of clients) {
    await c.waitFor((x) => x.v === host.v && x.v >= v);
  }
}

/** 以 guest 视角发送版本化命令：先同步到主持人的最新版本，避免 STALE_VERSION 干扰断言 */
async function guestSend(host, guest, cmd) {
  await waitStable(host, [guest]);
  guest.lastError = null;
  guest.send(Object.assign({ version: guest.v }, cmd));
}

async function waitForUndo(host, predicate) {
  await host.waitFor((c) => c.snapshot.undo && predicate(c.snapshot.undo));
}

async function main() {
  await startServer();

  // 1. 创建房间
  const created = await postJSON('/api/rooms', { nick: '张主持' });
  assert.strictEqual(created.status, 200);
  assert.match(created.body.room, /^\d{6}$/);
  const code = created.body.room;
  console.log(`  (房间码 ${code})`);

  // 2. 主持人加入
  const host = new Client();
  await host.open();
  host.send({ type: 'join', room: code, nick: '张主持', participantId: created.body.participantId, hostToken: created.body.hostToken });
  await host.waitFor((c) => c.joined && c.joined.you.host === true, 2000, 'host join');
  assert.strictEqual(host.snapshot.hostId, created.body.participantId);

  // 3. 参与者加入
  const alice = new Client();
  await alice.open();
  alice.send({ type: 'join', room: code, nick: '小明' });
  await alice.waitFor((c) => c.joined && c.joined.you.nick === '小明');
  const aliceId = alice.joined.you.id;
  assert.ok(alice.joined.you.secret, '新参与者应收到 secret');

  const bob = new Client();
  await bob.open();
  bob.send({ type: 'join', room: code, nick: '小红' });
  await bob.waitFor((c) => c.snapshot && c.snapshot.participants.length === 3);
  const bobId = bob.joined.you.id;

  // 4. 两人申请发言；版本原子递增
  const v0 = alice.v;
  alice.send({ type: 'request_speak', version: alice.v });
  await alice.waitFor((c) => c.v === v0 + 1);
  bob.send({ type: 'request_speak', version: bob.v });
  await bob.waitFor((c) => c.v === v0 + 2);
  await host.waitFor((c) => c.v === v0 + 2);
  assert.deepStrictEqual(host.snapshot.order, [aliceId, bobId]);

  // 5. 过期命令：用旧版本申请 -> STALE_VERSION 且携带最新快照
  alice.lastError = null;
  alice.send({ type: 'request_speak', version: v0 });
  await alice.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
  assert.strictEqual(alice.lastError.snapshot.version, host.snapshot.version, '过期命令必须返回最新快照');
  assert.deepStrictEqual(alice.lastError.snapshot.order, [aliceId, bobId]);

  // 6. 取消发言
  const v1 = bob.v;
  bob.send({ type: 'cancel_speak', version: v1 });
  await bob.waitFor((c) => c.v === v1 + 1);
  assert.deepStrictEqual(bob.snapshot.order, [aliceId]);

  // 7. 非主持人不能开始发言
  alice.lastError = null;
  alice.send({ type: 'start_speaking', version: alice.v, participantId: aliceId });
  await alice.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');
  assert.strictEqual(alice.v, v1 + 1, '被拒绝的命令不得改变版本');

  // 8. 主持人开始发言 -> 从队列移除，startedAt 计时
  const beforeStart = Date.now();
  host.send({ type: 'start_speaking', version: host.v, participantId: aliceId });
  await host.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === aliceId);
  assert.deepStrictEqual(host.snapshot.order, []);
  assert.ok(host.snapshot.speaking.startedAt >= beforeStart);
  assert.ok(Math.abs(host.snapshot.serverTs - Date.now()) < 1000);
  const startVersion = host.v;

  // 9. 重新排队并拖动排序（alice 正在发言，由 bob 和 carol 排队）
  const carol = new Client();
  await carol.open();
  carol.send({ type: 'join', room: code, nick: '小刚' });
  await carol.waitFor((c) => c.joined && c.joined.you.nick === '小刚');
  const carolId = carol.joined.you.id;

  bob.send({ type: 'request_speak', version: bob.v });
  await host.waitFor((c) => c.snapshot.order.length === 1);
  carol.send({ type: 'request_speak', version: carol.v });
  await host.waitFor((c) => c.snapshot.order.length === 2);
  assert.deepStrictEqual(host.snapshot.order, [bobId, carolId]);

  // 非法 reorder：成员集合不一致
  host.lastError = null;
  host.send({ type: 'reorder', version: host.v, order: [carolId] });
  await host.waitFor((c) => c.lastError && c.lastError.code === 'ORDER_MISMATCH');

  // 合法 reorder
  host.lastError = null;
  host.send({ type: 'reorder', version: host.v, order: [carolId, bobId] });
  await host.waitFor((c) => c.snapshot.order[0] === carolId && c.v > startVersion + 1);
  assert.deepStrictEqual(host.snapshot.order, [carolId, bobId]);

  // 10. 断线重连：bob 断线期间保留席位与队列位置，重连后恢复一致顺序
  const bobSecret = bob.joined.you.secret;
  const orderBefore = host.snapshot.order;
  await bob.close();
  await host.waitFor((c) => {
    const p = c.snapshot.participants.find((x) => x.id === bobId);
    return p && p.online === false;
  });
  assert.deepStrictEqual(host.snapshot.order, orderBefore, '离线后队列位置保留');

  const bob2 = new Client();
  await bob2.open();
  bob2.send({ type: 'join', room: code, nick: '小红', participantId: bobId, secret: bobSecret });
  await bob2.waitFor((c) => c.joined && c.joined.you.id === bobId);
  assert.deepStrictEqual(bob2.snapshot.order, host.snapshot.order, '重连后顺序一致');
  assert.ok(bob2.snapshot.speaking, '重连后当前发言状态恢复');
  assert.strictEqual(bob2.snapshot.speaking.id, aliceId);
  const gap = Date.now() + (bob2.snapshot.serverTs - Date.now()) - bob2.snapshot.speaking.startedAt;
  assert.ok(gap > 0 && gap < 10000, `重连后可继续计时（gap=${gap}ms）`);

  // 11. 主持人结束发言
  host.send({ type: 'end_speaking', version: host.v });
  await host.waitFor((c) => c.snapshot.speaking === null);

  // 12. 一键清空（队列 + 发言 + 记录）
  // 队列中还有 bob、carol；让 bob 开始发言，制造 队列+发言+记录 数据
  host.send({ type: 'start_speaking', version: host.v, participantId: bobId });
  await host.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === bobId);
  assert.strictEqual(host.snapshot.order.length, 1, 'carol 仍在队列');
  assert.ok(host.snapshot.logs.length > 0);

  host.send({ type: 'clear', version: host.v });
  await host.waitFor((c) => c.snapshot.logs.length === 1); // 清空本身留一条系统记录
  assert.deepStrictEqual(host.snapshot.order, []);
  assert.strictEqual(host.snapshot.speaking, null);
  assert.match(host.snapshot.logs[0].text, /一键清空/);

  // 13. 操作记录类型完整
  const kinds = new Set(host.snapshot.logs.map((l) => l.kind));
  assert.ok(kinds.has('system'));
  // 此前记录里应有 talk / queue / presence（清空后只剩 system，但整个过程中曾出现）

  // ===========================================================================
  // 协商换位
  // ===========================================================================

  // 14a. 三人排队：dave -> alice -> bob
  const dave = new Client();
  await dave.open();
  dave.send({ type: 'join', room: code, nick: '小兰' });
  await dave.waitFor((c) => c.joined && c.joined.you.nick === '小兰');
  const daveId = dave.joined.you.id;

  dave.send({ type: 'request_speak', version: dave.v });
  await host.waitFor((c) => c.snapshot.order.length === 1);
  alice.send({ type: 'request_speak', version: alice.v });
  await host.waitFor((c) => c.snapshot.order.length === 2);
  bob2.send({ type: 'request_speak', version: bob2.v });
  await host.waitFor((c) => c.snapshot.order.length === 3);
  assert.deepStrictEqual(host.snapshot.order, [daveId, aliceId, bobId]);

  // 14b. dave 向 bob 发起换位邀请：记录发起时版本与位置，随快照广播
  const inviteVersion = dave.v;
  dave.send({ type: 'swap_invite', version: inviteVersion, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === daveId && s.to === bobId));
  let swap = host.snapshot.swaps.find((s) => s.from === daveId && s.to === bobId);
  assert.strictEqual(swap.version, inviteVersion, '邀请应记录发起时的队列版本');
  assert.strictEqual(swap.fromPos, 0);
  assert.strictEqual(swap.toPos, 2);
  const swapId = swap.id;
  await bob2.waitFor((c) => c.snapshot.swaps.some((s) => s.id === swapId && s.status === 'pending'));

  // 14c. 每人同一时刻最多参与一条待处理邀请
  dave.lastError = null;
  dave.send({ type: 'swap_invite', version: dave.v, participantId: aliceId });
  await dave.waitFor((c) => c.lastError && c.lastError.code === 'SWAP_BUSY');
  alice.lastError = null;
  alice.send({ type: 'swap_invite', version: alice.v, participantId: bobId });
  await alice.waitFor((c) => c.lastError && c.lastError.code === 'SWAP_BUSY');

  // 不能邀请自己 / 不在队列中的人
  dave.lastError = null;
  dave.send({ type: 'swap_invite', version: dave.v, participantId: daveId });
  await dave.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TARGET');
  dave.lastError = null;
  dave.send({ type: 'swap_invite', version: dave.v, participantId: carolId });
  await dave.waitFor((c) => c.lastError && c.lastError.code === 'TARGET_NOT_QUEUED');

  // 14d. 非目标方不能接受；目标方接受 -> 原子交换
  alice.lastError = null;
  alice.send({ type: 'swap_accept', version: alice.v, swapId });
  await alice.waitFor((c) => c.lastError && c.lastError.code === 'SWAP_NOT_YOURS');
  assert.deepStrictEqual(host.snapshot.order, [daveId, aliceId, bobId], '被拒绝的接受不得改动队列');

  bob2.send({ type: 'swap_accept', version: bob2.v, swapId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swapId && s.status === 'accepted'));
  assert.deepStrictEqual(host.snapshot.order, [bobId, aliceId, daveId], '接受后双方位置交换');

  // 14e. 已结束的邀请不能重复操作
  bob2.lastError = null;
  bob2.send({ type: 'swap_accept', version: bob2.v, swapId });
  await bob2.waitFor((c) => c.lastError && c.lastError.code === 'SWAP_RESOLVED');

  // 14f. 发起邀请后被主持人重排 -> 邀请失效且队列保持为新顺序
  bob2.send({ type: 'swap_invite', version: bob2.v, participantId: aliceId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === bobId && s.to === aliceId));
  const swap2 = host.snapshot.swaps.find((s) => s.from === bobId && s.to === aliceId && s.status === 'pending');

  host.send({ type: 'reorder', version: host.v, order: [daveId, bobId, aliceId] });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap2.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap2.id).reason, 'reordered');
  assert.deepStrictEqual(host.snapshot.order, [daveId, bobId, aliceId]);

  // 14g. 一方取消申请 -> 相关待处理邀请失效
  dave.send({ type: 'swap_invite', version: dave.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === daveId && s.to === bobId));
  const swap3 = host.snapshot.swaps.find((s) => s.from === daveId && s.to === bobId && s.status === 'pending');
  dave.send({ type: 'cancel_speak', version: dave.v });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap3.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap3.id).reason, 'canceled_speak');

  // 14h. 一方开始发言 -> 相关待处理邀请失效（bob 邀请 alice，主持人请 bob 发言）
  assert.deepStrictEqual(host.snapshot.order, [bobId, aliceId], 'dave 取消后队列剩 bob、alice');
  bob2.send({ type: 'swap_invite', version: bob2.v, participantId: aliceId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === bobId && s.to === aliceId));
  const swap4 = host.snapshot.swaps.find((s) => s.from === bobId && s.to === aliceId && s.status === 'pending');
  host.send({ type: 'start_speaking', version: host.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap4.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap4.id).reason, 'started_speaking');

  // 14i. 一方主动离开 -> 相关待处理邀请失效（alice 邀请已归队的 dave，随后 dave 离开）
  host.send({ type: 'end_speaking', version: host.v });
  await host.waitFor((c) => c.snapshot.speaking === null);
  dave.send({ type: 'request_speak', version: dave.v });
  await host.waitFor((c) => c.snapshot.order.length === 2);
  alice.send({ type: 'swap_invite', version: alice.v, participantId: daveId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === aliceId && s.to === daveId));
  const swap5 = host.snapshot.swaps.find((s) => s.from === aliceId && s.to === daveId && s.status === 'pending');
  dave.send({ type: 'leave' });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap5.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap5.id).reason, 'left');

  // 14j. 拒绝流程：alice 邀请 bob，bob 拒绝，队列不变
  assert.deepStrictEqual(host.snapshot.order, [aliceId]);
  bob2.send({ type: 'request_speak', version: bob2.v });
  await host.waitFor((c) => c.snapshot.order.length === 2);
  assert.deepStrictEqual(host.snapshot.order, [aliceId, bobId]);
  alice.send({ type: 'swap_invite', version: alice.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === aliceId && s.to === bobId));
  const swap6 = host.snapshot.swaps.find((s) => s.from === aliceId && s.to === bobId && s.status === 'pending');
  bob2.send({ type: 'swap_reject', version: bob2.v, swapId: swap6.id });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap6.id && s.status === 'rejected'));
  assert.deepStrictEqual(host.snapshot.order, [aliceId, bobId], '拒绝后队列不变');

  // 14k. 撤回流程：alice 撤回邀请
  alice.send({ type: 'swap_invite', version: alice.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === aliceId && s.to === bobId));
  const swap7 = host.snapshot.swaps.find((s) => s.from === aliceId && s.to === bobId && s.status === 'pending');
  alice.send({ type: 'swap_cancel', version: alice.v, swapId: swap7.id });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap7.id && s.status === 'cancelled'));

  // 14l. 被主持人移除 -> 相关待处理邀请失效，收尾队列
  alice.send({ type: 'swap_invite', version: alice.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === aliceId && s.to === bobId));
  const swap8 = host.snapshot.swaps.find((s) => s.from === aliceId && s.to === bobId && s.status === 'pending');
  host.send({ type: 'remove', version: host.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap8.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap8.id).reason, 'removed');

  // 14m. 接受不存在的邀请 -> NO_SWAP；防御性的位置重校验由各联动失效场景覆盖
  // （协议层面：任何能改变参与方位置的队列操作都会先联动失效邀请）
  bob2.lastError = null;
  bob2.send({ type: 'swap_accept', version: bob2.v, swapId: 'sw_not_exist' });
  await bob2.waitFor((c) => c.lastError && c.lastError.code === 'NO_SWAP');

  await dave.close();

  // ===========================================================================
  // 安全撤销（四类逆操作 + 介入变更边界）
  // ===========================================================================

  // 16. reorder 逆操作：恢复拖动前队列
  {
    const created = await createRoom('撤销主持');
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿甲');
    const g2 = await guestJoin(created.room, '阿乙');
    const g3 = await guestJoin(created.room, '阿丙');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    const id3 = g3.joined.you.id;

    await waitStable(h, [g1, g2, g3]);
    g1.send({ type: 'request_speak', version: g1.v });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    g2.send({ type: 'request_speak', version: g2.v });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    g3.send({ type: 'request_speak', version: g3.v });
    await h.waitFor((c) => c.snapshot.order.length === 3);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2, id3]);

    const vBefore = h.v;
    await hostCmd(h, { type: 'reorder', order: [id3, id1, id2] });
    assert.deepStrictEqual(h.snapshot.order, [id3, id1, id2]);
    // 快照展示可撤销操作摘要
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'reorder');
    assert.strictEqual(h.snapshot.undo.version, vBefore + 1, '撤销锚点为操作提交后的版本');
    assert.match(h.snapshot.undo.summary, /顺序/);
    assert.strictEqual(h.snapshot.undo.reason, null);

    // 撤销 -> 队列原子恢复，undo 清空，追加 audit 记录
    const vUndo = h.v;
    h.send({ type: 'undo_host', version: vUndo });
    await h.waitFor((c) => c.v === vUndo + 1);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2, id3]);
    assert.strictEqual(h.snapshot.undo, null);
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'audit' && /恢复.*顺序/.test(l.text)),
      '撤销需追加可见审计记录');

    // 撤销后再次撤销 -> UNDO_NONE，版本不变
    h.lastError = null;
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'UNDO_NONE');
    assert.ok(h.lastError.snapshot, '错误应附带最新快照');

    // 拖动排序后出现介入变更（新成员申请发言）-> 撤销立即失效
    await hostCmd(h, { type: 'reorder', order: [id2, id3, id1] });
    await waitForUndo(h, (u) => u.status === 'available');
    const staleV = h.v;
    await guestSend(h, g1, { type: 'cancel_speak' });
    await h.waitFor((c) => c.snapshot.undo && c.snapshot.undo.status === 'invalid');
    assert.strictEqual(h.snapshot.undo.op, 'reorder');
    assert.strictEqual(h.snapshot.undo.reason, 'queue_changed');
    assert.ok(h.snapshot.undo.reasonText, '失效需带可读原因');

    // 失效后撤销 -> UNDO_INVALIDATED，不产生新版本
    h.lastError = null;
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'UNDO_INVALIDATED');
    assert.strictEqual(h.lastError.snapshot.version, staleV + 1);
    assert.strictEqual(h.v, staleV + 1, '失效撤销不得改变版本');

    // 用过期版本撤销 -> STALE_VERSION + 最新快照
    h.lastError = null;
    h.send({ type: 'undo_host', version: staleV - 1 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
    assert.strictEqual(h.lastError.snapshot.version, h.snapshot.version);

    // 非主持人不能撤销
    await guestSend(h, g2, { type: 'undo_host' });
    await g2.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');

    await h.close(); await g1.close(); await g2.close(); await g3.close();
  }

  // 17. remove 逆操作：恢复队列位置；正在发言的成员恢复发言且 startedAt 保留
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿猫');
    const g2 = await guestJoin(created.room, '阿狗');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;

    await waitStable(h, [g1, g2]);
    g1.send({ type: 'request_speak', version: g1.v });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    g2.send({ type: 'request_speak', version: g2.v });
    await h.waitFor((c) => c.snapshot.order.length === 2);

    // 移出队首 -> 撤销恢复到原下标
    await hostCmd(h, { type: 'remove', participantId: id1 });
    assert.deepStrictEqual(h.snapshot.order, [id2]);
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'remove');
    assert.match(h.snapshot.undo.summary, /阿猫/);
    // 成员席位与在线状态不受影响
    const p1 = h.snapshot.participants.find((p) => p.id === id1);
    assert.ok(p1, '移出队列不得删除成员席位');

    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2], '应恢复到原队列位置');
    assert.strictEqual(h.snapshot.undo, null);

    // 移出正在发言者 -> 撤销恢复发言，startedAt 不变
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    const startedAt = h.snapshot.speaking.startedAt;
    assert.ok(h.snapshot.speaking && h.snapshot.speaking.id === id1);
    await hostCmd(h, { type: 'remove', participantId: id1 });
    assert.strictEqual(h.snapshot.speaking, null);
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === id1);
    assert.strictEqual(h.snapshot.speaking.startedAt, startedAt, '撤销恢复发言须保留原计时起点');

    // 移出后有新介入（他人申请）-> 撤销失效
    await hostCmd(h, { type: 'remove', participantId: id2 });
    assert.deepStrictEqual(h.snapshot.order, []);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.undo && c.snapshot.undo.status === 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'queue_changed');
    h.lastError = null;
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'UNDO_INVALIDATED');
    assert.deepStrictEqual(h.snapshot.order, [id2], '失效撤销不得改动队列');

    await h.close(); await g1.close(); await g2.close();
  }

  // 18. start_speaking 逆操作：发言人回到原队列位置，并恢复之前的发言人
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿东');
    const g2 = await guestJoin(created.room, '阿西');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;

    await waitStable(h, [g1, g2]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);

    // 请队首发言：从队列移除
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    assert.deepStrictEqual(h.snapshot.order, [id2]);
    assert.strictEqual(h.snapshot.speaking.id, id1);
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'start_speaking');

    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking === null);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2], '撤销后发言人回到原队列位置');
    assert.strictEqual(h.snapshot.undo, null);

    // 带「切换发言人」语义：id1 发言中，请 id2 发言（自动结束 id1），撤销应恢复 id1 与计时
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    const firstStartedAt = h.snapshot.speaking.startedAt;
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await hostCmd(h, { type: 'start_speaking', participantId: id2 });
    assert.strictEqual(h.snapshot.speaking.id, id2);
    assert.deepStrictEqual(h.snapshot.order, []);
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === id1);
    assert.strictEqual(h.snapshot.speaking.startedAt, firstStartedAt, '原发言人计时连续');
    assert.deepStrictEqual(h.snapshot.order, [id2], '新发言人回到原队列位置');

    await h.close(); await g1.close(); await g2.close();
  }

  // 19. end_speaking 逆操作：恢复当前发言，startedAt 保持连续
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿南');
    const id1 = g1.joined.you.id;

    await waitStable(h, [g1]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    const startedAt = h.snapshot.speaking.startedAt;

    await hostCmd(h, { type: 'end_speaking' });
    assert.strictEqual(h.snapshot.speaking, null);
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'end_speaking');

    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === id1);
    assert.strictEqual(h.snapshot.speaking.startedAt, startedAt, '恢复发言须沿用原 startedAt');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'audit' && /计时连续/.test(l.text)));

    await h.close(); await g1.close();
  }

  // 20. 介入变更边界：仅保留最近一次主持人操作；成员/换位/清空/一键清空均使撤销失效
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿春');
    const g2 = await guestJoin(created.room, '阿夏');
    const g3 = await guestJoin(created.room, '阿秋');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    const id3 = g3.joined.you.id;

    await waitStable(h, [g1, g2, g3]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    await guestSend(h, g3, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 3);

    // 连续两次主持人操作：只保留最近一次（旧的不可再撤销）
    await hostCmd(h, { type: 'reorder', order: [id2, id1, id3] });
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'reorder');
    await hostCmd(h, { type: 'reorder', order: [id2, id3, id1] });
    assert.strictEqual(h.snapshot.undo.op, 'reorder');
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.undo === null);
    assert.deepStrictEqual(h.snapshot.order, [id2, id1, id3], '撤销只能回退到最近一次操作之前');
    assert.notDeepStrictEqual(h.snapshot.order, [id1, id2, id3], '不允许连续撤销多步');

    // 成员加入/断线/离开均属于版本化状态变更，撤销立即失效
    await hostCmd(h, { type: 'reorder', order: [id1, id3, id2] });
    await waitForUndo(h, (u) => u.status === 'available');
    const g4 = await guestJoin(created.room, '阿冬');
    await h.waitFor((c) => c.snapshot.undo && c.snapshot.undo.status === 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'presence_changed');
    await g4.close();
    // 等待服务端处理完断线提交后再继续，避免主持人命令踩中过期版本
    await h.waitFor((c) => {
      const p = c.snapshot.participants.find((x) => x.nick === '阿冬');
      return p && p.online === false;
    });

    // 换位状态变更同样使撤销失效
    await hostCmd(h, { type: 'reorder', order: [id3, id1, id2] });
    await waitForUndo(h, (u) => u.status === 'available');
    await guestSend(h, g1, { type: 'swap_invite', participantId: id2 });
    await h.waitFor((c) => c.snapshot.undo && c.snapshot.undo.status === 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'swap_changed');

    // 一键清空使撤销立即失效并注明原因（clear 本身不可撤销）
    await hostCmd(h, { type: 'reorder', order: [id3, id2, id1] });
    await waitForUndo(h, (u) => u.status === 'available');
    await hostCmd(h, { type: 'clear' });
    assert.strictEqual(h.snapshot.undo.status, 'invalid', '一键清空使撤销失效');
    assert.strictEqual(h.snapshot.undo.reason, 'cleared');
    h.lastError = null;
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'UNDO_INVALIDATED');

    // 清空记录（日志变更）也使挂起撤销失效
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await hostCmd(h, { type: 'remove', participantId: id1 });
    await waitForUndo(h, (u) => u.status === 'available');
    await hostCmd(h, { type: 'clear_logs' });
    assert.strictEqual(h.snapshot.undo.status, 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'logs_cleared');

    await h.close(); await g1.close(); await g2.close(); await g3.close();
  }

  // 21. 撤销边界：不回滚在线状态与身份凭证；审计记录独立可见
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿梅');
    const g2 = await guestJoin(created.room, '阿兰');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    const secret2 = g2.joined.you.secret;

    await waitStable(h, [g1, g2]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);

    // 主持人移出 g2 后，g2 断线；撤销恢复队列位置但保持离线
    await hostCmd(h, { type: 'remove', participantId: id2 });
    assert.deepStrictEqual(h.snapshot.order, [id1]);
    await g2.close();
    await h.waitFor((c) => {
      const p = c.snapshot.participants.find((x) => x.id === id2);
      return p && p.online === false;
    });
    // 注意：断线本身是版本化变更，已使撤销失效，主持人无法借撤销回滚在线状态
    assert.strictEqual(h.snapshot.undo.status, 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'presence_changed');
    // 身份凭证仍然有效：g2 可凭原 id+secret 重连，席位仍在
    const g2b = new Client();
    await g2b.open();
    g2b.send({ type: 'join', room: created.room, nick: '阿兰', participantId: id2, secret: secret2 });
    await g2b.waitFor((c) => c.joined && c.joined.you.id === id2);
    const still = g2b.snapshot.participants.find((p) => p.id === id2);
    assert.ok(still && still.online === true);
    assert.ok(!g2b.snapshot.order.includes(id2), '失效撤销没有恢复队列位置');

    // 审计记录不参与撤销：成功撤销产生的 audit 日志在后续操作后依然保留
    // 构造一次成功撤销并验证审计独立
    await guestSend(h, g2b, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    await hostCmd(h, { type: 'reorder', order: [id2, id1] });
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.undo === null);
    const auditCount = h.snapshot.logs.filter((l) => l.kind === 'audit').length;
    await guestSend(h, g1, { type: 'cancel_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    assert.strictEqual(
      h.snapshot.logs.filter((l) => l.kind === 'audit').length,
      auditCount,
      '后续变更不得删除既有审计记录'
    );

    await h.close(); await g1.close(); await g2b.close();
  }

  // ===========================================================================
  // 限时发言（SPEAK_TIME_UNIT_MS=50ms：协议中的 1「分钟」= 50ms）
  // ===========================================================================
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '计时甲');
    const g2 = await guestJoin(created.room, '计时乙');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;

    await waitStable(h, [g1, g2]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);

    // 22a. 非法时长：0 / 31 / 非整数 / 非数字均被拒绝且不改变版本
    for (const bad of [0, 31, 1.5, 'abc']) {
      const vBad = h.v;
      h.lastError = null;
      h.send({ type: 'start_speaking', version: h.v, participantId: id1, durationMin: bad });
      await h.waitFor((c) => c.lastError && c.lastError.code === 'BAD_DURATION');
      assert.strictEqual(h.v, vBad, '非法时长不得改变版本');
    }

    // 22b. 不限时：endsAt 为 null，仍下发唯一轮次标识 roundId
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    assert.ok(h.snapshot.speaking && h.snapshot.speaking.id === id1);
    assert.strictEqual(h.snapshot.speaking.endsAt, null);
    assert.ok(h.snapshot.speaking.roundId, '每轮发言都应下发唯一轮次标识');
    assert.ok(h.snapshot.speaking.startedAt > 0);
    await hostCmd(h, { type: 'end_speaking' });

    // 22c. 限时开始：快照下发 startedAt / endsAt / roundId，时长换算正确并写记录
    await hostCmd(h, { type: 'start_speaking', participantId: id1, durationMin: 30 });
    const sp1 = h.snapshot.speaking;
    assert.ok(sp1 && sp1.id === id1);
    assert.strictEqual(sp1.endsAt - sp1.startedAt, 30 * SPEAK_TIME_UNIT_MS);
    const round1 = sp1.roundId;
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'talk' && /限时 30 分钟/.test(l.text)));

    // 22d. 切换发言人使旧定时任务失效：旧回调不得结束新轮次
    await hostCmd(h, { type: 'start_speaking', participantId: id2 }); // 不限时新轮次
    assert.strictEqual(h.snapshot.speaking.id, id2);
    assert.strictEqual(h.snapshot.speaking.endsAt, null);
    assert.notStrictEqual(h.snapshot.speaking.roundId, round1, '新轮次应有新的 roundId');
    await sleep(30 * SPEAK_TIME_UNIT_MS + 400); // 远超旧轮次 endsAt
    assert.ok(h.snapshot.speaking && h.snapshot.speaking.id === id2, '旧定时回调不得结束新轮次');
    assert.ok(!h.snapshot.logs.some((l) => /时间到/.test(l.text)), '旧轮次不得产生到期记录');

    // 22e. 手动结束使定时任务失效
    await hostCmd(h, { type: 'end_speaking' }); // 结束 id2
    await hostCmd(h, { type: 'start_speaking', participantId: id1, durationMin: 30 });
    await hostCmd(h, { type: 'end_speaking' }); // 立即手动结束
    const vAfterManualEnd = h.v;
    await sleep(30 * SPEAK_TIME_UNIT_MS + 400);
    assert.strictEqual(h.v, vAfterManualEnd, '手动结束后旧定时任务不得再产生版本变更');
    assert.strictEqual(h.snapshot.speaking, null);

    // 22f. 撤销开始使定时任务失效（id1 从队列中被请出发言，撤销后回到原位）
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    assert.deepStrictEqual(h.snapshot.order, [id1]);
    await hostCmd(h, { type: 'start_speaking', participantId: id1, durationMin: 30 });
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'start_speaking');
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking === null);
    const vAfterUndoStart = h.v;
    assert.deepStrictEqual(h.snapshot.order, [id1], '撤销开始后发言人回到原队列位置');
    await sleep(30 * SPEAK_TIME_UNIT_MS + 400);
    assert.strictEqual(h.v, vAfterUndoStart, '撤销开始后旧定时任务不得再产生版本变更');

    // 22g. 到期自动结束：version+1、写操作记录、按普通状态变更使挂起撤销失效
    // （start_speaking 本身会武装撤销；随后的自动结束应使其失效）
    await hostCmd(h, { type: 'start_speaking', participantId: id1, durationMin: 1 }); // 50ms
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'start_speaking');
    const vAuto = h.v;
    await h.waitFor((c) => c.snapshot.speaking === null && c.v === vAuto + 1, 3000);
    assert.ok(
      h.snapshot.logs.some((l) => l.kind === 'talk' && /时间到，自动结束/.test(l.text)),
      '到期自动结束应写入操作记录'
    );
    assert.strictEqual(h.snapshot.undo.status, 'invalid', '自动结束应使挂起撤销失效');
    assert.strictEqual(h.snapshot.undo.reason, 'speaking_changed');
    h.lastError = null;
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'UNDO_INVALIDATED');

    // 22h. 断线重连后倒计时继续（endsAt / roundId 不变，剩余时间在轮次内）
    await hostCmd(h, { type: 'start_speaking', participantId: id2, durationMin: 30 });
    const sp3 = h.snapshot.speaking;
    await g2.close(); // 发言人断线：进行中的轮次与倒计时保留
    await h.waitFor((c) => {
      const p = c.snapshot.participants.find((x) => x.id === id2);
      return p && p.online === false;
    });
    assert.ok(h.snapshot.speaking && h.snapshot.speaking.id === id2, '断线不影响进行中的轮次');
    const g2b = new Client();
    await g2b.open();
    g2b.send({
      type: 'join', room: created.room, nick: '计时乙',
      participantId: id2, secret: g2.joined.you.secret
    });
    await g2b.waitFor((c) => c.joined && c.joined.you.id === id2);
    assert.ok(g2b.snapshot.speaking && g2b.snapshot.speaking.id === id2);
    assert.strictEqual(g2b.snapshot.speaking.endsAt, sp3.endsAt, '重连后倒计时终点不变');
    assert.strictEqual(g2b.snapshot.speaking.roundId, sp3.roundId, '重连后轮次标识不变');
    const remain = sp3.endsAt - g2b.snapshot.serverTs;
    assert.ok(remain > 0 && remain <= 30 * SPEAK_TIME_UNIT_MS, `重连后剩余时间应在轮次内（remain=${remain}ms）`);
    await h.waitFor((c) => c.snapshot.speaking === null, 3000); // 该轮到期自动结束
    await g2b.close();
    // 等服务端处理完 g2b 的断线提交（版本稳定）后再发下一条主持人命令
    await h.waitFor((c) => {
      const p = c.snapshot.participants.find((x) => x.id === id2);
      return p && p.online === false;
    });

    // 22i. 撤销「结束发言」恢复限时轮次：endsAt / roundId 还原，定时任务重新排程
    await hostCmd(h, { type: 'start_speaking', participantId: id1, durationMin: 30 });
    const sp4 = h.snapshot.speaking;
    await hostCmd(h, { type: 'end_speaking' });
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === id1);
    assert.strictEqual(h.snapshot.speaking.endsAt, sp4.endsAt, '撤销恢复限时轮次 endsAt 不变');
    assert.strictEqual(h.snapshot.speaking.roundId, sp4.roundId, '撤销恢复限时轮次 roundId 不变');
    await h.waitFor((c) => c.snapshot.speaking === null, 3000); // 重新排程的定时任务到点触发
    assert.ok(h.snapshot.logs.some((l) => /时间到，自动结束/.test(l.text)));

    // 22j. 一键清空使定时任务失效
    await hostCmd(h, { type: 'start_speaking', participantId: id2, durationMin: 30 });
    await hostCmd(h, { type: 'clear' });
    const vAfterClear = h.v;
    await sleep(30 * SPEAK_TIME_UNIT_MS + 400);
    assert.strictEqual(h.v, vAfterClear, '清空后旧定时任务不得再产生版本变更');
    assert.strictEqual(h.snapshot.speaking, null);

    await h.close(); await g1.close();
  }

  // ===========================================================================
  // 队首点名接力（ROLLCALL_TIMEOUT_MS=800ms）
  // ===========================================================================
  {
    const created = await createRoom('点名主持');
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '点甲');
    const g2 = await guestJoin(created.room, '点乙');
    const g3 = await guestJoin(created.room, '点丙');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    const id3 = g3.joined.you.id;

    await waitStable(h, [g1, g2, g3]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    await guestSend(h, g3, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 3);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2, id3]);

    // 23a. 启动：非主持人被拒；主持人启动后快照含 callId / deadline / 已点名成员
    g1.lastError = null;
    await guestSend(h, g1, { type: 'rollcall_start' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');

    h.lastError = null;
    h.send({ type: 'rollcall_start', version: h.v - 1 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
    assert.strictEqual(h.snapshot.rollcall, null, '过期命令不得启动点名');

    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id1);
    const rc1 = h.snapshot.rollcall;
    assert.ok(/^cl_/.test(rc1.callId), '快照需包含 callId');
    assert.strictEqual(rc1.total, 3);
    assert.deepStrictEqual(rc1.called, [id1], '本轮已点名成员需随快照下发');
    assert.deepStrictEqual(rc1.pending, [id2, id3]);
    assert.ok(rc1.deadline > h.snapshot.serverTs, '快照需包含未来的截止时间');
    assert.ok(rc1.deadline - h.snapshot.serverTs <= ROLLCALL_TIMEOUT_MS);
    const callId1 = rc1.callId;

    // 进行中再次启动 -> ROLLCALL_ACTIVE
    h.lastError = null;
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'ROLLCALL_ACTIVE');

    // 非当前对象不能确认/跳过；错误 callId 被拒（version 与 callId 双重校验）
    g2.lastError = null;
    await waitStable(h, [g2]);
    g2.send({ type: 'rollcall_confirm', version: g2.v, callId: callId1 });
    await g2.waitFor((c) => c.lastError && c.lastError.code === 'ROLLCALL_NOT_YOURS');
    g1.lastError = null;
    g1.send({ type: 'rollcall_skip', version: g1.v, callId: 'cl_wrong_call' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'BAD_CALL');
    // 旧版本确认 -> STALE_VERSION（即使 callId 正确）
    g1.lastError = null;
    g1.send({ type: 'rollcall_confirm', version: g1.v - 1, callId: callId1 });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
    assert.strictEqual(h.snapshot.rollcall.callId, callId1, '被拒命令不改变点名状态');

    // 23b. 当前对象主动跳过：移到队尾并自动点名下一人；本轮每人最多被点一次
    await waitStable(h, [g1]);
    g1.send({ type: 'rollcall_skip', version: g1.v, callId: callId1 });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id2);
    assert.deepStrictEqual(h.snapshot.order, [id2, id3, id1], '跳过者移到队尾');
    let rc = h.snapshot.rollcall;
    assert.strictEqual(rc.callId, callId1, '同一轮 callId 不变');
    assert.deepStrictEqual(rc.called, [id1, id2], '已点名集合按点名顺序增长');
    assert.deepStrictEqual(rc.pending, [id3]);
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'rollcall' && /主动跳过/.test(l.text)));

    // 23c. 超时未确认：自动移到队尾并点名下一人（旧定时回调由 roundNo 守卫）
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id3,
      ROLLCALL_TIMEOUT_MS + 1500, 'id2 超时后应自动点名 id3');
    assert.deepStrictEqual(h.snapshot.order, [id3, id1, id2], 'id2 超时移到队尾');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'rollcall' && /超时未确认/.test(l.text)));

    // id3 也超时 -> 三人本轮都已点过 -> 接力结束
    await h.waitFor((c) => c.snapshot.rollcall === null, ROLLCALL_TIMEOUT_MS + 1500);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2, id3], '全部跳过后队列保持轮后的队尾顺序');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'rollcall' && /全部成员均已跳过/.test(l.text)));
    // 旧定时回调不得在结束后再推进任何状态
    const vStable = h.v;
    await sleep(ROLLCALL_TIMEOUT_MS + 300);
    assert.strictEqual(h.v, vStable, '点名结束后旧回调不得产生新版本');

    // 23d. 确认：从队列移除并开始发言，接力在一人确认后结束
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id1);
    const callId2 = h.snapshot.rollcall.callId;
    assert.notStrictEqual(callId2, callId1, '新一轮应有新的 callId');
    await waitStable(h, [g1]);
    g1.send({ type: 'rollcall_confirm', version: g1.v, callId: callId2 });
    await h.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === id1);
    assert.strictEqual(h.snapshot.rollcall, null, '确认后本轮接力结束');
    assert.deepStrictEqual(h.snapshot.order, [id2, id3], '确认者从队列移除');
    assert.ok(h.snapshot.speaking.roundId, '确认后开始发言并产生轮次标识');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'rollcall' && /已确认点名，开始发言/.test(l.text)));
    await hostCmd(h, { type: 'end_speaking' });

    // 23e. 旧定时回调不得推进新一轮：
    // 先启动一轮、等其定时器排上，再取消并立即重启；旧轮回调到期时
    // （第二轮才过了很短时间）不得推进第二轮，第二轮对象必须保持不变。
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id2);
    await hostCmd(h, { type: 'rollcall_cancel' });
    assert.strictEqual(h.snapshot.rollcall, null);
    // 在旧轮定时器到期前 150ms 启动新一轮（旧轮剩余约 ROLLCALL_TIMEOUT_MS）
    await sleep(150);
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id2);
    const callIdB = h.snapshot.rollcall.callId;
    const vAfterRestart = h.v;
    // 等到被取消那一轮的定时器本应触发的时刻（约再等 650ms，小于第二轮 800ms）
    await sleep(ROLLCALL_TIMEOUT_MS - 100);
    assert.ok(h.snapshot.rollcall, '旧轮回调不得结束新一轮');
    assert.strictEqual(h.snapshot.rollcall.callId, callIdB);
    assert.strictEqual(h.snapshot.rollcall.currentId, id2, '旧轮回调不得提前推进新一轮的点名对象');
    assert.strictEqual(h.v, vAfterRestart, '旧轮回调不得产生新版本');
    // 第二轮自身的定时器到期后正常接力（id2 超时 -> id3）
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id3,
      ROLLCALL_TIMEOUT_MS + 1500, '第二轮自身回调应正常推进');
    await hostCmd(h, { type: 'rollcall_cancel' });

    // 23f. 主持人取消点名：队列保持不变，后续可重新启动
    const orderBeforeCancel = h.snapshot.order.slice();
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.snapshot.rollcall);
    await hostCmd(h, { type: 'rollcall_cancel' });
    assert.strictEqual(h.snapshot.rollcall, null);
    assert.deepStrictEqual(h.snapshot.order, orderBeforeCancel, '取消点名不改动队列');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'rollcall' && /取消了队首点名/.test(l.text)));
    h.lastError = null;
    h.send({ type: 'rollcall_cancel', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'NO_ROLLCALL');

    // 23g. 断线重连后恢复当前对象与剩余时间（deadline/callId 不变）
    await hostCmd(h, { type: 'clear' });
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g3, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    assert.deepStrictEqual(h.snapshot.order, [id2, id3]);
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id2);
    const rcLive = h.snapshot.rollcall;
    await g2.close();
    await h.waitFor((c) => {
      const p = c.snapshot.participants.find((x) => x.id === id2);
      return p && p.online === false;
    });
    assert.ok(h.snapshot.rollcall && h.snapshot.rollcall.currentId === id2, '断线不改变点名对象');
    const g2b = new Client();
    await g2b.open();
    g2b.send({ type: 'join', room: created.room, nick: '点乙', participantId: id2, secret: g2.joined.you.secret });
    await g2b.waitFor((c) => c.joined && c.joined.you.id === id2);
    assert.ok(g2b.snapshot.rollcall, '重连后恢复点名状态');
    assert.strictEqual(g2b.snapshot.rollcall.callId, rcLive.callId, '重连后 callId 不变');
    assert.strictEqual(g2b.snapshot.rollcall.currentId, id2, '重连后恢复当前点名对象');
    assert.strictEqual(g2b.snapshot.rollcall.deadline, rcLive.deadline, '重连后截止时间不变');
    const rcRemain = g2b.snapshot.rollcall.deadline - g2b.snapshot.serverTs;
    assert.ok(rcRemain > 0 && rcRemain <= ROLLCALL_TIMEOUT_MS, `重连后恢复剩余时间（${rcRemain}ms）`);
    // 重连后的本人确认可用
    g2b.send({ type: 'rollcall_confirm', version: g2b.v, callId: rcLive.callId });
    await h.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === id2);
    assert.strictEqual(h.snapshot.rollcall, null);
    assert.deepStrictEqual(h.snapshot.order, [id3], '重连确认后从队列移除');
    await hostCmd(h, { type: 'end_speaking' });

    // 23h. 当前对象取消发言：立即推进到下一人（不等待超时）；名单耗尽后接力结束
    // 清空后重建队列 [id1, id2, id3]
    await hostCmd(h, { type: 'clear' });
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2b, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    await guestSend(h, g3, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 3);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2, id3]);

    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id1);
    const callIdH = h.snapshot.rollcall.callId;
    // 让 id1、id2 依次超时跳过，id3 成为当前对象
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id2,
      ROLLCALL_TIMEOUT_MS + 1500);
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id3,
      ROLLCALL_TIMEOUT_MS + 1500);
    assert.deepStrictEqual(h.snapshot.order, [id3, id1, id2]);
    assert.deepStrictEqual(h.snapshot.rollcall.called, [id1, id2, id3]);
    // 当前对象 id3 取消发言 -> 立即结束（不再等待超时）
    await waitStable(h, [g3]);
    g3.send({ type: 'cancel_speak', version: g3.v });
    await h.waitFor((c) => c.snapshot.rollcall === null, 2000, '当前对象取消且名单耗尽 -> 接力结束');
    assert.deepStrictEqual(h.snapshot.order, [id1, id2], '取消者离开队列；跳过者位置不动');
    assert.ok(callIdH.startsWith('cl_'));
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'rollcall' && /无法继续接受点名/.test(l.text)),
      '当前对象取消应记录自动推进');

    // 23i. 空队列 / 有人发言时启动被拒
    await hostCmd(h, { type: 'clear' });
    assert.deepStrictEqual(h.snapshot.order, []);
    h.lastError = null;
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'ROLLCALL_EMPTY');
    await guestSend(h, g3, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await hostCmd(h, { type: 'start_speaking', participantId: id3 });
    h.lastError = null;
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'ROLLCALL_SPEAKING');
    await hostCmd(h, { type: 'end_speaking' });

    // 23j. 点名期间主持人直接请人发言 -> 本轮点名结束
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2b, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    await guestSend(h, g3, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 3);
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id1);
    await hostCmd(h, { type: 'start_speaking', participantId: id3 });
    assert.strictEqual(h.snapshot.rollcall, null, '主持人直接请人发言应结束本轮点名');
    assert.strictEqual(h.snapshot.speaking.id, id3);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2], '被请者从队列移除，未点到者不被移动');
    await hostCmd(h, { type: 'end_speaking' });
    await hostCmd(h, { type: 'clear' });

    // 23k. 当前点名对象主动离开房间 -> 立即推进下一人；旧回调不得干扰
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2b, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    h.send({ type: 'rollcall_start', version: h.v });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id1);
    g1.send({ type: 'leave' });
    await h.waitFor((c) => c.snapshot.rollcall && c.snapshot.rollcall.currentId === id2,
      2000, '当前对象离开后应立即点名下一人');
    assert.deepStrictEqual(h.snapshot.order, [id2], '离开者从队列移除');
    assert.deepStrictEqual(h.snapshot.rollcall.called, [id1, id2]);
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'rollcall' && /无法继续接受点名/.test(l.text)));
    await hostCmd(h, { type: 'rollcall_cancel' });
    await hostCmd(h, { type: 'clear' });

    await h.close(); await g2b.close(); await g3.close();
  }

  // ===========================================================================
  // 议题投票（TOPIC_MIN_SECONDS=1：可用 1 秒议题快速验证到期结算）
  // ===========================================================================
  {
    const created = await createRoom('投票主持');
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '投甲');
    const g2 = await guestJoin(created.room, '投乙');
    const g3 = await guestJoin(created.room, '投丙');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    const id3 = g3.joined.you.id;
    await waitStable(h, [g1, g2, g3]);

    // 24a. 非主持人不能发起议题；过期版本被拒；无议题时结束/投票被拒
    g1.lastError = null;
    await guestSend(h, g1, { type: 'topic_create', title: '甲的议题', durationSec: 60 });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');

    h.lastError = null;
    h.send({ type: 'topic_end', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'NO_TOPIC');
    g1.lastError = null;
    await guestSend(h, g1, { type: 'topic_vote', topicId: 'tp_x', choice: 'yes' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'NO_TOPIC');

    // 标题为空 / 时长非法（0、超上限、非整数、非数字）均拒绝且不改变版本
    for (const badTitle of ['', '   ']) {
      const vBad = h.v;
      h.lastError = null;
      h.send({ type: 'topic_create', version: h.v, title: badTitle, durationSec: 60 });
      await h.waitFor((c) => c.lastError && c.lastError.code === 'NO_TOPIC_TITLE');
      assert.strictEqual(h.v, vBad, '空标题不得改变版本');
    }
    for (const badDur of [0, TOPIC_MIN_SECONDS - 1, 3601, 1.5, 'abc']) {
      const vBad = h.v;
      h.lastError = null;
      h.send({ type: 'topic_create', version: h.v, title: '时长非法', durationSec: badDur });
      await h.waitFor((c) => c.lastError && c.lastError.code === 'BAD_DURATION');
      assert.strictEqual(h.v, vBad, '非法时长不得改变版本');
    }

    // 24b. 主持人发起议题：快照下发 topicId / deadline / 空票数
    const createV = h.v;
    h.send({ type: 'topic_create', version: createV, title: '是否同意周末加班？', durationSec: 60 });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.title === '是否同意周末加班？');
    const topic = h.snapshot.topic;
    assert.ok(/^tp_/.test(topic.id), '快照需包含 topicId');
    assert.ok(topic.deadline > h.snapshot.serverTs, '截止时间应在未来');
    assert.strictEqual(topic.deadline - topic.createdAt, 60000);
    assert.deepStrictEqual(topic.counts, { yes: 0, no: 0, abstain: 0 });
    assert.strictEqual(topic.votedCount, 0);
    assert.deepStrictEqual(topic.votes, []);
    assert.deepStrictEqual(h.snapshot.topicHistory, [], '进行中议题不在已结算列表');
    const topicId1 = topic.id;

    // 24c. 投票：每人一票，实时票数随快照广播；可改投，以最新选择为准
    g1.send({ type: 'topic_vote', version: g1.v, topicId: topicId1, choice: 'yes' });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.counts.yes === 1);
    g2.send({ type: 'topic_vote', version: g2.v, topicId: topicId1, choice: 'no' });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.counts.no === 1);
    g3.send({ type: 'topic_vote', version: g3.v, topicId: topicId1, choice: 'abstain' });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.counts.abstain === 1);
    assert.strictEqual(h.snapshot.topic.votedCount, 3);
    assert.deepStrictEqual(h.snapshot.topic.counts, { yes: 1, no: 1, abstain: 1 });

    // g1 改投：赞成 -> 反对，总票数不变（3），反对变为 2
    await waitStable(h, [g1]);
    g1.send({ type: 'topic_vote', version: g1.v, topicId: topicId1, choice: 'no' });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.counts.no === 2
      && c.snapshot.topic.counts.yes === 0 && c.snapshot.topic.votedCount === 3);
    const g1Vote = h.snapshot.topic.votes.find((v) => v.participantId === id1);
    assert.strictEqual(g1Vote.choice, 'no', '改投后服务端以最新选择为准');

    // 非法选项被拒
    g1.lastError = null;
    g1.send({ type: 'topic_vote', version: g1.v, topicId: topicId1, choice: 'maybe' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'BAD_CHOICE');

    // version 与 topicId 双重校验：旧版本 -> STALE_VERSION；旧议题 id -> BAD_TOPIC
    g1.lastError = null;
    g1.send({ type: 'topic_vote', version: g1.v - 1, topicId: topicId1, choice: 'yes' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
    g1.lastError = null;
    await waitStable(h, [g1]);
    g1.send({ type: 'topic_vote', version: g1.v, topicId: 'tp_wrong_topic', choice: 'yes' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TOPIC');

    // 24d. 主持人发起新议题：旧议题立即按 replaced 提前结算（反对 2 / 赞成 0 / 弃权 1 -> 否决），
    // 新议题票数从零开始；旧议题不出现在当前 topic 而进入 history
    h.send({ type: 'topic_create', version: h.v, title: '第二个议题', durationSec: 60 });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.id !== topicId1);
    const topicId2 = h.snapshot.topic.id;
    assert.strictEqual(h.snapshot.topic.votedCount, 0, '新议题票数从零开始');
    assert.deepStrictEqual(h.snapshot.topic.counts, { yes: 0, no: 0, abstain: 0 });
    assert.strictEqual(h.snapshot.topicHistory.length, 1);
    const settled1 = h.snapshot.topicHistory[0];
    assert.strictEqual(settled1.id, topicId1);
    assert.strictEqual(settled1.title, '是否同意周末加班？');
    assert.strictEqual(settled1.endedBy, 'replaced');
    assert.deepStrictEqual(settled1.counts, { yes: 0, no: 2, abstain: 1 });
    assert.strictEqual(settled1.total, 3);
    assert.strictEqual(settled1.result, 'rejected');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'topic' && /已结算/.test(l.text)));

    // 对旧议题投票 -> BAD_TOPIC（version 即使最新也拒绝）
    g1.lastError = null;
    await waitStable(h, [g1]);
    g1.send({ type: 'topic_vote', version: g1.v, topicId: topicId1, choice: 'yes' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TOPIC');
    assert.strictEqual(h.snapshot.topic.id, topicId2, '对旧议题投票不得影响当前议题');

    // 24e. 主持人提前结束：按 host 结算（赞成 1 -> 通过）；结束后投票被拒
    g1.send({ type: 'topic_vote', version: g1.v, topicId: topicId2, choice: 'yes' });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.counts.yes === 1);
    h.send({ type: 'topic_end', version: h.v });
    await h.waitFor((c) => c.snapshot.topic === null && c.snapshot.topicHistory.length === 2);
    const settled2 = h.snapshot.topicHistory[0];
    assert.strictEqual(settled2.id, topicId2);
    assert.strictEqual(settled2.endedBy, 'host');
    assert.deepStrictEqual(settled2.counts, { yes: 1, no: 0, abstain: 0 });
    assert.strictEqual(settled2.result, 'passed');
    // 最新结算排在最前
    assert.strictEqual(h.snapshot.topicHistory[1].id, topicId1);
    g1.lastError = null;
    await waitStable(h, [g1]);
    g1.send({ type: 'topic_vote', version: g1.v, topicId: topicId2, choice: 'no' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'NO_TOPIC');

    // 24f. 到期自动结算：1 秒议题；赞成=反对 -> 平票(tie)
    h.send({ type: 'topic_create', version: h.v, title: '很快到期的议题', durationSec: TOPIC_MIN_SECONDS });
    await h.waitFor((c) => c.snapshot.topic);
    const topicId3 = h.snapshot.topic.id;
    await waitStable(h, [g1, g2]);
    g1.send({ type: 'topic_vote', version: g1.v, topicId: topicId3, choice: 'yes' });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.counts.yes === 1);
    g2.send({ type: 'topic_vote', version: g2.v, topicId: topicId3, choice: 'no' });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.counts.no === 1);
    const vBeforeDeadline = h.v;
    await h.waitFor((c) => c.snapshot.topic === null
      && c.snapshot.topicHistory[0].id === topicId3
      && c.v === vBeforeDeadline + 1, 3000, '到期自动结算并广播');
    const settled3 = h.snapshot.topicHistory[0];
    assert.strictEqual(settled3.endedBy, 'deadline');
    assert.deepStrictEqual(settled3.counts, { yes: 1, no: 1, abstain: 0 });
    assert.strictEqual(settled3.result, 'tie', '赞成与反对相等应为平票');

    // 24g. 旧定时回调不得结算新议题：先建 1 秒议题，再在其到期前用长议题替换，
    // 旧议题已按 replaced 结算；等旧回调时刻过后，新议题必须仍在进行中
    h.send({ type: 'topic_create', version: h.v, title: '会被替换的短议题', durationSec: TOPIC_MIN_SECONDS });
    await h.waitFor((c) => c.snapshot.topic);
    const shortId = h.snapshot.topic.id;
    h.send({ type: 'topic_create', version: h.v, title: '替换后的长议题', durationSec: 60 });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.id !== shortId);
    const longId = h.snapshot.topic.id;
    assert.strictEqual(h.snapshot.topicHistory[0].id, shortId);
    assert.strictEqual(h.snapshot.topicHistory[0].endedBy, 'replaced');
    const vReplaced = h.v;
    await sleep(TOPIC_MIN_SECONDS * 1000 + 600);
    assert.ok(h.snapshot.topic && h.snapshot.topic.id === longId, '旧议题回调不得结算新议题');
    assert.strictEqual(h.v, vReplaced, '旧回调不得产生新版本');
    assert.strictEqual(h.snapshot.topicHistory.length, 4, '旧回调不得追加结算记录');

    // 24h. 主持人提前结束使定时任务失效：再等超过原 deadline，不得重复结算
    h.send({ type: 'topic_end', version: h.v });
    await h.waitFor((c) => c.snapshot.topic === null);
    const vAfterEnd = h.v;
    await sleep(700);
    assert.strictEqual(h.v, vAfterEnd, '提前结束后旧定时任务不得再产生版本变更');

    // 24i. 断线重连后恢复实时票数与剩余时间（deadline/topicId 不变，计时连续）
    h.send({ type: 'topic_create', version: h.v, title: '断线恢复议题', durationSec: 60 });
    await h.waitFor((c) => c.snapshot.topic);
    const liveTopic = h.snapshot.topic;
    g3.send({ type: 'topic_vote', version: g3.v, topicId: liveTopic.id, choice: 'abstain' });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.counts.abstain === 1);
    await g3.close();
    const g3b = new Client();
    await g3b.open();
    g3b.send({ type: 'join', room: created.room, nick: '投丙', participantId: id3, secret: g3.joined.you.secret });
    await g3b.waitFor((c) => c.joined && c.joined.you.id === id3);
    assert.ok(g3b.snapshot.topic, '重连后恢复当前议题');
    assert.strictEqual(g3b.snapshot.topic.id, liveTopic.id, '重连后 topicId 不变');
    assert.strictEqual(g3b.snapshot.topic.deadline, liveTopic.deadline, '重连后截止时间不变');
    assert.deepStrictEqual(g3b.snapshot.topic.counts, { yes: 0, no: 0, abstain: 1 }, '重连后实时票数恢复');
    const topicRemain = g3b.snapshot.topic.deadline - g3b.snapshot.serverTs;
    assert.ok(topicRemain > 0 && topicRemain <= 60000, `重连后剩余时间应在议题时长内（${topicRemain}ms）`);
    // 重连后本人改投仍可用
    g3b.send({ type: 'topic_vote', version: g3b.v, topicId: liveTopic.id, choice: 'yes' });
    await h.waitFor((c) => c.snapshot.topic && c.snapshot.topic.counts.yes === 1
      && c.snapshot.topic.counts.abstain === 0);

    // 收尾：提前结束，避免遗留定时器
    h.send({ type: 'topic_end', version: h.v });
    await h.waitFor((c) => c.snapshot.topic === null);

    await h.close(); await g1.close(); await g2.close(); await g3b.close();
  }

  // ===========================================================================
  // 累计发言配额（QUOTA_TIME_UNIT_MS=50ms：协议中的 1「分钟」= 50ms）
  // ===========================================================================
  {
    const created = await createRoom('配额主持');
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '额甲');
    const g2 = await guestJoin(created.room, '额乙');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    await waitStable(h, [g1, g2]);

    // 25a. 未开启时快照 quota 为 null；非主持人 / 非法时长 / 过期版本均被拒
    assert.strictEqual(h.snapshot.quota, null, '未开启配额时快照 quota 为 null');
    g1.lastError = null;
    await guestSend(h, g1, { type: 'quota_reset', durationMin: 10 });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');
    for (const bad of [0, 31, 1.5, 'abc']) {
      const vBad = h.v;
      h.lastError = null;
      h.send({ type: 'quota_reset', version: h.v, durationMin: bad });
      await h.waitFor((c) => c.lastError && c.lastError.code === 'BAD_DURATION');
      assert.strictEqual(h.v, vBad, '非法配额时长不得改变版本');
    }
    h.lastError = null;
    h.send({ type: 'quota_reset', version: h.v - 1, durationMin: 10 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
    assert.strictEqual(h.snapshot.quota, null, '过期命令不得开启配额');

    // 25b. 开启配额周期：快照下发 periodId / limitMs / 全员已用与剩余毫秒
    await hostCmd(h, { type: 'quota_reset', durationMin: 30 }); // 30 * 50 = 1500ms
    const q1 = h.snapshot.quota;
    assert.ok(q1 && /^qp_/.test(q1.periodId), '快照需包含周期标识 periodId');
    assert.strictEqual(q1.limitMs, 30 * QUOTA_TIME_UNIT_MS);
    assert.ok(q1.startedAt > 0);
    assert.strictEqual(q1.members[id1].usedMs, 0);
    assert.strictEqual(q1.members[id1].remainingMs, 30 * QUOTA_TIME_UNIT_MS);
    assert.strictEqual(q1.members[id2].remainingMs, 30 * QUOTA_TIME_UNIT_MS);
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'quota' && /配额周期/.test(l.text)));

    // 25c. 每轮发言按实际时长扣减：g1 发言约 250ms 后手动结束
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    // 发言中的快照：已用 + 剩余恒等于周期额度（含未结算的实时部分）
    const liveQ = h.snapshot.quota.members[id1];
    assert.strictEqual(liveQ.usedMs + liveQ.remainingMs, 30 * QUOTA_TIME_UNIT_MS);
    await sleep(250);
    await hostCmd(h, { type: 'end_speaking' });
    const used1 = h.snapshot.quota.members[id1].usedMs;
    assert.ok(used1 >= 150 && used1 <= 700, `按实际发言时长扣减（used=${used1}ms）`);
    assert.strictEqual(h.snapshot.quota.members[id1].remainingMs, 30 * QUOTA_TIME_UNIT_MS - used1);

    // 25d. 手动结束与到期回调竞争只结算一次：手动结束后等待超过到期点，
    // 版本与已用额度都不得再变化
    const vAfterEnd = h.v;
    await sleep(30 * QUOTA_TIME_UNIT_MS + 300);
    assert.strictEqual(h.v, vAfterEnd, '手动结束后配额到期回调不得再产生版本变更');
    assert.strictEqual(h.snapshot.quota.members[id1].usedMs, used1, '配额只结算一次');

    // 25e. 重置新周期：periodId 更新、全员已用清零；耗尽后自动结束并禁止排队
    await hostCmd(h, { type: 'quota_reset', durationMin: 1 }); // 50ms
    const q2 = h.snapshot.quota;
    assert.notStrictEqual(q2.periodId, q1.periodId, '重置应产生新的周期标识');
    assert.strictEqual(q2.members[id1].usedMs, 0, '重置后已用额度清零');
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    const vExh = h.v;
    await h.waitFor((c) => c.snapshot.speaking === null && c.v === vExh + 1, 3000,
      '余额耗应尽快自动结束发言');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'quota' && /配额已用尽/.test(l.text)),
      '额度耗尽自动结束应写入操作记录');
    assert.strictEqual(h.snapshot.quota.members[id1].usedMs, 1 * QUOTA_TIME_UNIT_MS,
      '耗尽时结算为本周期全额');
    assert.strictEqual(h.snapshot.quota.members[id1].remainingMs, 0);
    // 禁止再次排队
    g1.lastError = null;
    await guestSend(h, g1, { type: 'request_speak' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'QUOTA_EXHAUSTED');
    assert.deepStrictEqual(h.snapshot.order, [], '耗尽成员不得进入队列');
    // 主持人也不能再请其发言
    h.lastError = null;
    h.send({ type: 'start_speaking', version: h.v, participantId: id1 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'QUOTA_EXHAUSTED');
    assert.strictEqual(h.snapshot.speaking, null);

    // 25f. 断线重连后继续显示真实余额（周期标识与已用/剩余一致）
    const g1b = new Client();
    await g1b.open();
    g1b.send({ type: 'join', room: created.room, nick: '额甲', participantId: id1, secret: g1.joined.you.secret });
    await g1b.waitFor((c) => c.joined && c.joined.you.id === id1);
    assert.ok(g1b.snapshot.quota, '重连后快照仍含配额');
    assert.strictEqual(g1b.snapshot.quota.periodId, q2.periodId, '重连后周期标识不变');
    assert.strictEqual(g1b.snapshot.quota.members[id1].remainingMs, 0, '重连后显示真实余额（已耗尽）');
    assert.strictEqual(g1b.snapshot.quota.members[id1].usedMs, 1 * QUOTA_TIME_UNIT_MS);

    // 25g. 旧周期定时回调不得扣减新周期 + 重置后原耗尽成员可再次排队：
    // g2 在 5 分钟周期开始发言（到期点 250ms），随即重置为 30 分钟新周期；
    // 旧到期点过后发言必须继续，新周期按重置时刻起的实际时长扣减
    await hostCmd(h, { type: 'quota_reset', durationMin: 5 }); // 250ms 周期
    await hostCmd(h, { type: 'start_speaking', participantId: id2 });
    await hostCmd(h, { type: 'quota_reset', durationMin: 30 }); // 新周期 1500ms；id1 额度随之恢复
    const q3 = h.snapshot.quota;
    assert.notStrictEqual(q3.periodId, q2.periodId);
    assert.strictEqual(q3.members[id1].remainingMs, 30 * QUOTA_TIME_UNIT_MS, '重置后原耗尽成员余额恢复');
    // 原耗尽成员可再次排队
    await guestSend(h, g1b, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1 && c.snapshot.order[0] === id1);
    // 超过旧周期到期点后，新周期的发言必须继续
    await sleep(5 * QUOTA_TIME_UNIT_MS + 300);
    assert.ok(h.snapshot.speaking && h.snapshot.speaking.id === id2, '旧周期回调不得结束新周期的发言');
    await hostCmd(h, { type: 'end_speaking' });
    const settled2 = h.snapshot.quota.members[id2].usedMs;
    assert.ok(settled2 >= 400 && settled2 <= 1400, `新周期按实际发言时长扣减（used=${settled2}ms）`);
    assert.strictEqual(h.snapshot.quota.members[id2].remainingMs, 30 * QUOTA_TIME_UNIT_MS - settled2);

    await h.close(); await g1b.close(); await g2.close();
  }

  // ===========================================================================
  // 行动项（状态机：待接受 -> 处理中 -> 待确认 -> 已完成 / 已拒绝）
  // ===========================================================================
  {
    const created = await createRoom('行动主持');
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '行甲');
    const g2 = await guestJoin(created.room, '行乙');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    await waitStable(h, [g1, g2]);

    // 26a. 非主持人不能创建；空标题 / 非法截止时间 / 不存在的负责人 / 过期版本均被拒
    g1.lastError = null;
    await guestSend(h, g1, { type: 'task_create', participantId: id1, title: '越权', deadline: Date.now() + 3600000 });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');
    assert.strictEqual((h.snapshot.tasks || []).length, 0, '越权创建不得产生行动项');

    const vBad0 = h.v;
    h.lastError = null;
    h.send({ type: 'task_create', version: h.v, participantId: id1, title: '   ', deadline: Date.now() + 3600000 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'NO_TASK_TITLE');
    assert.strictEqual(h.v, vBad0, '空标题不得改变版本');

    for (const bad of [Date.now() - 3600000, Date.now() + 400 * 24 * 3600 * 1000, 'abc', null]) {
      const vBad = h.v;
      h.lastError = null;
      h.send({ type: 'task_create', version: h.v, participantId: id1, title: '非法截止', deadline: bad });
      await h.waitFor((c) => c.lastError && c.lastError.code === 'BAD_DEADLINE');
      assert.strictEqual(h.v, vBad, '非法截止时间不得改变版本');
    }

    h.lastError = null;
    h.send({ type: 'task_create', version: h.v, participantId: 'p_ghost', title: '幽灵负责', deadline: Date.now() + 3600000 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'NO_PARTICIPANT');

    h.lastError = null;
    h.send({ type: 'task_create', version: h.v - 1, participantId: id1, title: '过期命令', deadline: Date.now() + 3600000 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
    assert.strictEqual(h.lastError.snapshot.version, h.snapshot.version, '过期命令必须返回最新快照');
    assert.strictEqual((h.snapshot.tasks || []).length, 0, '过期命令不得创建行动项');

    // 26b. 主持人创建：快照下发行动项（待接受），含首条状态变更记录与操作日志
    const dl = Date.now() + 3600000;
    h.send({ type: 'task_create', version: h.v, participantId: id1, title: '整理会议纪要', deadline: dl });
    await h.waitFor((c) => c.snapshot.tasks && c.snapshot.tasks.length === 1);
    const task1 = h.snapshot.tasks[0];
    assert.ok(/^tk_/.test(task1.id), '快照需包含 taskId');
    assert.strictEqual(task1.status, 'pending');
    assert.strictEqual(task1.title, '整理会议纪要');
    assert.strictEqual(task1.assigneeId, id1);
    assert.strictEqual(task1.assigneeNick, '行甲');
    assert.strictEqual(task1.deadline, dl);
    assert.ok(Array.isArray(task1.history) && task1.history.length === 1, '创建即写入首条状态变更记录');
    assert.strictEqual(task1.history[0].from, null);
    assert.strictEqual(task1.history[0].to, 'pending');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'task' && /创建行动项/.test(l.text)));
    const taskId1 = task1.id;

    // 26c. 身份校验：非负责人（含主持人与其他成员）不能接受；不存在的任务 -> NO_TASK
    g2.lastError = null;
    await guestSend(h, g2, { type: 'task_accept', taskId: taskId1 });
    await g2.waitFor((c) => c.lastError && c.lastError.code === 'TASK_NOT_YOURS');
    h.lastError = null;
    h.send({ type: 'task_accept', version: h.v, taskId: taskId1 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'TASK_NOT_YOURS');
    g1.lastError = null;
    await guestSend(h, g1, { type: 'task_accept', taskId: 'tk_none' });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'NO_TASK');
    assert.strictEqual(h.snapshot.tasks[0].status, 'pending', '被拒命令不得改变状态');

    // 负责人接受 -> 处理中，状态变更记录追加
    await guestSend(h, g1, { type: 'task_accept', taskId: taskId1 });
    await h.waitFor((c) => c.snapshot.tasks[0].status === 'active');
    assert.strictEqual(h.snapshot.tasks[0].history.length, 2);
    assert.strictEqual(h.snapshot.tasks[0].history[1].from, 'pending');
    assert.strictEqual(h.snapshot.tasks[0].history[1].to, 'active');
    assert.strictEqual(h.snapshot.tasks[0].history[1].by, id1);

    // 26d. 状态校验：重复接受 / 处理中重复标记完成的前置状态不符 -> BAD_TASK_STATE（附最新快照）
    g1.lastError = null;
    await guestSend(h, g1, { type: 'task_accept', taskId: taskId1 });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TASK_STATE');
    assert.strictEqual(g1.lastError.snapshot.tasks[0].status, 'active', '错误应附带最新快照');

    // 负责人标记完成 -> 待确认
    await guestSend(h, g1, { type: 'task_complete', taskId: taskId1 });
    await h.waitFor((c) => c.snapshot.tasks[0].status === 'review');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'task' && /标记为完成/.test(l.text)));

    // 待确认状态下负责人重复标记 -> BAD_TASK_STATE（防止多端重复提交）
    g1.lastError = null;
    await guestSend(h, g1, { type: 'task_complete', taskId: taskId1 });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TASK_STATE');

    // 26e. 主持人退回处理中 -> 处理中；再次完成 -> 待确认；确认完成 -> 已完成
    h.send({ type: 'task_return', version: h.v, taskId: taskId1 });
    await h.waitFor((c) => c.snapshot.tasks[0].status === 'active');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'task' && /退回处理中/.test(l.text)));

    // 非主持人不能退回 / 确认
    g1.lastError = null;
    await guestSend(h, g1, { type: 'task_return', taskId: taskId1 });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');
    g1.lastError = null;
    await guestSend(h, g1, { type: 'task_confirm', taskId: taskId1 });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');

    // 主持人不能确认非待确认状态（处理中直接确认属于越权跳转）
    h.lastError = null;
    h.send({ type: 'task_confirm', version: h.v, taskId: taskId1 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TASK_STATE');
    assert.strictEqual(h.snapshot.tasks[0].status, 'active');

    await guestSend(h, g1, { type: 'task_complete', taskId: taskId1 });
    await h.waitFor((c) => c.snapshot.tasks[0].status === 'review');
    h.send({ type: 'task_confirm', version: h.v, taskId: taskId1 });
    await h.waitFor((c) => c.snapshot.tasks[0].status === 'done');
    const hist1 = h.snapshot.tasks[0].history;
    assert.deepStrictEqual(
      hist1.map((x) => x.to),
      ['pending', 'active', 'review', 'active', 'review', 'done'],
      '完整状态流转记录应保留'
    );
    assert.ok(hist1.every((x) => x.ts > 0 && x.by && x.byNick), '每条记录含时间与操作者');

    // 已完成后任何流转都被拒（终态）
    h.lastError = null;
    h.send({ type: 'task_return', version: h.v, taskId: taskId1 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TASK_STATE');
    g1.lastError = null;
    await guestSend(h, g1, { type: 'task_accept', taskId: taskId1 });
    await g1.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TASK_STATE');
    assert.strictEqual(h.snapshot.tasks[0].status, 'done');

    // 26f. 拒绝流程：创建 -> 负责人拒绝 -> 已拒绝（终态，不可再接受）
    h.send({ type: 'task_create', version: h.v, participantId: id2, title: '准备茶水', deadline: Date.now() + 3600000 });
    await h.waitFor((c) => c.snapshot.tasks.length === 2);
    const task2 = h.snapshot.tasks.find((t) => t.title === '准备茶水');
    assert.strictEqual(task2.assigneeId, id2);
    await guestSend(h, g2, { type: 'task_reject', taskId: task2.id });
    await h.waitFor((c) => c.snapshot.tasks.find((t) => t.id === task2.id).status === 'rejected');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'task' && /拒绝了行动项/.test(l.text)));
    g2.lastError = null;
    await guestSend(h, g2, { type: 'task_accept', taskId: task2.id });
    await g2.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TASK_STATE');
    // 负责人也不能标记完成一个未接受的任务
    g2.lastError = null;
    await guestSend(h, g2, { type: 'task_complete', taskId: task2.id });
    await g2.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TASK_STATE');

    // 26g. 断线重连后行动项状态与变更记录完整恢复
    await g1.close();
    await h.waitFor((c) => {
      const p = c.snapshot.participants.find((x) => x.id === id1);
      return p && p.online === false;
    });
    const g1b = new Client();
    await g1b.open();
    g1b.send({ type: 'join', room: created.room, nick: '行甲', participantId: id1, secret: g1.joined.you.secret });
    await g1b.waitFor((c) => c.joined && c.joined.you.id === id1);
    const t1r = g1b.snapshot.tasks.find((t) => t.id === taskId1);
    assert.ok(t1r && t1r.status === 'done', '重连后行动项状态恢复');
    assert.strictEqual(t1r.history.length, 6, '重连后状态变更记录保留');
    assert.strictEqual(t1r.deadline, dl, '重连后截止时间不变');
    const t2r = g1b.snapshot.tasks.find((t) => t.id === task2.id);
    assert.ok(t2r && t2r.status === 'rejected', '重连后已拒绝状态恢复');

    // 26h. 过期命令返回最新快照（多端重复确认防护）：主持人用旧版本确认
    h.lastError = null;
    h.send({ type: 'task_confirm', version: h.v - 1, taskId: taskId1 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
    assert.strictEqual(h.lastError.snapshot.version, h.snapshot.version);
    assert.strictEqual(h.snapshot.tasks.find((t) => t.id === taskId1).status, 'done', '过期命令不得改变状态');

    // 26i. 行动项变更使挂起的安全撤销失效（reason = task_changed）
    await guestSend(h, g1b, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'start_speaking');
    h.send({ type: 'task_create', version: h.v, participantId: id2, title: '写周报', deadline: Date.now() + 3600000 });
    await h.waitFor((c) => c.snapshot.undo && c.snapshot.undo.status === 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'task_changed');
    await hostCmd(h, { type: 'end_speaking' });

    await h.close(); await g1b.close(); await g2.close();
  }

  // 15. 房间不存在
  const ghost = new Client();
  await ghost.open();
  ghost.send({ type: 'join', room: '999999', nick: '幽灵' });
  await ghost.waitFor((c) => c.lastError && c.lastError.code === 'NO_ROOM');

  await host.close();
  await alice.close();
  await bob2.close();
  await carol.close();
  await ghost.close();
  console.log('\n全部测试通过 🎉');
}

function startServer() {
  return new Promise((resolve, reject) => {
    serverProc = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
      env: { ...process.env, PORT: TEST_PORT, SPEAK_TIME_UNIT_MS: String(SPEAK_TIME_UNIT_MS), ROLLCALL_TIMEOUT_MS: String(ROLLCALL_TIMEOUT_MS), TOPIC_MIN_SECONDS: String(TOPIC_MIN_SECONDS), QUOTA_TIME_UNIT_MS: String(QUOTA_TIME_UNIT_MS) },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let settled = false;
    let output = '';
    const onData = (d) => {
      output += String(d);
      if (process.env.SERVER_LOG) process.stderr.write(`[server] ${d}`);
      if (output.includes('已启动') && !settled) {
        settled = true;
        setTimeout(resolve, 100);
      }
    };
    serverProc.stdout.on('data', onData);
    serverProc.stderr.on('data', onData);
    serverProc.on('error', (e) => !settled && (settled = true, reject(e)));
    serverProc.on('exit', () => {
      if (!settled) {
        settled = true;
        reject(new Error(`server exited early:\n${output}`));
      }
    });
  });
}

main()
  .then(() => {
    if (serverProc) serverProc.kill();
    process.exit(0);
  })
  .catch((e) => {
    console.error(e);
    if (serverProc) serverProc.kill();
    process.exit(1);
  });
