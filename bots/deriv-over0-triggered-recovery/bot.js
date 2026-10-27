/**
 * Deriv Volatility 30 (1s) Digit Over 0 + triggered Over 1 recovery bot.
 * Run on DEMO first. Requires Node.js 20+ and `ws`.
 *
 * Environment:
 *   DERIV_APP_ID=your_app_id
 *   DERIV_API_TOKEN=your_demo_token
 *   BASE_STAKE=0.50
 *   RECOVERY_STAKE=1.00
 *   TAKE_PROFIT=5
 *   STOP_LOSS=5
 *   MAX_CONSECUTIVE_LOSSES=3
 *
 * This is an automated trading example, not financial advice.
 */
'use strict';

const WebSocket = require('ws');

const APP_ID = process.env.DERIV_APP_ID;
const TOKEN = process.env.DERIV_API_TOKEN;
if (!APP_ID || !TOKEN) {
  console.error('Set DERIV_APP_ID and DERIV_API_TOKEN environment variables.');
  process.exit(1);
}

const CFG = {
  symbol: '1HZ30V',
  currency: 'USD',
  baseStake: Number(process.env.BASE_STAKE ?? 0.50),
  recoveryStake: Number(process.env.RECOVERY_STAKE ?? 1.00),
  takeProfit: Number(process.env.TAKE_PROFIT ?? 5.00),
  stopLoss: Number(process.env.STOP_LOSS ?? 5.00),
  maxConsecutiveLosses: Number(process.env.MAX_CONSECUTIVE_LOSSES ?? 3),
  duration: 1,
  durationUnit: 't',
  appId: APP_ID,
  token: TOKEN,
};

for (const [k, v] of Object.entries({
  baseStake: CFG.baseStake,
  recoveryStake: CFG.recoveryStake,
  takeProfit: CFG.takeProfit,
  stopLoss: CFG.stopLoss,
  maxConsecutiveLosses: CFG.maxConsecutiveLosses,
})) {
  if (!Number.isFinite(v) || v <= 0) throw new Error(`Invalid ${k}: ${v}`);
}

const STATE = {
  mode: 'BASE',
  under2Count: 0,
  consecutiveLosses: 0,
  sessionProfit: 0,
  activeContractId: null,
  buying: false,
  stopped: false,
  monitoredTickCount: 0,
  pipSize: null,
  tickSubscriptionId: null,
};

let ws;
let nextReqId = 1;
const pending = new Map();
const contractSubscriptions = new Map();
const settledContracts = new Set();

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function stop(reason) {
  if (STATE.stopped) return;
  STATE.stopped = true;
  STATE.mode = 'STOPPED';
  log(`BOT STOPPED: ${reason}`);
}

function riskLimitReached() {
  if (STATE.sessionProfit >= CFG.takeProfit) return 'take-profit reached';
  if (STATE.sessionProfit <= -CFG.stopLoss) return 'stop-loss reached';
  if (STATE.consecutiveLosses >= CFG.maxConsecutiveLosses) return 'maximum consecutive losses reached';
  return null;
}

function send(payload, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return reject(new Error('WebSocket is not connected'));
    const req_id = nextReqId++;
    const timer = setTimeout(() => {
      pending.delete(req_id);
      reject(new Error(`API request timed out: ${Object.keys(payload)[0]}`));
    }, timeoutMs);
    pending.set(req_id, { resolve, reject, timer });
    ws.send(JSON.stringify({ ...payload, req_id }));
  });
}

function decimalPlacesFromPipSize(pipSize) {
  const n = Number(pipSize);
  return Number.isInteger(n) && n >= 0 && n <= 10 ? n : null;
}

function lastDigit(quote, pipSize) {
  const places = decimalPlacesFromPipSize(pipSize);
  if (places === null) throw new Error(`Invalid quote precision/pip_size: ${pipSize}`);
  const normalized = Math.round(Number(quote) * (10 ** places));
  return Math.abs(normalized) % 10;
}

function handleMessage(msg) {
  if (msg.error) {
    const p = msg.req_id != null ? pending.get(msg.req_id) : null;
    if (p) {
      clearTimeout(p.timer);
      pending.delete(msg.req_id);
      p.reject(new Error(`${msg.error.code}: ${msg.error.message}`));
    } else {
      log('API error:', msg.error.code, msg.error.message);
    }
    return;
  }

  if (msg.req_id != null && pending.has(msg.req_id)) {
    const p = pending.get(msg.req_id);
    clearTimeout(p.timer);
    pending.delete(msg.req_id);
    p.resolve(msg);
  }

  if (msg.msg_type === 'tick') {
    onTick(msg.tick).catch(err => {
      log('Tick handler error:', err.message);
      stop('internal tick handler error');
    });
  }

  if (msg.msg_type === 'proposal_open_contract') {
    onContractUpdate(msg.proposal_open_contract);
  }
}

function connect() {
  return new Promise((resolve, reject) => {
    ws = new WebSocket(`wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(CFG.appId)}`);
    const connectTimeout = setTimeout(() => reject(new Error('Connection timed out')), 15000);
    ws.once('open', async () => {
      clearTimeout(connectTimeout);
      try {
        const auth = await send({ authorize: CFG.token });
        log('Authorized. Account:', auth.authorize?.loginid, 'Currency:', auth.authorize?.currency);
        const symbols = await send({ active_symbols: 'brief', product_type: 'basic' });
        const found = (symbols.active_symbols || []).find(s => s.symbol === CFG.symbol);
        if (!found) throw new Error(`Symbol ${CFG.symbol} not available. Check active_symbols and update CFG.symbol.`);
        log('Using symbol:', found.symbol, found.display_name || '');
        const tickResp = await send({ ticks: CFG.symbol, subscribe: 1 });
        STATE.tickSubscriptionId = tickResp.subscription?.id || null;
        log('Tick stream subscribed. Bot is running in the account selected by API token.');
        resolve();
      } catch (e) {
        reject(e);
      }
    });
    ws.on('message', raw => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }
      handleMessage(msg);
    });
    ws.on('error', err => log('WebSocket error:', err.message));
    ws.on('close', () => {
      log('WebSocket closed. Bot will stop safely; reconcile contract state manually.');
      stop('WebSocket disconnected; manual reconciliation required');
    });
  });
}

async function buyDigitOver(barrier, stake, label) {
  if (STATE.stopped || STATE.buying || STATE.activeContractId) return;
  const limit = riskLimitReached();
  if (limit) return stop(limit);

  STATE.buying = true;
  try {
    log(`Requesting ${label}: Digit Over ${barrier}, stake ${stake} ${CFG.currency}`);
    const proposalResp = await send({
      proposal: 1, amount: stake, basis: 'stake', contract_type: 'DIGITOVER',
      currency: CFG.currency, duration: CFG.duration, duration_unit: CFG.durationUnit,
      symbol: CFG.symbol, barrier: String(barrier),
    });
    if (!proposalResp.proposal?.id) throw new Error('Proposal response did not contain proposal.id');

    const buyResp = await send({ buy: proposalResp.proposal.id, price: stake });
    const contractId = buyResp.buy?.contract_id;
    if (!contractId) throw new Error('Buy response did not contain contract_id');

    STATE.activeContractId = contractId;
    log(`PURCHASED ${label}. Contract ID: ${contractId}`);
    const sub = await send({ proposal_open_contract: 1, contract_id: contractId, subscribe: 1 });
    if (sub.proposal_open_contract) onContractUpdate(sub.proposal_open_contract);
    if (sub.subscription?.id) contractSubscriptions.set(contractId, sub.subscription.id);
  } catch (e) {
    log('Purchase failed:', e.message);
    stop('purchase/API error; inspect account transactions before restarting');
  } finally {
    STATE.buying = false;
  }
}

function onContractUpdate(c) {
  if (!c || !c.is_sold) return;
  const settledId = String(c.contract_id);
  if (settledContracts.has(settledId)) return;
  if (STATE.activeContractId && settledId !== String(STATE.activeContractId)) return;
  settledContracts.add(settledId);

  const profit = Number(c.profit ?? 0);
  const status = String(c.status || '').toLowerCase();
  STATE.sessionProfit += profit;
  log(`SETTLED contract=${c.contract_id} status=${status} profit=${profit.toFixed(2)} session=${STATE.sessionProfit.toFixed(2)}`);

  if (profit > 0) {
    STATE.consecutiveLosses = 0;
    STATE.under2Count = 0;
    if (STATE.mode !== 'BASE') log('Winning contract; resetting to base mode.');
    STATE.mode = 'BASE';
  } else {
    STATE.consecutiveLosses += 1;
    STATE.under2Count = 0;
    STATE.mode = 'MONITOR';
    STATE.monitoredTickCount = 0;
    log(`Loss recorded. Monitoring for two consecutive digits 0 or 1. Loss streak=${STATE.consecutiveLosses}`);
  }

  STATE.activeContractId = null;
  const limit = riskLimitReached();
  if (limit) return stop(limit);
}

async function onTick(tick) {
  if (!tick || STATE.stopped) return;
  if (tick.pip_size != null) STATE.pipSize = tick.pip_size;
  if (STATE.pipSize == null) {
    log('Waiting for tick precision metadata (pip_size).');
    return;
  }
  if (STATE.buying || STATE.activeContractId) return;
  const limit = riskLimitReached();
  if (limit) return stop(limit);

  if (STATE.mode === 'BASE') {
    await buyDigitOver(0, CFG.baseStake, 'BASE');
    return;
  }

  if (STATE.mode === 'MONITOR') {
    let digit;
    try { digit = lastDigit(tick.quote, STATE.pipSize); }
    catch (e) { log(e.message); stop('could not determine last digit safely'); return; }

    STATE.monitoredTickCount += 1;
    log(`MONITOR tick #${STATE.monitoredTickCount} digit=${digit}, consecutive-under-2=${STATE.under2Count}`);
    if (digit === 0 || digit === 1) {
      STATE.under2Count += 1;
      log(`Qualifying tick (${digit}); count=${STATE.under2Count}/2`);
    } else {
      STATE.under2Count = 0;
    }

    if (STATE.under2Count === 2) {
      STATE.under2Count = 0;
      log('TRIGGER CONFIRMED: two consecutive digits 0 or 1. Buying Over 1 recovery.');
      await buyDigitOver(1, CFG.recoveryStake, 'RECOVERY');
    }
  }
}

async function main() {
  log('Starting triggered recovery bot:', {
    symbol: CFG.symbol, baseStake: CFG.baseStake, recoveryStake: CFG.recoveryStake,
    takeProfit: CFG.takeProfit, stopLoss: CFG.stopLoss,
    maxConsecutiveLosses: CFG.maxConsecutiveLosses,
  });
  await connect();
}

main().catch(err => {
  log('Fatal startup error:', err.message);
  stop('startup failure');
  if (ws) ws.close();
  process.exitCode = 1;
});
