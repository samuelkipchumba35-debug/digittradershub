# Deriv Over 0 + Triggered Over 1 Recovery Bot

## Strategy
- Configured symbol: `1HZ30V` (verify availability with Deriv's active-symbols API).
- Base trade: Digit Over 0.
- After a loss, pause base trades and monitor incoming ticks.
- Two consecutive tick events whose last digit is either 0 or 1 trigger one Digit Over 1 recovery trade.
- A recovery win returns to base mode. A loss resumes monitoring for a new qualifying pair.
- Take-profit, stop-loss, and maximum consecutive-loss limits are configurable.
- Recovery stake is fixed by default; this is not a Martingale multiplier.

## Important safety notes
- **Start on a Deriv demo account.** The API token selects the account; a real-account token can place real trades.
- Never commit an API token or app secret. Use environment variables in the hosting provider's secret settings.
- This code has not been validated against a live/demo Deriv session here. Verify contract availability, quote precision, and API behavior before use.
- Trading can lose money. Stop-loss controls do not guarantee protection from every failure or execution issue.

The bot stops after a WebSocket disconnect or an uncertain purchase/API error so that you can reconcile account transactions before restarting. A production deployment should add durable state storage and more complete contract-subscription recovery.

## Requirements
- Node.js 20+
- Deriv application ID
- Deriv demo API token with the permissions required for trading

## Setup
1. Open this directory in a terminal.
2. Run `npm install`.
3. Set the environment variables below, then run `npm start`.

### Linux/macOS
```bash
export DERIV_APP_ID="YOUR_APP_ID"
export DERIV_API_TOKEN="YOUR_DEMO_TOKEN"
export BASE_STAKE="0.50"
export RECOVERY_STAKE="1.00"
export TAKE_PROFIT="5"
export STOP_LOSS="5"
export MAX_CONSECUTIVE_LOSSES="3"
npm start
```

### Windows PowerShell
```powershell
$env:DERIV_APP_ID="YOUR_APP_ID"
$env:DERIV_API_TOKEN="YOUR_DEMO_TOKEN"
$env:BASE_STAKE="0.50"
$env:RECOVERY_STAKE="1.00"
$env:TAKE_PROFIT="5"
$env:STOP_LOSS="5"
$env:MAX_CONSECUTIVE_LOSSES="3"
npm start
```

## Hosting
This is a long-running Node.js WebSocket process, not a DBot-importable XML file or static website. Host it as a worker/service that supports persistent processes and outbound WebSockets (for example, a VPS or a compatible worker host). Vercel static hosting alone is not suitable for running this loop continuously.

## Files
- `bot.js`: strategy and Deriv WebSocket API logic
- `package.json`: Node.js package metadata and start command
