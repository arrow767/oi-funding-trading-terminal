# Exchange reference notes

Per-venue endpoint table with the units and rate-limit parameters the
adapters encode. Verify before changing adapter defaults.

| Exchange     | Discovery                                       | OI endpoint                                                     | Unit      | Price endpoint                                  | Rate limit (public)     |
|--------------|-------------------------------------------------|-----------------------------------------------------------------|-----------|-------------------------------------------------|-------------------------|
| Binance USDM | `GET /fapi/v1/exchangeInfo`                     | `GET /fapi/v1/openInterest?symbol=…`                            | coins     | `GET /fapi/v1/premiumIndex`                     | 2400 weight/min/IP      |
| Bybit        | `GET /v5/market/instruments-info?category=linear` | `GET /v5/market/open-interest?category=linear&symbol=…&intervalTime=5min` | coins     | `GET /v5/market/tickers?category=linear`        | 600 req/5s/IP           |
| OKX          | `GET /api/v5/public/instruments?instType=SWAP`  | `GET /api/v5/public/open-interest?instType=SWAP`                | contracts | `GET /api/v5/market/tickers?instType=SWAP`      | 20 req/2s/IP            |
| BingX        | `GET /openApi/swap/v2/quote/contracts`          | `GET /openApi/swap/v2/quote/openInterest?symbol=…`              | coins     | `GET /openApi/swap/v2/quote/premiumIndex`       | 100 req/10s/IP          |
| KuCoin       | `GET /api/v1/contracts/active`                  | `GET /api/v1/contracts/{symbol}` (field: openInterest)          | contracts | `GET /api/v1/ticker?symbol=…`                   | "Resource pool" model   |
| MEXC         | `GET /api/v1/contract/detail`                   | `GET /api/v1/contract/ticker` (batch; field: holdVol)           | contracts | same batch (field: lastPrice)                   | 20 req/s/IP             |
| Bitget       | `GET /api/v2/mix/market/contracts?productType=USDT-FUTURES` | `GET /api/v2/mix/market/open-interest?symbol=…&productType=USDT-FUTURES` | coins     | `GET /api/v2/mix/market/tickers?productType=USDT-FUTURES` | 20 req/s/IP             |
| Hyperliquid  | `POST /info {"type":"meta"}`                    | `POST /info {"type":"metaAndAssetCtxs"}` (field: openInterest)  | coins     | same call (field: markPx)                       | 1200 req/min/IP         |
| Aster        | `GET /fapi/v1/exchangeInfo` (binance-parity)    | `GET /fapi/v1/openInterest?symbol=…`                            | coins     | `GET /fapi/v1/premiumIndex`                     | tbd — verify docs       |

### Unit-detection logic (in code)

`InstrumentMeta.native_unit` + `contract_multiplier` are the canonical
source of truth. `UnitKind::to_coins` and `UnitKind::to_usd` in
`oi-core` are total functions over those two fields — they don't guess.

### Open questions to verify during each adapter's build-out

* **Bybit** 1-minute OI is NOT published. `intervalTime=5min` is the
  finest; collector polls every minute and takes the latest 5-minute
  bar, accepting that four minutes of each five will show a repeat. When
  adding WS, the `tickers` channel pushes `openInterest` on every
  trade — that becomes the fresher source.
* **KuCoin** returns OI in contracts on the single-symbol detail
  endpoint and has no batch endpoint — expect ~200 requests per minute.
* **Hyperliquid** `metaAndAssetCtxs` is one POST for the whole universe
  — cheapest by far; there's no reason to poll per-symbol.
