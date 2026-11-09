# Kalshi fixtures

`orderbook_snapshot.json` and `orderbook_delta.json` are public market-data
messages captured by the read-only access preflight on 2026-11-06. Their original
application receive timestamps and file hashes are in `provenance.json`.
They contain no authentication headers, private keys, or account/portfolio data.

The snapshot intentionally omits its empty YES side. Its NO bids must become
YES asks at `1_000_000 - no_price`. The delta carries a signed quantity change;
the SBE output must contain the resulting absolute quantity.

Other malformed/gap/decimal cases in the tests are synthetic and are not claimed
as exchange captures.
