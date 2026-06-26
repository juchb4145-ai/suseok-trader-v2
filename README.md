# suseok-trader-v2

Broker-neutral foundation for a Kiwoom OpenAPI+ based domestic stock trading system.

The project currently contains the Core API bootstrap, settings loading, SQLite initialization,
and PR 1 broker contract models. It intentionally does not contain Gateway transport, Kiwoom or
PyQt imports, strategy decisions, risk policy execution, or live order APIs.

## Broker Contract

Broker/Core contracts live under `domain/broker`:

- `events.py`: `GatewayEvent` and `EventEnvelope` for Gateway-to-Core observations.
- `commands.py`: `GatewayCommand` and `CommandEnvelope` for Core-to-Gateway instructions.
- `market.py`: `BrokerPriceTick` for normalized real-time market ticks.
- `orders.py`: `BrokerOrderRequest`, `BrokerOrderResult`, and `BrokerExecutionEvent`.
- `tr.py`: `BrokerTrRequest` and `BrokerTrResponse`.
- `conditions.py`: `BrokerConditionEvent`.
- `utils.py`: UTC timestamp generation, message ID generation, dictionary serialization helpers,
  payload normalization, and integer/float parsing helpers.

The contracts serialize through `to_dict()` and reconstruct through `from_dict()`. They preserve
idempotency keys, auto-generate event/command IDs when omitted, normalize broker-style numeric
strings, and reject unsafe values such as invalid stock codes, non-positive quantities, and
invalid limit prices.

See `docs/event_contract.md` for field-level details.

## Local Checks

```powershell
python -m pytest
python -m ruff check .
```
