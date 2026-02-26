# Civil 3D Bridge Stub

Execution model:

- Payloads queued in `ConcurrentQueue<TopologicPayload>`.
- Processing marshaled via `Application.ExecuteInCommandContextAsync`.
- Drawing writes protected by `DocumentLock` and `Transaction`.

Primary outputs:

- ALTA Item 20 table insertion command.
- Compliance notifier when `RPP_actual > RPP_allowable`.
