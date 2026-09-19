# transactions-case

## Known data quirks

- `transactions.payment_id` is not a stable identifier: some rows share the same `payment_id` but have `timestamp` values many months apart, which doesn't match a single payment's normal lifecycle (auth/capture/retry happening within minutes or days). Don't treat a matching `payment_id` as proof of the same real-world payment — `uuid` is the reliable per-row primary key and is what silver uses.