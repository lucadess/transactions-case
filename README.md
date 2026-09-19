# transactions-case

A Databricks medallion-architecture pipeline (staging → bronze → silver → gold) that pulls
customer, transaction, and operation-status-code data from a paginated REST API, lands it as
raw JSON, cleans/types it, and produces a transaction-level gold fact table with EUR-converted
amounts.

## 1. Assumptions made

- **One gold table, at transaction grain.** The gold layer currently produces a single
  `transactions` fact table joining `transactions`, `customers`, and `operation_status_code`.
  No other dimension/fact tables were built (see section 2).
- **`uuid` is the primary key for `transactions`, not `payment_id`.** Some rows share the same
  `payment_id` but have `timestamp` values many months apart — not consistent with a single
  payment's normal lifecycle (auth/capture/retry happening within minutes or days). Since
  `payment_id` doesn't reliably identify a single real-world payment, `uuid` is used as the
  row-level primary key everywhere (silver dedup, gold join), and `payment_id` is kept as a
  plain grouping column instead.
- **ECOM transactions are identified via `customers.acquire`.** A value of `"YYEcom"` marks an
  ECOM merchant; these are excluded from gold. There was no other column/convention in the
  source data for this, so it was clarified directly rather than guessed.
- **Currency conversion uses a single "latest rate" per run.** Amounts are converted to EUR
  using the free [Frankfurter API](https://www.frankfurter.app/), fetched once per gold run and
  applied to every transaction in that currency, regardless of the transaction's own timestamp.
  See section 2 for the more accurate alternative.
- **Test merchants are excluded from gold** using `customers.test_merchant`, and only
  "successful, operational" transactions are kept: `operation_status_code == 2` and
  `operation_result_code == '00'`.
- **Transactions join to customers via `merchant_id`**, not a per-transaction `customer_id` —
  there is no such column on `transactions`. This assumes each `merchant_id` maps to exactly
  one `customers` row; if a merchant ever had multiple customer records, the join would fan out.
- **Bronze does no transformation.** It lands the raw API response as-is (schema-on-read),
  exploding the `items` array into rows and dropping only the pagination envelope
  (`limit`/`offset`/`total`). All typing, casting, and cleansing happens in silver.
- **`overwrite` write mode everywhere, no merge/upsert**, i.e. every run does a full reload of
  every table rather than an incremental/SCD update. See section 2.
- **Full load from the source API, not incremental.** Every run re-pulls and reprocesses the
  complete dataset rather than only new/changed transactions since the last run. In a production
  scenario, the source would more realistically expose incremental transactions, which could be
  merged/upserted into existing tables instead of overwritten.
- **Unity Catalog, one schema per medallion layer, single catalog** (`<catalog>.staging`,
  `<catalog>.bronze`, `<catalog>.silver`, `<catalog>.gold`), configured in `configs/settings.yml`.
- **API key lives in a Databricks secret scope**, not in the repo. Scope/key names
  (`flatpay-case` / `api-key`) are placeholders — create the actual scope before running the
  staging job (see section 3).
- **Each API page is staged as its own JSON file in a UC volume**, rather than accumulated in
  memory and written once at the end. If a staging run fails partway through, rerunning it skips
  every page already written and only fetches what's missing, instead of reprocessing the whole
  endpoint from scratch.
- **The Databricks environment used (free tier) only has serverless compute**, which has no
  writable local/driver disk. This is why staging writes go straight to a UC volume rather than
  to local disk first — there is no local disk available to write to.

## 2. What I'd do differently or add with more time

- **Use the transaction's own date for FX conversion**, pulling the historical rate at that
  timestamp instead of a single "latest rate" applied to the whole run. Ideally this would also
  be ingested as its own table so exchange rate history is stored and can be joined against
  later, rather than re-fetched live every run.
- **Spend more time on test design** — better parameterize the test suite per table, with input
  and expected data driven from a shared structure, instead of the current mix of explicit and
  parametrized tests.
- **Start a proper dimensional model** for gold (not required by the current scope, but needed
  for scalability): `dim_date`, `dim_customers`, `fact_transactions`, etc., instead of a single
  flat fact table.
- **Don't just overwrite everything.** Slowly-changing data like customers would benefit from
  SCD1 (or SCD2, depending on requirements) so the latest state is always retained correctly
  across incremental loads, instead of a full overwrite every run.
- **Better schema evolution.** Right now, a schema change (new/renamed/removed column) has to be
  handled manually via a SQL command against the table — there's no automated migration path.
- **Deploy schemas and jobs with Databricks Asset Bundles (DAB)** instead of the current manual
  process (creating catalog schemas by hand, importing each job YAML through the Jobs UI). A DAB
  setup would define both as code and make environment setup reproducible via a single
  `databricks bundle deploy`.

## 3. How to run

### Local development / tests

1. Create a virtualenv and install dependencies: `pip install -r requirements.txt` (also needs
   `pytest`, already listed).
2. A local JDK is required to run the PySpark/Delta tests (`tests/`). Point `JAVA_HOME` at one,
   e.g. via Homebrew on macOS:
   ```bash
   export JAVA_HOME="$(/opt/homebrew/bin/brew --prefix openjdk@17)"
   export PATH="$JAVA_HOME/bin:$PATH"
   ```
   Add these two lines to your shell profile (`~/.zshrc`, etc.) so new terminals pick them up
   automatically.
3. Run the suite: `pytest` (config already points `pythonpath` at `src` and `tests` via
   `pytest.ini`).

### Databricks setup

1. **Connect the repo** as a Databricks Repo (or Git folder) pointing at this GitHub repository.
2. **Create the API secret scope**, then add the key:
   ```bash
   databricks secrets create-scope flatpay-case
   databricks secrets put-secret flatpay-case api-key
   ```
   (Or via the Databricks UI: Settings → Developer → Secrets.) Update the scope/key names in
   `src/staging/notebook/extract.py` if you use different ones.
3. **Import each job**: Databricks UI → Workflows → Jobs → Create Job → kebab menu → *Import
   from YAML* → paste the contents of `jobs/staging_job.yml`, `jobs/bronze_job.yml`,
   `jobs/silver_job.yml`, `jobs/gold_job.yml` (one job each). Each job's `git_source.git_branch`
   currently points at `main` — update it if you're running off a different branch.
4. **Run order matters**: `staging_job` → `bronze_job` → `silver_job` → `gold_job`. Each layer
   reads from the previous one's tables, so run (or schedule) them in that sequence.
5. For an ad-hoc single-table run instead of the full job, open the relevant
   `src/<layer>/notebook/<name>.py` notebook directly in Databricks and set its `table_name`
   widget.

## 4. Project structure

### `configs/`

Per-layer YAML config: table definitions (`configs/<layer>/tables.yml`) and shared settings
(`configs/settings.yml` — catalog name, per-layer schema names, API settings).

### `jobs/`

One Databricks Job YAML per layer (`staging_job.yml`, `bronze_job.yml`, `silver_job.yml`,
`gold_job.yml`), importable via the Jobs UI. Each job has one task per table, running that
layer's notebook with a different `table_name` parameter.

### `src/staging/` — resumable API ingestion

- **`extract.py`**
  - `ingest_table_to_volume(client, endpoint, staging_dir, limit)` — paginates one endpoint,
    skips offsets already staged, and writes each page **atomically** (`.tmp` then
    `os.rename`) so a crash mid-write never leaves a corrupt page.
- **`notebook/extract.py`** — Databricks entry point; ingests one endpoint per run
  (`table_name` widget).

### `src/bronze/` — raw landing into Delta, no business logic

- **`ingest.py`**
  - `ingest_data(spark, table_name)` — reads a table's staged JSON, explodes it, writes to the
    bronze Delta table. Pure schema-on-read, no casting or filtering.
  - `explode_json_array(df)` — flattens the `items` array into one row per record, dropping the
    `limit`/`offset`/`total` pagination columns.

### `src/silver/` — cleansing

- **`clean.py`**
  - `clean_data(spark, table_name)` — reads the bronze table, casts every column to its
    declared type, drops null-primary-key rows, deduplicates on the primary key, writes to
    silver.

### `src/gold/` — business-level aggregation

- **`gold_generators.py`**
  - `GoldGenerator` (ABC) — the template every gold table follows:
    - `transform()` — abstract; each subclass implements its own reads/joins/filters.
    - `load(df)` — shared: casts to the config schema and writes.
    - `generate()` — entry point, `load(transform())`.
- **`factory.py`**
  - `get_gold_generator(spark, table_name)` — returns the `GoldGenerator` registered for a
    table.
- **`generators/transactions.py`**
  - `TransactionsGenerator.get_transactions()` / `.get_customers()` /
    `.get_operation_status_code()` — each reads one of the three silver sources.
  - `TransactionsGenerator.transform()` — joins the three, filters to
    operational/non-ECOM/non-test-merchant transactions, adds `amount_eur`.
- **`fx_rates.py`**
  - `fetch_eur_rates()` — calls Frankfurter, returns a `{currency: EUR multiplier}` dict.

### `src/utils/` — shared building blocks used across every layer

- **`api_client/`**
  - `APIClient.fetch_page(endpoint, limit, offset)` — fetches one page, self-throttled, with
    retry/backoff on `429`/`5xx`.
- **`config_manager/`**
  - `get_config(path)` — loads and parses one YAML file into a dict.
- **`reader/`**
  - `get_reader(spark, source_path)` — picks a `Reader` for a source path (currently JSON only).
  - `JSONReader.read(spark)` — reads every `*.json` file in a directory into one DataFrame.
- **`writer/`**
  - `get_writer(spark, table, write_mode, ...)` — builds a configured `DeltaTableWriter`.
  - `DeltaTableWriter.write(df)` — creates the table if missing, validates its schema matches
    exactly (no silent evolution), then writes.
  - `create_table(spark, table, df=None, table_config=None)` — creates a Delta table from a
    DataFrame's schema (bronze) or a config's column list (silver/gold).
  - `apply_schema(df, columns)` / `get_primary_key(columns)` — select+cast a DataFrame to a
    config's columns, and derive the primary key from the `primary_key` tag.

### `tests/`

Mirrors `src/` by layer. `tests/fixtures/` holds config-independent test fixtures (a dedicated
`tests/fixtures/settings.yml`, plus per-layer input/expected Python data modules and, for
staging/bronze, real JSON page files) so tests never depend on or mutate the real `configs/`
directory. `tests/conftest.py` provides the shared local Spark+Delta session and
schema-isolation fixtures used by every Spark-backed test.
