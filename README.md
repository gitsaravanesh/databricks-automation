# bronze_silver

A Spark Declarative Pipeline that loads Parquet files from a landing folder into
**bronze** (raw, as-is) and **silver** (cleaned, one current row per key), packaged
as a Declarative Automation Bundle with sample data, tests and GitHub Actions CI/CD.

```
landing/incoming/<table>/*.parquet  ──Auto Loader──▶  bronze.<table>  ──AUTO CDC──▶  silver.<table>
   (temporary landing area)                            raw + lineage cols           latest row per key
landing/archive/<table>/   ◀── optional: processed files moved here (clean_source = MOVE)
```

## Repo layout

```
bronze-silver/
├── databricks.yml                      # bundle: variables + dev / test / prod targets
├── resources/
│   ├── bronze_silver.pipeline.yml      # the pipeline definition
│   └── bronze_silver.job.yml           # daily job that runs the pipeline
├── src/bronze_silver/                  # pipeline root folder
│   ├── transformations/                # pipeline source code (every file here is loaded)
│   │   ├── bronze.py                   # 10 bronze streaming tables (Auto Loader)
│   │   └── silver.py                   # cleaning views + AUTO CDC / de-duplication
│   └── utilities/                      # imported by transformations
│       ├── tables.py                   # the table list: keys, CDC or append, SCD type
│       └── settings.py                 # pipeline settings + Auto Loader options
├── sample_data/                        # 3 batches of CDC Parquet files + EXPECTED.md
├── scripts/
│   ├── make_sample_data.py             # regenerates sample_data/
│   ├── upload_batch.ps1                # upload a batch to the landing volume (Windows)
│   └── upload_batch.sh                 # same, macOS / Linux / Git Bash
├── tests/                              # pytest: config, settings, sample data
├── .github/workflows/ci-cd.yml         # CI/CD (GitHub Actions)
└── pyproject.toml                      # pytest + ruff settings
```

## The 10 tables

| Folder | Key | Type | Silver |
|---|---|---|---|
| stores | store_id | CDC | SCD 1 |
| customers | customer_id | CDC | **SCD 2** (history) |
| products | product_id | CDC | **SCD 2** (history) |
| orders | order_id | CDC | SCD 1 |
| order_items | order_id, line_no | CDC | SCD 1 |
| payments | payment_id | CDC | SCD 1 |
| shipments | shipment_id | CDC | SCD 1 |
| returns | return_id | CDC | SCD 1 |
| inventory | product_id, store_id | CDC | SCD 1 |
| web_events | event_id | append-only | de-duplicated |

CDC files carry `op` (c / u / d) and `mdb_commit_ts` (commit time). To add a table,
add one line to `utilities/tables.py` and put its files in `incoming/<folder>/`.

---

## Run it on Databricks Free Edition (dev)

### 1. One-time setup on your laptop

Install the Databricks CLI (Windows: `winget install Databricks.DatabricksCLI`;
macOS: `brew tap databricks/tap && brew install databricks`), then log in:

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks current-user me
```

Put the same URL in `databricks.yml` under `targets.dev.workspace.host`.

### 2. Create the schemas and the landing volume

In the workspace's SQL editor:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.dev_bronze;
CREATE SCHEMA IF NOT EXISTS workspace.dev_silver;
CREATE VOLUME IF NOT EXISTS workspace.dev_bronze.landing;
```

(In a shared workspace, use your own name instead of `dev` here and in `databricks.yml`.)

### 3. Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
```

This creates the pipeline `[dev <you>] bronze_silver` and the job
`[dev <you>] bronze_silver_job` (schedule paused in dev).

### 4. Batch 1: the initial load

Upload the files into the temporary landing area, then run the job:

```powershell
.\scripts\upload_batch.ps1 -Batch 1          # Windows
```
```bash
./scripts/upload_batch.sh 1                  # macOS / Linux
```
```bash
databricks bundle run -t dev bronze_silver_job
```

Every table loads for the first time. Open the pipeline in the UI to see the graph:
10 bronze tables feeding 10 silver tables.

### 5. Batch 2 and batch 3: incremental changes

Repeat with `-Batch 2`, run the job, then `-Batch 3`, run the job. Each run reads
**only the new files**. Compare with `sample_data/EXPECTED.md` after each batch:

- **Updates and deletes:** order 1 goes PLACED → SHIPPED → DELIVERED; order 5 disappears.
- **Missing key:** an `orders` row with no `order_id` stays in bronze but is dropped
  from silver. The pipeline's data quality tab shows 1 dropped record.
- **Out-of-order event:** batch 3 brings an older "CANCELLED" for order 2. It's
  ignored because its commit time is older than the "SHIPPED" already applied.
- **History (SCD 2):** customer 1 ends with 3 versions; customer 4 is closed, not removed.
- **Duplicates:** `web_events` has 26 rows in bronze but 24 in silver.

### 6. Optional: move processed files out of the landing area

To empty `incoming/` once files are loaded, deploy with:

```bash
databricks bundle deploy -t dev --var="clean_source=MOVE" --var="archive_after=1 hour"
```

Auto Loader then moves each processed file to `landing/archive/<table>/` once it is
older than `archive_after`. Moving happens during later runs that have new files, and
a file becomes eligible two runs after it was loaded at the earliest. It needs a
recent runtime; if the pipeline rejects the option, leave `clean_source` at `OFF`.
Because files may leave the landing area, bronze has `pipelines.reset.allowed = false`
so a full refresh can't empty it.

### 7. Clean up

```bash
databricks bundle destroy -t dev
```
```sql
DROP SCHEMA IF EXISTS workspace.dev_silver CASCADE;
DROP SCHEMA IF EXISTS workspace.dev_bronze CASCADE;
```

---

## Tests

```bash
pip install pytest ruff pyarrow pandas
ruff check src tests scripts
pytest -q
```

The tests check the table list (unique folders, keys, valid SCD types), the settings
and Auto Loader options, and that every sample file has the columns the pipeline needs.
To regenerate the sample data: `python scripts/make_sample_data.py`.

## CI/CD (GitHub Actions)

One workflow, `.github/workflows/ci-cd.yml`, with three jobs:

| When | Jobs that run |
|---|---|
| Pull request to `main` | **checks** (ruff + pytest) → **deploy-test** (validate, deploy, run the job once as a smoke test) |
| Push / merge to `main` | **checks** → **deploy-prod** (waits for approval, then validate and deploy) |
| Manual (Actions → CI/CD → Run workflow) | **checks** → deploy to the target you pick: test or prod |

Setup:
1. Create a Databricks service principal per environment, with access to the
   workspace, the catalog and schemas, and `READ FILES` on the landing external
   location, plus an OAuth secret.
2. Put its application ID in `targets.<env>.run_as` and the workspace URL in
   `targets.<env>.workspace.host` in `databricks.yml`.
3. In GitHub, go to **Settings → Environments** and create `test` and `production`.
   In each, add the secrets `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`
   for that environment's service principal.
4. On `production`, add **Required reviewers** (this is the approval step) and set
   **Deployment branches** to `main` only, so nothing else can deploy to prod.

Notes:
- The test target is shared, so the workflow lets only one test deploy run at a
  time, and the last pull request deployed is what's in test.
- Pull requests from forks don't receive secrets, so their test deploy fails; this
  setup is meant for a private repo where branches live in the same repo.
- In test and prod, the schemas, external locations and grants are expected to exist
  already (typically created with Terraform); the bundle deploys only the pipeline and job.

### Optional: no stored secret (workload identity federation)

Instead of storing an OAuth secret, GitHub can prove its identity to Databricks
with a short-lived token. In the workflow, for each deploy job:

```yaml
permissions:
  id-token: write
  contents: read
env:
  DATABRICKS_AUTH_TYPE: github-oidc
  DATABRICKS_HOST: https://<workspace>.cloud.databricks.com
  DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}   # not secret anymore: use a variable
```

An account admin then adds a federation policy to the service principal that trusts
this repo's environment:

```bash
databricks account service-principal-federation-policy create <service-principal-numeric-id> --json '{
  "oidc_policy": {
    "issuer": "https://token.actions.githubusercontent.com",
    "audiences": ["<databricks-account-id>"],
    "subject": "repo:<org>/<repo>:environment:production"
  }
}'
```

The subject must match GitHub's token exactly; repositories created or renamed
recently use a format with numeric IDs, so check GitHub's OIDC docs for yours.

## Exercises

1. Add an 11th table: a folder of Parquet files plus one line in `tables.py`.
2. Switch `orders` to SCD 2 and compare the history after all three batches
   (needs a full refresh of `silver.orders` to rebuild it).
3. Add a new column to a batch-4 file and watch the stream pick up the schema change.
4. Set `continuous: true` in the pipeline YAML, remove the job, and drop files in
   while it runs.
