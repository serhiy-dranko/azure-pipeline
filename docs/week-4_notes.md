# Week 4 notes

## Day 1: Incremental loading

### Part A: Warm-up

1. **Why a full load is a problem as data grows:** every run reads and rewrites all the data, so run time and cost grow with the total size, even when only one day is new.
2. **Safe to re-run:** running the same job again for the same date gives the same result as running it once, with no duplicate rows.
3. **Why the MERGE match includes `event_date`:** silver is partitioned by `event_date`, so Delta only has to look at the partition for that date, not the whole table.

### What changed

- Pipeline `pl_ecommerce_main` has a parameter `load_date` (default `2019-10-01`), passed to the silver notebook.
- Silver notebook reads the widget `load_date`, filters the cleaned data to that date, and writes with MERGE instead of a full overwrite.
- MERGE key: `event_date`, `event_time`, `event_type`, `product_id`, `user_id`, `user_session`.
- The old full-overwrite write for silver was removed.

### Re-run test (Task 5)

| Run | Date | Rows |
|---|---|---|
| Baseline (before MERGE) | 2019-10-01 | 1 241 586 |
| Baseline (before MERGE) | 2019-10-02 | 1 189 032 |
| First MERGE | 2019-10-01 | 1 241 586 |
| Repeated MERGE | 2019-10-01 | 1 241 586 |
| Repeated run | 2019-10-02 | 1 189 032 |

Result: running the same date again does not duplicate rows, and the other date is unchanged.

Not tested: inserting a new date. All October dates were already in silver, and deleting data just to test was not done.

### Time

- MERGE run time: `6 min` 

### Known limits (to fix later this week)

- `run_bronze` still copies both files on every run; the date parameter does not filter the copy.
- `run_gold` still rebuilds gold from the full silver table.
- `ecommerce_rejected` is still written with overwrite and is not yet part of the MERGE logic.
- Gold needs the same date-based approach before the pipeline is fully incremental.

## Day 2: Reliability
 
### Part A: Warm-up
1. Retries fix temporary problems, such as a slow cluster start or a short network drop. They do not fix bugs in code or wrong data.
2. A Fail activity keeps the run marked as Failed and gives a clear message. Without it, a handled failure can show the whole run as Succeeded.
3. A run can succeed and still be wrong, because no error only means the steps finished, not that the numbers are correct.

### What changed
- Retry: 2, Retry interval: 60 seconds on the pipeline activities (Validate and Publish passed).
- Silver notebook checks: raw count per date, removed count, and a silver count after MERGE. Raises an error if there is no data or the counts differ.
- Gold notebook check: `daily_summary` events for the date must equal the silver row count for that date.
- Fail activity `fail_at_silver_level` on the red (On failure) path from `run_silver`, with message `Silver step failed for <load_date>` and error code `SILVER_FAILED`. Validate and Publish passed.

### Check results (2019-10-01)
```
[silver] load_date=2019-10-01 raw=1244245 removed=2659 kept=1241586
[silver] rows in silver for 2019-10-01: 1241586
[gold] load_date=2019-10-01 daily_summary events=1241586 silver rows=1241586
```
Note: `removed` = rejected + duplicates, same as the Week 3 report.
 
### Alert and failure tests
- Alert rule `alert-adf-failed-runs` (action group `ag-adf-alerts`, email): created
- Failing test, `load_date = 2019-12-01`: run Failed as expected, retries seen, alert email received: yes
- Good run, `load_date = 2019-10-03`: Succeeded, counts printed

### Screenshots saved for Friday
- Failed run in Monitor: saved
- Alert email: saved


## Day 3: Secrets and GitHub
 
### Part A: Warm-up
1. Secrets in this project: the storage account key, the Kaggle API token, and any Databricks token. A key is a secret if it grants access.
2. A managed identity has no key to store, leak or rotate, so there is nothing to steal.
3. Least privilege means giving each identity only the access it needs, at the smallest scope.

### Part B: Audit
- silver_ecommerce and gold_ecommerce: no secret values in code. The storage key is read with `dbutils.secrets.get(scope, "storage-account-key")`. The word "key" in the notebook matches only the secret name and the `fs.azure.account.key` setting.
- Known limit: Databricks still reaches storage with the account key (from Key Vault). Identity-based access is a later improvement.
- Linked services: `ls_adls_bigdata` and `ls_databricks` both use System-assigned managed identity.
- Project folder: no `.env` file and no tokens found by search (`KGAT`, `AccountKey=`).

### Part C: Key Vault
- The storage key is stored in `kv-bigdata-dev-01111` as `storage-account-key`. The tasks named `kv-bigdata-dev-001` and `storage-key`; the project uses the existing vault and secret name.
- Kaggle token is not stored in any vault: it was used only from an environment variable for the download and is not needed after that.

### Part D: Managed identity for Data Factory
- Role Storage Blob Data Contributor on `stbigdatadev00001` is assigned to the Data Factory identity (objectId 95752934-...).
- Linked service `ls_adls_bigdata` uses System-assigned managed identity. Test connection and a pipeline run were checked.

### Part E: GitHub
- Repo `azure-pipeline` created (private, github.com/serhiy-dranko/azure-pipeline).
- ADF Git integration failed: Internal Server Error, then Bad credentials. OAuth re-authorization did not open.
- Status: postponed. Workaround: push `adf/`, `notebooks/` and the architecture files with git from the project folder.

## Day 4: Serve and Measure
 
### Part A: Warm-up
1. An external table is a table name that points to Delta files in storage. The data stays in place.
2. Many small files slow reads, because Spark has to open each file.
3. Run each query twice so the second run is measured without cache warm-up.

### Part B: Query the gold layer
- Unity Catalog: an external location is missing, so tables cannot be registered from the storage path. The legacy `hive_metastore` is disabled for this workspace (UC_HIVE_METASTORE_DISABLED_EXCEPTION).
- Workaround used: the gold tables were published as managed tables in the catalog `dbw_bigdata_dev_1234`, schema `ecommerce`: `gold_daily_summary`, `category_funnel`, `revenue_by_category_brand`, `hourly_traffic`, `product_performance`, `ref_category_group`.
- Business questions (in `sql/queries.sql`):
  1. Which categories earn the most revenue? Answer: `electronics 381 695 319.29`
  2. Which brands earn the most revenue (excluding unknown)? Answer: `apple 238 712 020.58`
  3. Which products are viewed a lot but rarely bought? Answer: product 1004856 (samsung), view-to-purchase 6.5%.
  4. Busiest hours (UTC)? Answer: 16:00.

### Part C: Timing one change
| Query | Before OPTIMIZE | After OPTIMIZE |
|---|---|---|
| silver count per day | 1st 7 s, 2nd 4 s | 1st 10 s, 2nd 7 s |
 
OPTIMIZE on silver/ecommerce_events: 61 partitions considered, 61 skipped, 0 files added, 0 files removed. Each date partition already holds one large file (written with `repartition("event_date")` in Week 3), so there was nothing to compact. The change in time (4 s to 7 s) is measurement noise, not an effect of OPTIMIZE.
 
### Part D: Cost per run (one full run, 41 min 48 s; gold step 13 min 31 s)

- VM Standard_D4s_v3: $0.192 per hour (Pay as you go, East US, Linux).
- Databricks Premium, All-Purpose, East US: $0.413 per hour for 0.75 DBU, total with VM $0.605 per hour. Cluster UI shows 1.5 DBU per hour for single node, which gives a higher estimate.
- Full run: about $0.42 (page rate) to $0.71 (cluster UI rate).
- Gold step: about $0.14 to $0.23.
- Data Factory: data movement 0.140 DIU-hours at $0.25 = $0.035; orchestration about 6 activity runs = $0.006. Total about $0.04 per full run.
- Storage (Hot, LRS, East US, $0.0208 per GB per month): 47.91 GB, about $1.00 per month, about $0.03 per day. Silver is larger than bronze because Spark writes int64 and float64 types.
- Monitor: one alert rule, one metric time series, inside the 10 free series per month: $0.
- Event Hubs: not used in this project.

### Part E: Actual spend (Cost analysis, rg-bigdata-dev, Nov 2026, EUR)
- Databricks dbw-bigdata-dev-1234: €10.39. Higher than the one-run estimate, because the cluster ran longer than one pipeline run (debugging, repeated runs, auto-terminate wait).
- Data Factory adf-bigdata-dev-1234: €0.52.
- Storage stbigdatadev00001: €0.59 (estimate about €1 per month; read and write operations add to it).
- Key Vault kv-bigdata-dev-01111: under €0.01.
- Monitor alert and action groups: €0.00.
- Log Analytics law-bigdata: €0.00.
- Event Hubs ehns-wiki-001: €13.07 (wiki-pipeline, not part of this project).
- App Service func-wiki-pipeline-001: €4.60 (wiki-pipeline).
- Storage stbigdataproject001: €3.68 (Week 1 storage).

Conclusion: the estimate for one run is correct in order of magnitude. Actual Databricks cost is higher because clusters run longer than the pipeline. Stopping clusters after work and keeping auto-terminate at 20 minutes is the main cost control.
