# Project notes: Week 3 (eCommerce behavior data)

## Part A: Warm-up

1. Difference between `source` and `bronze`:
   - source: `Lives outside or at the boundary`
   - bronze: `Lives directly inside your Data Lake storage`
2. Why we never edit bronze: `Bronze is meant to be an exact, bit-for-bit replica (or raw snapshot) of your original source data at the moment it was ingested`
3. Why not `pandas.read_csv` on a laptop: `Using pandas.read_csv() on a laptop works great for small to medium-sized datasets, but it hits a hard wall when working with big data.`
- Upload speed (Mbps / 8 = MB/s): `20 Mbps / 8 = 2.5 MB/s`
- Estimated time for 14 GB at that speed: `14 GB * 1024 = 14336 MB; 14336 MB / 2.5 MB/s = 5734.4 seconds;  5734.4 seconds / 60 = 95,57 minutes = 1 hour, 35 minutes, and 34 seconds`

## Part B: Plan

### Dataset (Task 3)
- Files: `2019-Oct.csv` (5 668 612 855 bytes), `2019-Nov.csv` (9 006 762 395 bytes)
- Columns: event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
- Messy columns: `category_code` and `brand` (empty values seen in the first rows of 2019-Oct.csv, for example brand is empty on the third row, category_code is empty on the first row)


### Architecture (Task 5)
- File: `architecture.png`

## Part C: Resources (Week 3 setup)

| Resource | Name | Region |
|---|---|---|
| Resource group | rg-bigdata-dev | East US |
| Storage account (ADLS Gen2) | stbigdatadev00001 | East US |
| Data Factory | adf-bigdata-dev-1234 | East US |
| Databricks workspace | dbw-bigdata-dev | East US (Premium SKU) |
| Key Vault | `kv-bigdata-dev-1234` (or new name, fill in) | East US |

- Containers: source, bronze, silver, gold
- Budget alert active: yes 

## Part D: Data

- Downloaded with Kaggle CLI on: `03.11.2026 13:43:03`
- Verified against the Azure size list: yes

## Part E: Upload (Task 12)

| File | Local bytes | Azure bytes | Match |
|---|---|---|---|
| ecommerce/2019-Oct.csv | 5668612855 | 5668612855 | yes |
| ecommerce/2019-Nov.csv | 9006762395 | 9006762395 | yes |

## Issues and decisions

- Storage account name became `stbigdatadev00001` because `stbigdatadev001` was taken.
- Data Factory name became `adf-bigdata-dev-1234` because `adf-bigdata-dev` was taken.
- Databricks: Standard SKU is not supported, used Premium.
- Storage account TLS minimum was TLS1_0, changed to TLS1_2.
- Upload used `--auth-mode key` as agreed (not the recommended identity login).

## Week 3, Day 2: Bronze

### Part A: Warm-up (Task 1)
 
1. Linked service vs dataset: a linked service is the saved connection to a system (which storage account, how to sign in). A dataset is a pointer to specific data inside that connection (which container, folder and file).
2. Binary dataset for bronze: it copies the file as raw bytes without reading its content, so bronze stays an exact copy and nothing (quotes, line endings, number formats, empty values) is changed.
3. Managed identity vs storage account key: the managed identity is an identity Azure gives Data Factory, with no password to store, leak or rotate. A storage account key is a password with full access to the whole account.
4. Idempotent: running the pipeline once or several times gives the same result. Re-running overwrites the same paths instead of creating new copies.

### Part B: Bronze layout (Task 2)
 
Bronze paths (container `bronze`):
- `ecommerce/year=2019/month=10/2019-Oct.csv`
- `ecommerce/year=2019/month=11/2019-Nov.csv`
Why partition by year and month: the files are monthly. Each month is its own folder, so one month can be found, reloaded or removed without touching the other, and Spark can read `year` and `month` from the folder names and skip folders it does not need.
 
Why not convert to another format in bronze: bronze must stay an exact copy of what the source sent, so that any cleaning rule can be fixed later and silver and gold rebuilt from the original bytes. Conversion (for example to Parquet) is done in silver.

### Pipeline
- Name: `pl_ingest_bronze`
- Linked service: `ls_adls_bigdata` (managed identity, Storage Blob Data Contributor)
- Dataset: `ds_adls_binary` (Binary, parameters: container, directory, fileName)
- Copy type: binary, no compression

### Run results (Task 12, from Monitor)
- Run ID: `b4d49302-e98d-4882-9da8-84b0e07edd4d`
- Status: `Succeeded`
- Files copied: 2
- Duration: `1m 22s`


### Size check
| File | source bytes | bronze bytes | Match |
|---|---|---|---|
| ecommerce/2019-Oct.csv | 5668612855 | 5668612855 | yes |
| ecommerce/2019-Nov.csv | 9006762395 | 9006762395 | yes |


### Failure test (Task 15)
- Failed activity: copy_to_bronze inside foreach_file
- Error in plain words: `Azure don't see file in the folder` (the file 2012-Oct.csv does not exist in source/ecommerce, Azure returned NotFound)
- Where to check the cause: `ecommerce files and  Pipeline -> Parameters -> files -> Default value` (Monitor, the run, the activity error; the file list in source)
- Did bronze change? `no` (no 2012 folder was created)



# Week 3, Day 3:

## Silver (results)
 
### Cleaning rules (final)
 
| Column | Rule | Reason |
|---|---|---|
| category_code | Empty -> `unknown` | The store did not categorize the product. Filling keeps the row, and `main_category` becomes `unknown` for these rows, which is visible in gold. |
| brand | Empty -> `unknown` | The brand was not recorded. The product is still valid for counting events and revenue. |
| price | Reject if empty, 0 or below | Revenue and averages need a real price. Filling with 0 would make averages wrong, so the row is quarantined instead. |
| event_time, product_id, user_id | Reject if empty or not convertible | Without these we cannot say who did what, to which product, and when. |
| event_type | Reject if not view, cart, remove_from_cart or purchase | Only the four known event types are valid. Anything else breaks the funnel. |
| Duplicates | Key: event_time, event_type, product_id, user_id, user_session | This key identifies one event. Keeping one row per key removes repeated records of the same event. |
 
### Partitioning
- Column: `event_date` (date of `event_time`)
- Why: almost every business question is "per day" or "per week". About 61 folders, each about 1.8 million rows, is a balanced size. `user_id` would create millions of tiny folders and would not match the questions.
- Small-files control: `repartition("event_date")` before writing, so each date folder gets about one file.
### Why Delta and not CSV
- CSV keeps every value as text and has no types, so the notebook has to re-create them every time.
- Delta stores data as compressed Parquet with real types, which makes the table much smaller and faster to read.
- The `_delta_log` transaction log records every change, so a write is either complete or not done at all, and older versions can be read back.
### Results (Task 16 to 18)
- Raw (bronze): 109 950 743
- Rejected: 256 761 (all reason `bad_price`)
- Duplicates removed: 130 646
- Silver: 109 563 336
- Check: 256 761 + 130 646 + 109 563 336 = 109 950 743, no rows lost
- Duplicate keys in silver: 0
- Date range: 2019-10-01 to 2019-11-30
- Price range in silver: 0.77 to 2574.07
### What surprised me in the data
- All 256 761 rejected rows failed on `bad_price`. Next step: check whether these are NULL, 0 or negative (`rejected_df.filter("price = 0").count()` etc.) and write the decision here.
- `remove_from_cart` does not appear in silver. Next step: check the raw event_type values in bronze and write the finding here.
- 12 rows in silver have an empty `user_session`, which is part of the duplicate key.
- Pending: the full null check across all columns, and the decision for the 12 empty `user_session` rows.

## Week 3, Day 4: Gold (results)

### Grain table (final)
| Gold table | Grain | Business question |
|---|---|---|
| daily_summary | one day | Which activity we have each day?|
| category_funnel | one category | Which category are strongest / weakest part of business?|
| revenue_by_category_brand | one month, category, brand | Which category and brand have the biggest pie of total sales ?|
| hourly_traffic | one hour of day (UTC) | How many events per hour we have ? |
| product_performance | one product (views >= 100) | Which product are the most popular ?|

### Answers to business questions (Task 14)
- Top category by revenue: electronics 381,695,319 $
- Top brand by revenue (excluding unknown): apple 238,712,020 $
- Viewed a lot, rarely bought: 1004856 samsung 
- Busiest hour (UTC): 16 123620 average event per day


### OPTIMIZE (Task 11 and 12)
| Table | Files before | Files after |
|---|---|---|
| daily_summary | 1 | 1 |
| category_funnel | 1 | 1 |
| revenue_by_category_brand | 1 | 1 |
| hourly_traffic | 1 | 1 |
| product_performance | 1 | 1 |
| ref_category_group | 1 | 1 |

ZORDER: product_id in product_performance, because queries go by product.

### Pipeline run
- gold step duration (Task 19): 13m 31s
- Status: Succeeded