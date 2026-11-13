# Project 1 - Core Azure Pipeline

## 1. Overview
This project builds a small data pipeline on Azure for one public dataset: about 110 million click events from an online store, collected over October and November 2019 (Kaggle, mkechinov/ecommerce-behavior-data-from-multi-category-store). The raw files are about 14 GB of CSV. The goal is to load them into a data lake, clean them, and produce business tables that answer questions about traffic, funnel conversion and revenue, with one pipeline that runs everything in order.

## 2. Architecture

![architecture](architecture.png-1.png)

Follow one file through the system. The October CSV is uploaded once to the source container. Azure Data Factory copies it, byte for byte, into bronze/ecommerce/year=2019/month=10/. A Databricks notebook reads it from bronze, enforces types, removes bad and duplicate rows, and writes the clean events as a Delta table to silver. A second notebook reads silver and writes five small business tables, plus one reference table, to gold. Data Factory runs the three steps in order from one pipeline, pl_ecommerce_main.

## 3. Design Choices
### Partitioning
- Bronze: by year and month. 
    The files are monthly, so each month sits in its own folder. One month can be found, reloaded or removed without touching the other.
- Silver: by event_date. 
    Most questions are asked per day. The table has about 61 date folders with roughly 1.8 million rows each, a healthy size. Partitioning by user_id would create millions of tiny folders.
- Gold: not partitioned. 
    The tables are small, a few thousand rows at most. Splitting them into folders would make reads slower.

### File formats
- Bronze: original CSV, copied as binary. 
    A Binary dataset moves the file as raw bytes, so bronze stays an exact copy. The sizes in bronze equal the sizes in source.
- Silver and gold: 
    Delta. Delta stores compressed Parquet with real types, so prices are numbers and timestamps are timestamps. The transaction log records each change, so a write is complete or not done at all.

### Cleaning rules

| Column | Rule | Reason |
|---|---|---|
| `category_code` | Empty becomes `unknown` | The store did not categorize the product. The event is still valid. |
| `brand` | Empty becomes `unknown` | The brand was not recorded. The event still counts. |
| `price` | Rejected if empty, 0 or below | Revenue needs a real price. Filling with 0 would distort averages. |
| `event_time`, `product_id`, `user_id` | Rejected if empty or not convertible | Without them we cannot say who did what, to which product, and when. |
| `event_type` | Rejected if not view, cart, remove_from_cart or purchase | Only the four known event types are valid. |
| Duplicates | One row per key: event_time, event_type, product_id, user_id, user_session | The key identifies one event. |

*Rejected rows are kept in `silver/ecommerce_rejected` with a reason, not deleted, so the losses can be counted.

## 4. Gold Tables
| Table | Grain | Question it answers | Answer in one line |
|---|---|---|---|
| `daily_summary` | One day | How do events, purchases and revenue change per day? | `We have table with daily summuries` |
| `category_funnel` | One category | How many views turn into carts and purchases per category? | `For this we have view_to_cart_rate, cart_to_purchase_rate, view_to_purchase_rate ` |
| `revenue_by_category_brand` | One month, category and brand | Which categories and brands earn the most? | `electronics apple` |
| `hourly_traffic` | One hour of the day (UTC) | When is traffic highest? | Peak hour: `16` |
| `product_performance` | One product with at least 100 views | Which products are viewed a lot but rarely bought? | `1004856 samsung view_to_purchase_rate 6.5` |

## 5. Results
### Row counts (Day 3)
- Raw (bronze): 109 950 743
- Rejected: 256 761 (all with reason `bad_price`)
- Duplicates removed: 130 646
- Silver: 109 563 336
- Check: 256 761 + 130 646 + 109 563 336 = 109 950 743. No row is lost.
### Run times
- Full pipeline run (`pl_ecommerce_main`, manual trigger): 41 min 48 s, Succeeded
- Gold step alone (`run_gold`): 13 min 31 s
- Bronze copy and silver step: `...` (from the Monitor run details)
### Reproducibility check (Day 5)
| Check | Earlier value | Second run | Same? |
|---|---|---|---|
| Silver rows | 109 563 336 | 109 563 336| yes |
| Rejected rows | 256 761 | 256 761 | yes |
| Gold total events | 109 563 336 | 109 563 336 | yes |
| Gold total revenue | 505 126 293.44 | 505 126 293.44  | yes |

## 6. Problems and Fixes

1. **Storage account name taken.** `stbigdatadev001` was already used by another account in Azure, so the account was created as `stbigdatadev00001`. Names of storage accounts are global, so any project needs a name that is free. The check was done before the first command with `az storage account check-name`.

2. **Databricks Standard tier is no longer accepted.** The workspace creation failed with `DatabricksStandardSkuNotSupported`, so the workspace was created with Premium. The cost of the workspace itself is the same, and the notebooks run the same way. The cost comes from cluster time, which is why clusters are set to stop after 20 idle minutes.

3. **Pipeline parameter had the wrong format.** Publishing failed because the `files` parameter contained a whole pipeline JSON instead of an array. The value was replaced with the array alone.

## 7. Limits and Next Steps
- Reloads all data on every run (incremental loading next)
- No retries or alerts yet
- Storage key is used by Databricks (to improve)
- Code not yet in GitHub