# Analytics Reports

The reports in this directory were generated from a full end-to-end run on **1,052,394** Bronze records (which became **986,468** Silver records after deduplication/cleaning).

## Date Range
Backfill windows (see `data_quality/`):
- 2024-08-01 to 2026-06-30: 1,052,394 Bronze -> 986,468 Silver rows
- 2026-07-01 to 2026-07-31: 100,425 Bronze -> 100,049 Silver rows

## Reproducibility Commands
To reproduce these results, the following commands were run sequentially using the Makefile:

```bash
make backfill     # Fetch historical data to Bronze
make clean-silver # Deduplicate and clean to Silver
make mapreduce    # Run MapReduce aggregates
make gold         # Run Spark aggregates
make validate     # Cross-validate MR vs Spark
make analytics    # Generate these reports & train K-Means (k=5)
```

## Highlights
- **Data Quality**: The validation phase passed with 0 mismatches between Spark and MapReduce.
- **Machine Learning**: K-Means clustering with k=5 yielded a Silhouette Score of **0.50** (0.499, `ml/k_selection.csv`).
