# Analytics Reports

The reports in this directory were generated from a full end-to-end run on **1,052,394** Bronze records (which became **986,468** Silver records after deduplication/cleaning).

## Date Range
The dataset includes historical data from:
- August 2024
- June 2025 to June 2026

## Reproducibility Commands
To reproduce these results, the following commands were run sequentially using the Makefile:

`ash
make backfill     # Fetch historical data to Bronze
make clean-silver # Deduplicate and clean to Silver
make mapreduce    # Run MapReduce aggregates
make gold         # Run Spark aggregates
make validate     # Cross-validate MR vs Spark
make analytics    # Generate these reports & train K-Means (k=5)
`

## Highlights
- **Data Quality**: The validation phase passed with 0 mismatches between Spark and MapReduce.
- **Machine Learning**: K-Means clustering with k=5 yielded a Silhouette Score of **0.50**.