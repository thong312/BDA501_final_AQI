# FINAL PRESENTATION · SESSION 10 DEMO
## AQI Analytics Platform
**Monitoring, analyzing and alerting on New York City air quality**

* **COURSE:** BDA501 — Big Data Analytics
* **ASSESSMENT:** Final Project (40% of grade)
* **TEAM / CLASS:** Team 7 — MAS23
* **LECTURER:** Assoc.Prof.Dr.techn DANG Tran Khanh
* **MEMBERS:** Dương Thành Duy · Lý Minh Thông · Lê Công Huỳnh
* **DATE:** Session 10 · October 2026

---

## 01 — CONTEXT: Problem, users and goals achieved

### Problem
* Air-quality data is scattered across many providers with different formats and units.
* Hourly resolution and constantly growing; years of history stored in hundreds of thousands of small files.
* Raw data only contains concentrations — AQI must be computed using the EPA standard.
* Residents need early alerts, without spam when AQI hovers around a threshold.

### Goal → Result
* **Near real-time threshold alerts**
  * *Achieved:* alerts within 1 minute, at station and borough level, without spam.
* **Classify “pollution day types”**
  * *Achieved:* KMeans k = 5, silhouette 0.499 on 11,762 station-days.
* **Spatio-temporal analysis**
  * *Achieved:* AQI by borough, hour, month and year with Spark SQL.

**Users:** environmental agencies · researchers · alert apps for residents
**Scope:** the 5 boroughs of New York City

---

## 02 — JUSTIFICATION: Why this problem needs Big Data

* **VOLUME:** 1.16M Bronze records (historical backfill + streaming).
* **VELOCITY:** Real-time measurements flow continuously into Kafka (6 partitions); alerts every minute.
* **VARIETY:** 6 pollutants (PM2.5, PM10, O₃, NO₂, SO₂, CO) — many providers, different units.

---

## 03 — DECISION: Choosing the dataset

* **REJECTED: Beijing Multi-Site (UCI)**
  * Only 12 monitoring stations.
  * No real metadata table → no join/enrichment.
  * Too small to justify Spark.
* **CONSIDERED: EPA AQS (USA)**
  * Public domain, daily AQI usable as a baseline.
  * US historical data only.
  * Streaming would have to be simulated.
* **CHOSEN: OpenAQ**
  * One source with both batch (S3 archive) and stream (REST API v3).
  * Covers New York City.
  * S3 already Hive-partitioned: `locationid / year / month`.
  * Real API for the near real-time path.
  * *Selection principle: prefer a source that lets designed components become implemented components.*

---

## 04 — ARCHITECTURE: End-to-end architecture

*(Refer to the architecture diagram provided in the original slide covering OpenAQ API, Kafka, Spark Streaming, MapReduce, Bronze/Silver/Gold data lake layers, and PostgreSQL)*

---

## 05 — REAL-TIME PATH: From measurement to alert within one minute

* **Data source (OpenAQ API):** In the demo, a script simulates a real-time feed.
* **Kafka:** Durable queue, keeps per-station order, replayable after failure.
* **Spark Streaming:** One job, two branches running in parallel.
  * **Branch A (Storage):** Writes all raw data to the data lake as input for batch processing.
  * **Branch B (Alerts):** Every minute computes instant AQI and raises station- and borough-level alerts. No alert spam when AQI hovers around a threshold.
* A stopped job resumes exactly where it left off; replaying old data creates no duplicate alerts.

---

## 06 — DATA QUALITY: Cleaning keeps 94% of records

**Cleaning rules:**
* Keep only NYC stations, tag each with its borough.
* Drop 8,150 negative or out-of-range readings.
* Drop 57,776 duplicate readings.
* *Spark and MapReduce share the same rule set.*

*(Note: Y-axis on the chart starts at 900,000 rows to make the differences visible).*

---

## 07 — MAPREDUCE: MapReduce independently verifies Spark

* **MAP:** Each reading → key (station, pollutant, day)
* **SHUFFLE:** All readings with the same key go to one reducer
* **REDUCE:** Remove duplicates, compute mean, max, hours over threshold
* **COMPARE:** Check every key against Spark's output
* **Why no combiner?** A combiner aggregates before duplicates are removed, so a duplicate split across two mappers would be counted twice. The cost: more data is shuffled.
* **PASS:** 100% match with Spark. Zero difference on every metric; any mismatch stops the pipeline before the analytics step.

---

## 08 — SPARK: Spark pipeline: raw → clean → analytics-ready

* **BRONZE (Raw data):** Stored as-is, never modified. Any error is fixed by reprocessing from here.
* **SILVER (Clean data):** Filter errors, remove duplicates. Enrich with station and borough. Parquet, partitioned by day.
* **GOLD (Analytics-ready):** Daily AQI per EPA standard. Aggregates by borough, hour, month. Features for KMeans.

---

## 09 — SPARK SQL: Results: AQI by borough, hour and month

* **Q1 · Ranking days with AQI > 100:**
  * 2025: Manhattan leads with 9 days, followed by Brooklyn (6).
  * 2026: Queens, Manhattan and Brooklyn tie at 7 days.
* **Q2 · By month:**
  * July is the worst: Brooklyn averages AQI 55.9.
  * October is the cleanest month in all 5 boroughs.
* **Q3 · Widespread events:**
  * 2026-02-18: 11/21 stations (52.4%) at USG level or worse at the same time.

---

## 10 — QUERY PLAN: How Spark executes a query

**Q1 — rank boroughs by days with AQI > 100**
```text
Sort
+- Exchange              ← shuffle
   +- Window             ← rank per year
      +- Exchange        ← shuffle
         +- HashAggregate
            +- Exchange  ← shuffle by borough
               +- BroadcastHashJoin
                  :- Scan fact_daily_aqi
                  :    filter AQI > 100 on read
                  +- Broadcast dim_station
```
1. **Early filtering:** Only days with AQI > 100 are read from the Parquet files.
2. **Broadcast join:** The small station table is sent to every executor, so the large table is not shuffled.
3. **Shuffle (Exchange):** Happens when grouping by borough and when ranking per year.

---

## 11 — PERFORMANCE: Experiment: broadcast join and partition count

| # | Join strategy | Time (s) |
| :--- | :--- | :--- |
| 1 | Shuffle join (broadcast disabled) | [fill in] |
| 2 | Broadcast join | [fill in] |
| 3 | Broadcast + repartition to 20 | [fill in] |

* **Environment:** Spark 3.5.3, 2 workers × 2 cores × 2 GB.
* **Data:** ≈ 1.09 million Silver rows joined with the station table.
* **Observation:** [fill in after running make perf].

---

## 12 — MACHINE LEARNING: KMeans clustering of “pollution day types”

* Each point: one station on one day.
* Features: mean AQI, max AQI, hours > 100, peak hour, fine-dust / ozone ratio.

| Cluster | % of days | Mean AQI | Max AQI |
| :--- | :--- | :--- | :--- |
| Very clean day | 31.5% | 26.2 | 48.6 |
| Clean day | 11.7% | 28.1 | 46.6 |
| Moderate day | 27.6% | 28.5 | 52.4 |
| Mildly polluted day | 26.2% | 59.3 | 87.1 |
| **Heavily polluted day** | **3.0%** | **96.3** | **158.1** |

*3% of days fall into the heavily polluted cluster (average max AQI 158) — the group that needs alerts; the dashboard attaches a recommendation to each cluster.*

---

## 13 — SERVING: Getting results to users

* **PostgreSQL:** Current AQI of every station, alert history, analytics results and KMeans clusters.
* **Dashboard:** Map of NYC stations, AQI trends by borough, cluster profiles with recommendations.
* **Notifier:** Receives borough-level alerts from Kafka, sends Telegram messages.

---

## 14 — DEMO: Demo walkthrough

1. **Start the system:** Kafka, Spark, MinIO, PostgreSQL running on Docker.
2. **Stream real-time data:** A script simulates readings pushed into Kafka.
3. **Alert fires:** AQI crosses a threshold → alert in the database and on Telegram.
4. **Replay old data:** No duplicate alerts are created.
5. **Batch + MapReduce:** Raw → clean → analytics; MapReduce matches Spark.
6. **Dashboard:** Map, trends, alerts, KMeans clusters.

---

## 15 — TRADE-OFFS: Design choices and their cost

| Choice | Gain | Cost |
| :--- | :--- | :--- |
| All new data goes through Kafka | One single path, easy to operate | Historical data must be loaded separately |
| Two AQI types: instant and daily | Alerts don't wait 24 hours | Instant AQI is not the official EPA index |
| PostgreSQL instead of NoSQL | Consistent state updates | Deviates from the brief; harder to scale writes |
| Single-machine deployment | Easy to reproduce with Docker | Performance numbers are only relative |

*Future work: next-day AQI forecasting · move serving to Cassandra · run on Kubernetes.*

---

## 16 — CONTRIBUTIONS: Contribution of each member

*(Draft: roles inferred from git history — the team must confirm before presenting.)*

* **Lê Công Huỳnh (H):** Docker infrastructure, Kafka, MinIO, PostgreSQL, Poller, Streaming Query A + B, Alert rules, AQI, MapReduce + verification, Notifier.
* **Lý Minh Thông (T):** Batch pipeline run at 1-million-record scale, Spark SQL, KMeans, ML report, Flask dashboard + per-cluster recommendations, Performance experiment.
* **Dương Thành Duy (D):** Dataset survey and selection, Architecture and cluster/cloud design, NoSQL serving design, Final report.

---

## SUMMARY

A working system, from API to alert.
* **1.16M** Bronze records
* **0 diff** MapReduce ↔ Spark
* **0.499** KMeans silhouette, k = 5
* **58** unit tests

**Thank you — questions are welcome!**

*Q&A Topics: distributed execution · Spark plan / shuffle · model evaluation · NoSQL access patterns · streaming · scalability*