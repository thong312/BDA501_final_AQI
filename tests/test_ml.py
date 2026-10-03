import pytest

pytest.importorskip("pyspark")
from jobs.batch.analytics import FEATURES, name_clusters, train_and_evaluate_kmeans  # noqa: E402

SCHEMA = ("location_id int, aqi_mean double, aqi_max double, hours_over_100 int, peak_hour int, "
          "pm25_o3_ratio double")


def test_kmeans_selects_k_and_denormalizes(spark):
    rows = []
    for i in range(6):
        rows.append((100 + i, 30.0 + i, 45.0 + i, 0, 8, 0.8))      # ngay sach
        rows.append((200 + i, 150.0 + i, 190.0 + i, 12, 15, 0.3))  # ngay o nhiem
    df = spark.createDataFrame(rows, SCHEMA)
    res = train_and_evaluate_kmeans(df, FEATURES, k_range=[2, 3])
    assert [s["k"] for s in res["scores"]] == [2, 3]
    assert res["best_k"] == 2
    means = sorted(c[0] for c in res["centers"])
    assert means[0] == pytest.approx(32.5) and means[1] == pytest.approx(152.5)


def test_name_clusters_by_aqi_rank():
    centers = [[150.0, 0, 0, 0, 0], [30.0, 0, 0, 0, 0]]
    cfg = {"names_by_k": {2: [{"name": "Sach"}, {"name": "O nhiem"}]}}
    assert name_clusters(centers, FEATURES, cfg) == {1: ("Sach", ""), 0: ("O nhiem", "")}
