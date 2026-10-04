import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402


def test_dag_structure():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert not dagbag.import_errors
    dag = dagbag.dags["aq_daily_batch_pipeline"]
    assert len(dag.tasks) == 5

    def down(task_id):
        return {t.task_id for t in dag.get_task(task_id).downstream_list}

    assert down("clean_bronze_to_silver") == {"mapreduce_daily_stats", "aggregate_silver_to_gold"}
    assert down("mapreduce_daily_stats") == {"validate_mr_vs_spark"}
    assert down("aggregate_silver_to_gold") == {"validate_mr_vs_spark"}
    assert down("validate_mr_vs_spark") == {"analytics_ml_sql"}
