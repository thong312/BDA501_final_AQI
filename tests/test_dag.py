import pytest
from airflow.models import DagBag

def test_dag_loaded_successfully():
    """Kiểm tra DAG parse không có lỗi cú pháp và DAG aq_daily_batch_pipeline tồn tại"""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    
    assert len(dagbag.import_errors) == 0, "Có lỗi khi nạp file DAG"
    
    dag_id = 'aq_daily_batch_pipeline'
    assert dag_id in dagbag.dags
    
    dag = dagbag.dags[dag_id]
    assert len(dag.tasks) == 4
    
    # Kiểm tra thứ tự phụ thuộc
    clean_task = dag.get_task('clean_bronze_to_silver')
    agg_task = dag.get_task('aggregate_silver_to_gold')
    valid_task = dag.get_task('validate_mr_vs_spark')
    
    # downstream của clean phải chứa aggregate
    assert agg_task.task_id in [t.task_id for t in clean_task.downstream_list]
    # downstream của agg phải chứa valid
    assert valid_task.task_id in [t.task_id for t in agg_task.downstream_list]
