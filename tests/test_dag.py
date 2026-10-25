"""DAG integrity tests. Skipped unless Airflow is installed (the CI 'airflow' job installs it)."""

import pytest

pytest.importorskip("airflow")


def test_dag_imports_and_structure():
    try:
        from airflow.dag_processing.dagbag import DagBag  # Airflow >= 3.1
    except ImportError:
        from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder="dags", include_examples=False)
    assert bag.import_errors == {}
    dag = bag.get_dag("climate_daily")
    assert dag is not None
    assert {t.task_id for t in dag.tasks} == {
        "list_cities",
        "extract_city",
        "load_raw",
        "build_metrics",
        "check_quality",
    }
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] == 3


def test_target_day_applies_lag():
    import importlib.util

    spec = importlib.util.spec_from_file_location("climate_daily", "dags/climate_daily.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.target_day("2024-06-08") == "2024-06-01"
