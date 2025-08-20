# dags/etl_flights.py
# DAG demo: ejecuta src/etl.py y luego valida filas cargadas leyendo .artifacts/etl_result.json

import json
from pathlib import Path
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Rutas dentro del contenedor (mapeadas por docker-compose)
BASE = Path("/opt/airflow/repo")
ART = BASE / ".artifacts"

# Zona horaria CL
CLT = pendulum.timezone("America/Santiago")

default_args = {
    "owner": "franco",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="etl_flights_demo",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz=CLT),
    schedule=None,          # manual para el demo (Trigger DAG)
    catchup=False,
    tags=["demo", "etl"],
) as dag:

    # 1) Ejecuta tu script ETL
    run_etl = BashOperator(
        task_id="run_etl_script",
        bash_command="python -u /opt/airflow/repo/src/etl.py",
    )

    # 2) Lee el artefacto y loguea el conteo; retorna el valor (queda en XCom)
    def check_counts(**_):
        path = ART / "etl_result.json"
        if not path.exists():
            raise FileNotFoundError(f"No se encontró {path}")
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = int(data.get("rows_loaded", 0))
        print(f"✅ Filas cargadas: {rows}")
        return rows  # Queda en XCom como 'return_value'

    check = PythonOperator(
        task_id="check_loaded_rows",
        python_callable=check_counts,   # no uses provide_context en Airflow 2.x
    )

    run_etl >> check
