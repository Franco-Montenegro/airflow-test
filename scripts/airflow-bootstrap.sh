#!/usr/bin/env bash
set -euo pipefail

# Vars
: "${AIRFLOW_USER:=admin}"
: "${AIRFLOW_PASS:=admin}"

echo "[bootstrap] preparando carpetas…"
mkdir -p /opt/airflow/logs \
         /opt/airflow/repo/data/processed \
         /opt/airflow/repo/.artifacts
chmod -R 777 /opt/airflow/logs /opt/airflow/repo || true

echo "[bootstrap] instalando requirements (si existen)…"
if [ -f /opt/airflow/requirements.txt ] && grep -q '[^[:space:]]' /opt/airflow/requirements.txt; then
  pip install --no-cache-dir -r /opt/airflow/requirements.txt || true
fi

echo "[bootstrap] inicializando base de datos…"
airflow db check || airflow db init || airflow db upgrade || airflow db migrate

echo "[bootstrap] creando/actualizando usuario admin…"
# Si existe, actualiza la clave; si no, lo crea
if airflow users list 2>/dev/null | grep -E " ${AIRFLOW_USER} " >/dev/null 2>&1; then
  airflow users update --username "${AIRFLOW_USER}" --password "${AIRFLOW_PASS}"
else
  airflow users create \
    --username "${AIRFLOW_USER}" \
    --password "${AIRFLOW_PASS}" \
    --firstname Franco \
    --lastname Admin \
    --role Admin \
    --email admin@example.com
fi

echo "[bootstrap] arrancando webserver + scheduler…"
airflow webserver --port 8080 --hostname 0.0.0.0 &
exec airflow scheduler
