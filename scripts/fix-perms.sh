#!/usr/bin/env bash
set -e
# Arregla permisos de los directorios montados
sudo chown -R 50000:0 dags src data .artifacts airflow || true
sudo chmod -R 775 dags src data .artifacts airflow || true
echo "Permisos ajustados."
