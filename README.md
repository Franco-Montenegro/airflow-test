# Airflow Test

## Requisitos
- Docker Desktop / Docker Engine
- (Windows) WSL2 recomendado

## Primer uso
```bash
git clone <tu_repo>.git
cd airflow-demo
cp .env.docker.example .env.docker
# (opcional) scripts/fix-perms.sh si WSL está mañosa
docker compose up -d --build
