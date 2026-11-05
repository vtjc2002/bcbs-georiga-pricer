FROM python:3.12-slim@sha256:e695ab189fa2d082b1c61c69a6b888a5adeca61ceeccad107b1ed2a125721a9e
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY etl/requirements.txt etl/requirements.txt
RUN pip install --no-cache-dir -r etl/requirements.txt
COPY etl/*.py etl/
COPY metadata metadata
COPY tests tests
ENTRYPOINT ["python", "-m", "etl.cli"]
