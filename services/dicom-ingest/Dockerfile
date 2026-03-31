FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/imaging-common/ packages/imaging-common/
COPY services/dicom-ingest/ services/dicom-ingest/

RUN uv sync --frozen --package dicom-ingest --no-dev

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 104
EXPOSE 9000

CMD ["python", "-m", "dicom_ingest"]
