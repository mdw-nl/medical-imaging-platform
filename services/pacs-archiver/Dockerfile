FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/imaging-common/ packages/imaging-common/
COPY services/pacs-archiver/ services/pacs-archiver/

RUN uv sync --frozen --package pacs-archiver --no-dev

ENV PATH="/app/.venv/bin:$PATH"

RUN addgroup --system app && adduser --system --ingroup app app
USER app

CMD ["python", "-m", "pacs_archiver"]
