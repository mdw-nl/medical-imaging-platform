FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/imaging-common/ packages/imaging-common/
COPY services/dvh-calculator/ services/dvh-calculator/

RUN uv sync --frozen --package dvh-calculator --no-dev

ENV PATH="/app/.venv/bin:$PATH"

RUN addgroup --system app && adduser --system --ingroup app app
USER app

EXPOSE 8000

CMD ["python", "-m", "dvh_calculator"]
