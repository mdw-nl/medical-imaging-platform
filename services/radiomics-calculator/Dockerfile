FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/imaging-common/ packages/imaging-common/
COPY services/radiomics-calculator/ services/radiomics-calculator/

RUN uv sync --frozen --package radiomics-calculator --no-dev --extra runtime

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "radiomics_calculator"]
