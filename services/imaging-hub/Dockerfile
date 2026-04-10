FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/imaging-common/ packages/imaging-common/
COPY services/imaging-hub/ services/imaging-hub/

RUN uv sync --frozen --package imaging-hub --no-dev

ENV PATH="/app/.venv/bin:$PATH"

RUN addgroup --system app && adduser --system --ingroup app app \
    && apt-get update && apt-get install -y --no-install-recommends gosu && rm -rf /var/lib/apt/lists/*

COPY services/imaging-hub/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 11112
EXPOSE 9000

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "imaging_hub"]
