FROM python:3.12-slim

WORKDIR /app

COPY packages/imaging-common/ packages/imaging-common/
COPY services/imaging-hub/ services/imaging-hub/

RUN pip install --no-cache-dir ./packages/imaging-common/ ./services/imaging-hub/

RUN addgroup --system app && adduser --system --ingroup app app \
    && apt-get update && apt-get install -y --no-install-recommends gosu && rm -rf /var/lib/apt/lists/*

COPY services/imaging-hub/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 11112
EXPOSE 9000

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "imaging_hub"]
