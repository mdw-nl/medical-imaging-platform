FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl unzip \
    && curl -fsSL https://github.com/rordenlab/dcm2niix/releases/download/v1.0.20240202/dcm2niix_lnx.zip \
       -o /tmp/dcm2niix.zip \
    && unzip /tmp/dcm2niix.zip -d /usr/local/bin/ \
    && chmod +x /usr/local/bin/dcm2niix \
    && rm /tmp/dcm2niix.zip \
    && apt-get purge -y curl unzip && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

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
