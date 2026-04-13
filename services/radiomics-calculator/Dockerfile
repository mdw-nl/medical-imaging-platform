FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY packages/imaging-common/ packages/imaging-common/
COPY services/radiomics-calculator/ services/radiomics-calculator/

RUN pip install --no-cache-dir ./packages/imaging-common/ "./services/radiomics-calculator/[runtime]"

RUN addgroup --system app && adduser --system --ingroup app app
USER app

CMD ["python", "-m", "radiomics_calculator"]
