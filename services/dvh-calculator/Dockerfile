FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY packages/imaging-common/ packages/imaging-common/
COPY services/dvh-calculator/ services/dvh-calculator/

RUN pip install --no-cache-dir ./packages/imaging-common/ ./services/dvh-calculator/

RUN addgroup --system app && adduser --system --ingroup app app
USER app

EXPOSE 8000

CMD ["python", "-m", "dvh_calculator"]
