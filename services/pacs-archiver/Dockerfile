FROM python:3.12-slim

WORKDIR /app

COPY packages/imaging-common/ packages/imaging-common/
COPY services/pacs-archiver/ services/pacs-archiver/

RUN pip install --no-cache-dir ./packages/imaging-common/ ./services/pacs-archiver/

RUN addgroup --system app && adduser --system --ingroup app app
USER app

CMD ["python", "-m", "pacs_archiver"]
