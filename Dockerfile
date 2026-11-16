# Real Google Chrome + Playwright's Chromium on a private virtual display, with
# noVNC for the one-time manual Google Ads login (same recipe as
# notebooklm-docker).
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    dumb-init procps curl ca-certificates xvfb x11vnc fluxbox novnc websockify libatomic1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt \
    && playwright install chromium chrome --with-deps \
    && rm -rf /var/lib/apt/lists/*

COPY app ./app
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8101"]
