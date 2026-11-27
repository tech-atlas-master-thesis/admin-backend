FROM python:3.14

RUN adduser --system --no-create-home nonroot

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV MPLCONFIGDIR /tmp/matplotlib

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpango-1.0-0 libpangoft2-1.0-0 libharfbuzz-subset0 fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/

RUN mkdir -p /var/lib/registry-data
RUN chown -R nonroot /app /var/lib/registry-data
USER nonroot

EXPOSE 8080
CMD ["fastapi", "run", "main.py", "--port", "8080"]
