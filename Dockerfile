FROM python:3.12-slim

RUN useradd --create-home --shell /usr/sbin/nologin keziah
WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .

USER keziah
EXPOSE 8766

# Inside a container, 0.0.0.0 is what lets a published port reach the process.
# Publish that port on 127.0.0.1 unless you also set KEZIAH_API_KEY.
ENTRYPOINT ["keziah"]
CMD ["serve", "--mode", "hybrid", "--host", "0.0.0.0", "--port", "8766"]
