FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY exporter.py /app/exporter.py

EXPOSE 9115

CMD ["python", "/app/exporter.py"]
