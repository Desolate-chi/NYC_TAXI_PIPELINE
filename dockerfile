FROM python:3.11.9

RUN pip install pandas sqlalchemy psycopg2 response requests pyarrow

WORKDIR /app

COPY ingest_taxi_data.py ingest_taxi_data.py

ENTRYPOINT [ "python", "ingest_taxi_data.py" ]