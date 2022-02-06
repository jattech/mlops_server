FROM python:3.7

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

EXPOSE 5000
EXPOSE 5001
EXPOSE 5002
EXPOSE 5003
EXPOSE 5004
EXPOSE 5005

ENTRYPOINT sleep 10 && mlflow server --backend-store-uri postgresql://mlflow_user:mlflow@postgresml/mlflow_db --default-artifact-root ftp://test:test@ftpd/home/test/ --host 0.0.0.0

