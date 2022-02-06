SELECT 'CREATE DATABASE mlflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow_db')\gexec
GRANT ALL PRIVILEGES ON DATABASE mlflow_db TO mlflow_user;
