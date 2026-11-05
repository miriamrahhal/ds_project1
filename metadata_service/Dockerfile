FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY metadata_service/ ./metadata_service/
CMD ["uvicorn", "metadata_service.main:app", "--host", "0.0.0.0", "--port", "8000"]