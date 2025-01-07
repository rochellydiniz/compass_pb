FROM python:3.9-slim
RUN pip install boto3 python-dotenv pandas pytz
WORKDIR /img
COPY . .
CMD ["python", "ingestao_batch.py"]