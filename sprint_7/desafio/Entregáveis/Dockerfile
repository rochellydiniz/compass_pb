FROM python:3.9-slim
RUN pip install boto3 python-dotenv pandas pytz tmdbv3api requests
WORKDIR /img
COPY . .
CMD ["python", "ingestao_api_tmdb_aws.py"]