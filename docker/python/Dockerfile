FROM python:3.12.1-bullseye

WORKDIR /src

EXPOSE 3000

RUN pip install --upgrade google-cloud-bigquery\
    && pip install Jinja2 jmespath pandas pyarrow
