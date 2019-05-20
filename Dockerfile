FROM ubuntu:18.04

EXPOSE 5000

RUN useradd --create-home redash

# Ubuntu packages
RUN apt-get update &&  \
  apt-get install -y python-pip python-dev build-essential pwgen libffi-dev sudo git-core wget unzip \
  # Postgres client
  libpq-dev \
  # for SAML
  xmlsec1 \
  # Additional packages required for data sources:
  libaio1 libssl-dev libmysqlclient-dev freetds-dev libsasl2-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN pip install -U setuptools==23.1.0

WORKDIR /app

# We first copy only the requirements file, to avoid rebuilding on every file change.
COPY ./requirements ./requirements
RUN pip install -r /app/requirements/requirements.txt -r /app/requirements/requirements_dev.txt -r /app/requirements/requirements_all_ds.txt

COPY . .

RUN chown -R redash /app
USER redash

ENTRYPOINT ["/app/bin/docker-entrypoint"]
CMD ["server"]
