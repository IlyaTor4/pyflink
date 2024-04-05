FROM apache/flink:1.17.2-scala_2.12-java11
ARG FLINK_VERSION=1.17.1

RUN set -ex; \
  apt-get update && \
  apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev lzma liblzma-dev

RUN wget https://www.python.org/ftp/python/3.10.0/Python-3.10.0.tgz

RUN tar -xvf Python-3.10.0.tgz && \
  cd Python-3.10.0 && \
  ./configure --without-tests --enable-shared && \
  make -j4 && \
  make install && \
  ldconfig /usr/local/lib && \
  cd .. && rm -f Python-3.10.0.tgz && rm -rf Python-3.10.0 && \
  ln -s /usr/local/bin/python3 /usr/local/bin/python && \
  ln -s /usr/local/bin/pip3 /usr/local/bin/pip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  python -m pip install --upgrade pip; \
  pip install apache-flink==${FLINK_VERSION}; \
  pip install kafka-python

COPY kafka_example/requirements.txt ./
RUN pip install -r ./requirements.txt


# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.0-1.17/flink-connector-jdbc-3.1.0-1.17.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar;

#ENV DJANGO_SETTINGS_MODULE=settings.dev
WORKDIR /opt/flink

COPY kafka_example .
