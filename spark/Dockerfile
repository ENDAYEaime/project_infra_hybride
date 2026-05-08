FROM apache/spark:3.5.0

USER root

WORKDIR /app

COPY spark_ticket_processing.py .

RUN mkdir -p /tmp/.ivy2/cache && chmod -R 777 /tmp/.ivy2

USER spark

CMD ["/opt/spark/bin/spark-submit", \
     "--conf", "spark.jars.ivy=/tmp/.ivy2", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "/app/spark_ticket_processing.py"]