FROM openjdk:8-jdk-alpine

ARG SPARK_VER
ARG HDFS_VER
ENV SPARK_VERSION=${SPARK_VER}
ENV HADOOP_VERSION=${HDFS_VER}
ENV SPARK_DOWNLOAD=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
#http://apache.mirror.iphh.net/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN mkdir -p /opt \
  && apk --update add curl tzdata \
  && apk add bash \
  && curl -L $SPARK_DOWNLOAD -o /opt/spark.tar.gz \
#  && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
  && echo "export PATH=/opt/spark/bin:$PATH" >> /root/.bashrc

ENV SPARK_HOME=/apps/spark
ENV PATH=/apps/spark/bin/:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-1.8-openjdk/jre/bin:/usr/lib/jvm/java-1.8-openjdk/bin
WORKDIR /root
#COPY entrypoint.sh /root/entrypoint.sh
#RUN chmod +x /root/entrypoint.sh
#ENTRYPOINT [ "/root/entrypoint.sh" ]
CMD ["/bin/bash"]