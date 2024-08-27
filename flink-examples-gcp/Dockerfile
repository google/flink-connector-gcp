FROM flink:1.19

COPY . .
RUN mkdir /opt/flink/usrlib
ADD target/flink-examples-gcp-0.0.0-shaded.jar  /opt/flink/usrlib/gmf-examples.jar
RUN mkdir /opt/flink/plugins/gs-fs-hadoop/
RUN wget -q -O /opt/flink/plugins/gs-fs-hadoop/flink-gs-fs-hadoop-1.19.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-gs-fs-hadoop/1.19.0/flink-gs-fs-hadoop-1.19.0.jar
RUN chown -R flink:flink /opt/flink