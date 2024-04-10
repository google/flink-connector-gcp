FROM flink:1.18

RUN mkdir /opt/flink/usrlib
ADD target/gcsbenchmark-0.1.jar /opt/flink/usrlib/gcsbenchmark-0.1.jar
RUN mkdir /opt/flink/plugins/gs-fs-hadoop/
RUN wget -q -O /opt/flink/plugins/gs-fs-hadoop/flink-gs-fs-hadoop-1.18.1.jar https://repo1.maven.org/maven2/org/apache/flink/flink-gs-fs-hadoop/1.18.1/flink-gs-fs-hadoop-1.18.1.jar
RUN chown -R flink:flink /opt/flink