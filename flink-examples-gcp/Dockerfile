FROM flink:1.19

COPY . .
RUN mkdir /opt/flink/usrlib
ADD target/flink-examples-gcp-0.0.0-shaded.jar  /opt/flink/usrlib/flink-examples-gcp-0.0.jar

RUN chown -R flink:flink /opt/flink