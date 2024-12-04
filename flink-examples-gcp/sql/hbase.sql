ADD JAR './target/flink-examples-gcp-0.0.0-shaded.jar';

CREATE TABLE Words (
  word VARCHAR(32),
  ts AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'fields.word.var-len' = 'true',
  'rows-per-second' = '10'
);

-- register the HBase table 'mytable' in Flink SQL
CREATE TABLE hTable (
 total BIGINT,
 PRIMARY KEY (total) NOT ENFORCED
) WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'mytable',
 'zookeeper.quorum' = 'cmcbox.c.googlers.com:60000'
);

INSERT INTO hTable
SELECT COUNT(*) AS total 
FROM TABLE(
  TUMBLE(TABLE Words, DESCRIPTOR(ts), INTERVAL '2' MINUTE)
) GROUP BY window_start, window_end;