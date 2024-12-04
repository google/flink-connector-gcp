CREATE TABLE Words (
  word VARCHAR(32),
  ts AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'fields.word.var-len' = 'true',
  'rows-per-second' = '10'
);

CREATE TABLE BigtableSink (
  total BIGINT,
  cf1 ROW<window_start TIMESTAMP(3), window_end TIMESTAMP(3)>,
  PRIMARY KEY (total) NOT ENFORCED
) WITH (
  'connector' = 'hbase-2.2',
  'table-name' = 'test-table', -- BigTable Table Name
  'zookeeper.quorum' = 'localhost:2181', -- Zookeeper Quarom is required but not used by connector this is dummy string
  'properties.hbase.client.connection.impl' = 'com.google.cloud.bigtable.hbase1_x.BigtableConnection', -- Connection class to make connections
  'properties.google.bigtable.project.id' = '<project-id>',
  'properties.google.bigtable.instance.id' = '<bt-instance-id>'
);

INSERT INTO BigtableSink
SELECT COUNT(*) AS total, ROW(window_start, window_end)
FROM TABLE(
  TUMBLE(TABLE Words, DESCRIPTOR(ts), INTERVAL '2' MINUTE)
) GROUP BY window_start, window_end;