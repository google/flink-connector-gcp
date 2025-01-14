CREATE TABLE Words (
  word VARCHAR(32),
  ts AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'fields.word.var-len' = 'true',
  'rows-per-second' = '10'
);

CREATE TABLE BigtableHBaseSink (
  total BIGINT,
  cf1 ROW<window_start TIMESTAMP(3), window_end TIMESTAMP(3)>,
  PRIMARY KEY (total) NOT ENFORCED
) WITH (
  'connector' = 'hbase-2.2',
  'table-name' = '<bt-table-name>', -- BigTable Table Name
  'zookeeper.quorum' = 'localhost:2181', -- Zookeeper Quarom is required but not used by the connector. This is dummy string and does not need to be changed.
  'properties.hbase.client.connection.impl' = 'com.google.cloud.bigtable.hbase1_x.BigtableConnection', -- Connection class to make connections. Do not change.
  'properties.google.bigtable.project.id' = '<project-id>',
  'properties.google.bigtable.instance.id' = '<bt-instance-id>'
);

INSERT INTO BigtableHBaseSink
SELECT COUNT(*) AS total, ROW(window_start, window_end)
FROM TABLE(
  TUMBLE(TABLE Words, DESCRIPTOR(ts), INTERVAL '2' MINUTE)
) GROUP BY window_start, window_end;