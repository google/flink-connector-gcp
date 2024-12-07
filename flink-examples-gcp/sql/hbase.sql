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
 family1 ROW<window_start TIMESTAMP(3), window_end TIMESTAMP(3)>,
 PRIMARY KEY (total) NOT ENFORCED
) WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'mytable',
 'zookeeper.quorum' = 'localhost:2181'
);

INSERT INTO hTable
SELECT COUNT(*) AS total, ROW(window_start, window_end)
FROM TABLE(
  TUMBLE(TABLE Words, DESCRIPTOR(ts), INTERVAL '2' MINUTE)
) GROUP BY window_start, window_end;