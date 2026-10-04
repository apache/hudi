SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '60000 ms';

CREATE TABLE `orders_hudi` (
  `id` STRING NOT NULL,
  `name` STRING,
  `event_ts` TIMESTAMP(3) NOT NULL,
  `partition_date` DATE NOT NULL,
  PRIMARY KEY (`id`) NOT ENFORCED
)
PARTITIONED BY (`partition_date`)
WITH (
  'connector' = 'hudi',
  'path' = 'file:///tmp/hudi-architect-orders',
  'table.type' = 'COPY_ON_WRITE',
  'write.operation' = 'insert',
  'write.insert.cluster' = 'false'
);

INSERT INTO `orders_hudi` (
  `id`,
  `name`,
  `event_ts`,
  `partition_date`
)
SELECT
  `id`,
  `name`,
  `event_ts`,
  `partition_date`
FROM `orders_source`;
