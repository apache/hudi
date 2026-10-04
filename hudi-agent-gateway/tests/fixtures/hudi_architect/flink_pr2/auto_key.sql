SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '60000 ms';

CREATE TABLE `events_hudi` (
  `payload` STRING,
  `event_ts` TIMESTAMP(3) NOT NULL
)
WITH (
  'connector' = 'hudi',
  'path' = 'file:///tmp/hudi-architect-events',
  'table.type' = 'COPY_ON_WRITE',
  'write.operation' = 'insert',
  'write.insert.cluster' = 'false'
);

INSERT INTO `events_hudi` (
  `payload`,
  `event_ts`
)
SELECT
  `payload`,
  `event_ts`
FROM `events_source`;
