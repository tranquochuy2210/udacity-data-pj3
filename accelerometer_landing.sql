CREATE EXTERNAL TABLE IF NOT EXISTS `project-3`.`accelerometer_landing` (
    `user` string,
    `timeStamp` bigint,
    `x` float,
    `y` float,
    `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES(
    'serialization.format' = '0'
) LOCATION 's3://huytq57-project-3/accelerometer/landing'
TBLPROPERTIES ('has_encrypted_data'='false');