CREATE EXTERNAL TABLE IF NOT EXISTS `project-3`.`machine_learning_curated` (
  `serialNumber` string,
  `z` float,
  `timeStamp` bigint,
  `birthDay` string,
  `shareWithPublicAsOfDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `registrationDate` bigint,
  `customerName` string,
  `user` string,
  `sensorReadingTime` bigint,
  `y` float,
  `shareWithFriendsAsOfDate` bigint,
  `x` float,
  `distanceFromObject` int,
  `lastUpdateDate` bigint,
  `phone` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://huytq57-project-3/machine_learning_curated/'
TBLPROPERTIES ('classification' = 'json');