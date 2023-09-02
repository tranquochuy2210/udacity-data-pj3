CREATE EXTERNAL TABLE IF NOT EXISTS `project-3`.`customers_curated` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDay` string,
  `serialNumber` string,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '0'
) LOCATION 's3://huytq57-project-3/customer/curated/'
TBLPROPERTIES ('has_encrypted_data'='false',  'classification'='json');