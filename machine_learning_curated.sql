CREATE EXTERNAL TABLE `machine_learning_curated`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://jammal/machine_learning/curated/'
TBLPROPERTIES (
  'CreatedByJob'='step_trainer_trusted_to_curated_job', 
  'CreatedByJobRun'='jr_b8951af5269b856d89bb3e582986fe76e3a0c57dfcc36faa209f4bd30ec01675', 
  'classification'='json')