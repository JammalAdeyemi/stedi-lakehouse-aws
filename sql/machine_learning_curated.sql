CREATE EXTERNAL TABLE `machine_learning_curated`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer')
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
  'CreatedByJobRun'='jr_440a83bf66c9acd8c96bfe2f8f0bacada3adb3f462673c068d0ada678178ad4c', 
  'classification'='json')