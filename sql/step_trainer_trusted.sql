CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://jammal/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Step_Trainer_Landing_To_Trusted', 
  'CreatedByJobRun'='jr_880fcdd4c5cd4a8c656fe210c8700904f3bc5144e880b52be06dd88664c1a6ae', 
  'classification'='json')