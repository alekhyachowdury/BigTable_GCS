# BigTable_GCS

mvn clean compile exec:java -Dexec.mainClass=com.ibm.ikea.poc.BigTable_GCS 
"-Dexec.args=
--project=GCP-PROJECT-ID
--bigtableProjectId=GCP-PROJECT-ID
--bigtableInstanceId=BIG-TABLE-INSTANCE-ID
--bigtableTableId=TABLE-NAME
--outputGCSBucketPath=OUTPUT-BUCKET-PATH
--runner=dataflow  
--gcpTempLocation=TEMP-BUCKET-PATH-FOR-DATAFLOW-FILES
--region=GCP-REGION"
