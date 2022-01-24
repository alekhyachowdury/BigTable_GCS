package com.ibm.ikea.poc;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.beam.sdk.io.hbase.*;
import java.util.Date;
import java.text.SimpleDateFormat;

public class BigTable_GCS {

	
	  public interface BigtableOptions extends DataflowPipelineOptions {
		    @Description("The Bigtable project ID, this can be different than your Dataflow project")
		    @Default.String("bigtable-project")
		    String getBigtableProjectId();

		    void setBigtableProjectId(String bigtableProjectId);

		    @Description("The Bigtable instance ID")
		    @Default.String("bigtable-instance")
		    String getBigtableInstanceId();

		    void setBigtableInstanceId(String bigtableInstanceId);

		    @Description("The Bigtable table ID in the instance.")
		    @Default.String("mobile-time-series")
		    String getBigtableTableId();

		    void setBigtableTableId(String bigtableTableId);
		    
		    @Description("The Output GCS Bucket.")
		    @Default.String("OutputGCSBucketPath")
		    String getOutputGCSBucketPath();

		    void setOutputGCSBucketPath(String outputGCSBucketPath);
		  }
	  
	  

	  public static void main(String[] args) {
		  
		  	
		    BigtableOptions options =
		        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtableOptions.class);
		    Pipeline p = Pipeline.create(options);

		    CloudBigtableScanConfiguration config =
		        new CloudBigtableScanConfiguration.Builder()
		            .withProjectId(options.getBigtableProjectId())
		            .withInstanceId(options.getBigtableInstanceId())
		            .withTableId(options.getBigtableTableId())		            
		            .build();
		    
		    SimpleDateFormat date = new SimpleDateFormat("yyyy.MM.dd.HH:mm:ss");
		    String timeStamp = date.format(new Date());		    
		    String outputFileName = options.getOutputGCSBucketPath() + timeStamp;
		    p.apply("ReadFromBigTable",Read.from(CloudBigtableIO.read(config)))
		      
		        .apply("ParseRowsAndColumns",
			            ParDo.of(
			                new DoFn<Result, String>() {
			                  @ProcessElement
			                  public void processElement(@Element Result row, OutputReceiver<String> out) {
			                
			                	String rowValue = Bytes.toString(row.getRow());
			                    String columnValue1 = Bytes.toString(row.getValue(Bytes.toBytes("columnfamily1"),Bytes.toBytes("column1")));
			                    String columnValue2 = Bytes.toString(row.getValue(Bytes.toBytes("columnfamily1"),Bytes.toBytes("column2")));
			                    String outPutValue = rowValue+ "," +columnValue1+ "," +columnValue2;
			                    System.out.println(outPutValue);
			                    out.output(outPutValue);
			                  }
			                }))
		        .apply("WriteToGCS",TextIO.write().to(outputFileName).withOutputFilenames().withSuffix(".csv"));
		  
		   
		    p.run().waitUntilFinish();
		  }

}
