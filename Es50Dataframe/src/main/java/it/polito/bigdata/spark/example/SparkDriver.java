package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es50 - dataframe").getOrCreate();
		

		// Close the Spark context
		ss.close();
	}
}
