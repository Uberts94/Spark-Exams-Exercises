package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		String outputPath;
		String prefix;
		
		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];

	
		SparkSession ss = SparkSession.builder().master("local").appName("Es32 - dataset").getOrCreate();

		// Close the Spark context
		ss.close();
	}
}
