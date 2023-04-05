package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		
		inputPath=args[0];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es48 - SparkSQL").getOrCreate();
		

		// Close the Spark context
		ss.close();
	}
}
