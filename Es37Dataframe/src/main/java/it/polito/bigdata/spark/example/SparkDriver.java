package it.polito.bigdata.spark.example;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es37 - dataframe").getOrCreate();
		
		DataFrameReader dfr = ss.read().option("delimiter", ",").format("csv").option("header", false).option("inferSchema", true);
		
		Dataset<Row> df = dfr.load(inputPath).selectExpr("_c0 as sensorId", "_c2 as value");
		
		
		
		df.groupBy("sensorId").max("value").javaRDD().coalesce(1).saveAsTextFile(outputPath);
		
		// Close the Spark context
		ss.close();
	}
}
