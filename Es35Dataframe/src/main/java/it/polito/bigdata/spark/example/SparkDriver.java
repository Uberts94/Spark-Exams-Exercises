package it.polito.bigdata.spark.example;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.max;

public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es35 - dataframe").getOrCreate();
		
		DataFrameReader dfr = ss.read().option("delimiter", ",").format("csv").option("header", false).option("inferSchema", true);
		
		Dataset<Row> df = dfr.load(inputPath).selectExpr("_c0 as sensorId", "_c1 as timestamp", "_c2 as value");
		
		Double max = df.select("value").agg(max("value")).first().getDouble(0);
		
		df.filter(row -> {
			if(row.getAs("value").equals(max)) return true;
			else return false;
		}).select("timestamp")
		.javaRDD().saveAsTextFile(outputPath);;
		
		
		// Close the Spark context
		ss.close();
	}
}
