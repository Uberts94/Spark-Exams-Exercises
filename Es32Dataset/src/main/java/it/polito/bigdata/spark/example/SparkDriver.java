package it.polito.bigdata.spark.example;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.max;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		
		inputPath=args[0];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es32 - dataset").getOrCreate();
		
		DataFrameReader dfr = ss.read().option("delimiter", ",").format("csv").option("header", false)
								.option("inferSchema", true);
		
		Dataset<Reading> readings = dfr.load(inputPath).as(Encoders.bean(Reading.class));
		
		Dataset<Double> maxValueDF = readings.agg(max("_c2")).as(Encoders.DOUBLE());
		
		System.out.println(maxValueDF.first());
		// Close the Spark context
		ss.close();
	}
}
