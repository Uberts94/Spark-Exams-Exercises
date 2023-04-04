package it.polito.bigdata.spark.example;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;

import org.apache.spark.sql.Column;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		
		inputPath=args[0];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es36 - dataframe").getOrCreate();
		
		DataFrameReader dfr = ss.read().option("delimiter", ",").format("csv").option("header", false)
								.option("inferSchema", true);
		
		Dataset<Row> df = dfr.load(inputPath).selectExpr("_c2 as value");
		
		Double avg  = df.agg(avg(new Column("value"))).first().getDouble(0);
		
		System.out.println("Average pollution: "+avg);
		
		// Close the Spark context
		ss.close();
	}
}
