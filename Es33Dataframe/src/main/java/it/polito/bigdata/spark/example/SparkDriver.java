package it.polito.bigdata.spark.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		
		inputPath=args[0];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es33 - dataframe").getOrCreate();
		
		DataFrameReader dfr = ss.read().option("delimited", ",").format("csv").option("inferSchema", true);
		
		Dataset<Row> df = dfr.load(inputPath);
		
		List<Row> top3 = df.select("_c2").sort(new Column("_c2").desc()).takeAsList(3);
		
		System.out.println(top3);
				
		// Close the Spark context
		ss.close();
	}
}
