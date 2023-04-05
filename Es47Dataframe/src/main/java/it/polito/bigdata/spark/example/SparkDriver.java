package it.polito.bigdata.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Column;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es47 - dataframe").getOrCreate();
		
		Dataset<Row> maleUsers = ss.read().option("delimiter", ",").format("csv").option("header", true).option("inferSchema", true)
									.load(inputPath).select("name", "gender","age")
									.filter(row -> row.getString(row.fieldIndex("gender")).equals("male"))
									.selectExpr("name", "age+1 as newAge")
									.sort(new Column("newAge").desc(), new Column("name"));
		
		maleUsers.javaRDD().coalesce(1).saveAsTextFile(outputPath);
		
		// Close the Spark context
		ss.close();
	}
}
