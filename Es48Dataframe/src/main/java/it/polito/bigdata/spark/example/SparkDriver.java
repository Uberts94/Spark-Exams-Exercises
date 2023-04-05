package it.polito.bigdata.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession; 
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es48 - dataframe").getOrCreate();
		
		Dataset<Row> persons = ss.read().option("delimiter", ",").format("csv").option("header", true)
								.option("inferSchema", true).load(inputPath);
		
		Dataset<Row> filteredByCount = persons.groupBy("name").count()
								.filter(row -> row.getLong(row.fieldIndex("count")) > 1)
								.selectExpr("name as namef");
		
		Dataset<Row> personsAvg = persons.join(filteredByCount, persons.col("name")
								.equalTo(filteredByCount.col("namef")))
								.groupBy("name").avg("age");
		
		personsAvg.javaRDD().coalesce(1).saveAsTextFile(outputPath);
		
		// Close the Spark context
		ss.close();
	}
}
