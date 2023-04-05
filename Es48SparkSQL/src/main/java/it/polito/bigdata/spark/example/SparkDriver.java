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
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es48 - SparkSQL").getOrCreate();
		
		Dataset<Row> persons = ss.read().option("delimiter", ",").format("csv")
								 .option("header", true).option("inferSchema", true).load(inputPath);
		
		persons.createOrReplaceTempView("people");
		
		Dataset<Row> personsAvg = ss.sql(""
				+ "SELECT name, avg(age) as avgAge, count(*) as counter "
				+ "FROM people "
				+ "GROUP BY name")
				.filter(row -> row.getLong(row.fieldIndex("counter")) > 1)
				.select("name", "avgAge");
		
		personsAvg.javaRDD().coalesce(1).saveAsTextFile(outputPath);
		
		// Close the Spark session
		ss.close();
	}
}
