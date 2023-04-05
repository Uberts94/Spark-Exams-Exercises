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
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es47 - SparkSQL").getOrCreate();
		
		Dataset<Row> persons = ss.read().option("delimiter", ",").format("csv")
									.option("header", true).option("inferSchema", true).load(inputPath);
		
		persons.createOrReplaceTempView("people");
		
		Dataset<Row> maleUsers = ss.sql("SELECT name, age+1 as newAge "
									   + "FROM people "
									   + "WHERE gender = 'male' "
									   + "ORDER BY newAge DESC, name");
		
		maleUsers.javaRDD().coalesce(1).saveAsTextFile(outputPath);
		// Close the Spark context
		ss.close();
	}
}
