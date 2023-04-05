package it.polito.bigdata.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es49 - dataframe").getOrCreate();
		
		ss.udf().register("range", (Integer inf, Integer sup) -> "["+inf+"-"+sup+"]", DataTypes.StringType);
		
		Dataset<Row> persons = ss.read().option("delimiter", ",").format("csv").option("header", true)
								 .option("inferSchema", true)
								 .load(inputPath)
								 .selectExpr("name", "surname", "range((age-(age%10)), (age-(age%10))+9) as rangeage");
		
		persons.write().format("csv").option("header", true).save(outputPath);
								 	
		// Close the Spark context
		ss.close();
	}
}
