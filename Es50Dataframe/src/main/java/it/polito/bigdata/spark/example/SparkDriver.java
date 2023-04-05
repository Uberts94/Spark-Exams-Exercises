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
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es50 - dataframe").getOrCreate();
		
		ss.udf().register("combineCredentials", (String name, String surname) 
				-> new String(name+" "+surname), DataTypes.StringType);
		
		Dataset<Row> mappedProfiles = ss.read().option("delimiter", ",").format("csv")
										.option("header", true).option("inferSchema", true)
										.load(inputPath)
										.selectExpr("combineCredentials(name, surname) as name_surname");
		
		mappedProfiles.write().format("csv").option("header", true).save(outputPath);
		// Close the Spark session
		ss.close();
	}
}
