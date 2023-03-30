package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Es35").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Es35").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		JavaRDD<String> inputRDD = sc.textFile(inputPath).cache();
		
		float max = inputRDD.map(line -> {
			String[] sensor = line.split(",");
			return Float.parseFloat(sensor[2]);
 		}).reduce((e1, e2) -> {
 			if(e1 > e2) return e1;
 			else return e2;
 		});
		
		JavaRDD<String> outputRDD = inputRDD.filter(line -> {
			String[] sensor = line.split(",");
			if(Float.parseFloat(sensor[2]) == max) return true;
			else return false;
		}).map(line -> {
			String[] sensor = line.split(",");
			return sensor[1];
		});
		
		outputRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
