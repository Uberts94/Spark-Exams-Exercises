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
		
		inputPath=args[0];
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Es32").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Es32").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		JavaRDD<String> inputRDD = sc.textFile(inputPath).cache();

		float max = inputRDD.map(line -> {
			String[] entry = line.split(",");
			return Float.parseFloat(entry[2]);
		}).top(1).get(0);
		
		float maxReduce = inputRDD.map(line -> {
			String[] entry = line.split(",");
			return Float.parseFloat(entry[2]);
		}).reduce((e1, e2) -> {
			if(e1 > e2) return e1;
			else return e2;
		});

		System.out.println("Max result with top(1):	"+max);
		System.out.println("Max result with reduce:	"+maxReduce);
		
		// Close the Spark context
		sc.close();
	}
}
