package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

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
		float threshold;
		
		inputPath=args[0];
		outputPath=args[1];
		threshold=Float.parseFloat(args[2]);
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Es40").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Es40").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		JavaPairRDD<Integer, String> outputRDD = inputRDD.filter(line -> {
			String[] sensor = line.split(",");
			if(Float.parseFloat(sensor[2]) > threshold) return true;
			else return false;
		}).mapToPair(line -> {
			String[] sensor = line.split(",");
			return new Tuple2<String, Integer>(sensor[0], 1);
		}).reduceByKey((e1, e2) -> e1+e2).mapToPair(e -> new Tuple2<Integer, String>(e._2(), e._1())).sortByKey(false);

		outputRDD.saveAsTextFile(outputPath);
		
		// Close the Spark context
		sc.close();
	}
}
