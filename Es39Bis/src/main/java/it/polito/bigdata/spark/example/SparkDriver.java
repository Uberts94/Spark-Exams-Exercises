package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.List;

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
		SparkConf conf=new SparkConf().setAppName("Es39Bis").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Es39Bis").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		JavaPairRDD<String, List<String>> outputRDD = inputRDD.mapToPair(line -> {
			String[] sensor = line.split(",");
			List<String> list = new ArrayList<String>();
			if(Float.parseFloat(sensor[2]) > threshold) list.add(sensor[1]);
			return new Tuple2<String, List<String>>(sensor[0], list);
		}).reduceByKey((e1, e2) -> {
			if(!e2.isEmpty()) e1.add(e2.get(0));
			return e1;
		});
		
		outputRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
