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


		String inputPath1;
		String inputPath2;
		String outputPath;
		int threshold;
		
		inputPath1=args[0];
		inputPath2=args[1];
		outputPath=args[2];
		threshold=Integer.parseInt(args[3]);

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Es43").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Es43").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		JavaRDD<String> readingsRDD = sc.textFile(inputPath1);
		
		JavaPairRDD<String, Float> criticalStationPerc = readingsRDD.mapToPair(line -> {
			String[] reading = line.split(",");
			if(Integer.parseInt(reading[5]) < threshold) 
				return new Tuple2<String, Tuple2<Integer, Integer>>(reading[0], new Tuple2<Integer, Integer>(1, 1));
			else return new Tuple2<String, Tuple2<Integer, Integer>>(reading[0], new Tuple2<Integer, Integer>(0, 1));
		}).reduceByKey((e1, e2) -> {
			return new Tuple2<Integer, Integer>(e1._1()+e2._1(), e1._2()+e2._2());
		}).mapToPair(e -> {
			return new Tuple2<Float, String>((float) e._2()._1()/e._2()._2(), e._1());
		}).filter(e -> {
			if (e._1 > 0.2) return true;
			else return false;
		}).sortByKey(false).mapToPair(e -> {
			return new Tuple2<String, Float>(e._2(), e._1());
		});
        
		criticalStationPerc.saveAsTextFile(outputPath);
		
		// Close the Spark context
		sc.close();
	}
}
