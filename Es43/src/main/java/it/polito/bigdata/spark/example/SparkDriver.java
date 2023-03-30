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


		String inputPath1;
		String inputPath2;
		String outputPath1;
		String outputPath2;
		int threshold;
		
		inputPath1=args[0];
		inputPath2=args[1];
		outputPath1=args[2];
		outputPath2=args[3];
		threshold=Integer.parseInt(args[4]);
		List<String> timeslots = new ArrayList<String>();
		timeslots.add("[0-3]");
		timeslots.add("[4-7]");
		timeslots.add("[8-11]");
		timeslots.add("[12-15]");
		timeslots.add("[16-19]");
		timeslots.add("[20-23]");
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Es43").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Es43").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		JavaRDD<String> readingsRDD = sc.textFile(inputPath1).cache();
		
		// Task1
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
			if (e._1 > 0.5) return true;
			else return false;
		}).sortByKey(false).mapToPair(e -> {
			return new Tuple2<String, Float>(e._2(), e._1());
		});
        
		criticalStationPerc.saveAsTextFile(outputPath1);
		
		// Task2
		JavaPairRDD<Tuple2<String, String>, Float> criticalPercTimeslot = readingsRDD.mapToPair(line -> {
			String[] reading = line.split(",");
			int hour = Integer.parseInt(reading[2]), under = 0;
			String timeslot = new String("");
			if(Integer.parseInt(reading[5]) < threshold) under = 1;
			if(hour >= 0 && hour <= 3) timeslot = new String(timeslots.get(0));
			else if(hour >= 4 && hour <= 7) timeslot = new String(timeslots.get(1));
			else if(hour >= 8 && hour <= 11) timeslot = new String(timeslots.get(2));
			else if(hour >= 12 && hour <= 15) timeslot = new String(timeslots.get(3));
			else if(hour >= 16 && hour <= 19) timeslot = new String(timeslots.get(4));
			else if(hour >= 20 && hour <= 23) timeslot = new String(timeslots.get(5));
			return new Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>>
					(new Tuple2<String, String>(timeslot, reading[0]), new Tuple2<Integer, Integer>(under, 1));
 		}).reduceByKey((e1,e2) -> {
 			return new Tuple2<Integer, Integer>(e1._1()+e2._1(), e1._2()+e2._2());
 		}).mapToPair(e -> {
 			return new Tuple2<Float, Tuple2<String, String>>
 					((float) e._2()._1()/e._2()._2(), new Tuple2<String, String>(e._1()._1(), e._1()._2()));
 		}).filter(e -> {
 			if(e._1 > 0.5) return true;
 			else return false;
 		}).sortByKey(false).mapToPair(e -> new Tuple2<Tuple2<String, String>, Float>(new Tuple2<String, String>(e._2()._1(), e._2()._2()), e._1()));
		
		criticalPercTimeslot.saveAsTextFile(outputPath2);
		
		// Close the Spark context
		sc.close();
	}
}
