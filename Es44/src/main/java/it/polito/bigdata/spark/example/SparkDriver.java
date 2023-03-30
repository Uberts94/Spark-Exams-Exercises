package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.HashMap;
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
		String inputPath3;
		String outputPath;
		float threshold;
		
		inputPath1=args[0];
		inputPath2=args[1];
		inputPath3=args[2];
		outputPath=args[3];
		threshold=Float.parseFloat(args[4]);

		HashMap<String, List<String>> userPref = new HashMap<String, List<String>>();
		
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Es44").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Es44").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder1
		JavaRDD<String> input1RDD = sc.textFile(inputPath1);
		
		// Read the content of the input file/folder2
		JavaRDD<String> input2RDD = sc.textFile(inputPath2);
		
		// Read the content of the input file/folder2
		JavaRDD<String> preferences = sc.textFile(inputPath3);
		
		JavaPairRDD<String, String> prefRDD = preferences.mapToPair(line -> {
			String[] userP = line.split(",");
			return new Tuple2<String, String>(userP[0], userP[1]);
		});
		
		for(Tuple2<String, String> t : prefRDD.collect()) {
			if(userPref.containsKey(t._1())) {
				List<String> temp = userPref.get(t._1());
				temp.add(t._2());
				userPref.put(t._1(), temp);
			} else {
				// Adding for the first time
				List<String> temp = new ArrayList<String>();
				temp.add(t._2());
				userPref.put(t._1(), temp);
			}
		}
		
		final Broadcast<HashMap<String, List<String>>> userPreferences = sc.broadcast(userPref);
		
		// <Movie, Tuple2<UserId, Genre>>
		JavaPairRDD<String, Tuple2<String, String>> watchedRDD = input1RDD.mapToPair(line -> {
			String[] movie = line.split(",");
			return new Tuple2<String, String>(movie[1], movie[0]);
		}).join(input2RDD.mapToPair(line -> {
			String[] movie = line.split(",");
			return new Tuple2<String, String>(movie[0], movie[2]);
		}));
		
		JavaPairRDD<String, Float> misleadingRDD = watchedRDD.mapToPair(e -> {
			int notGen = 0;
			if(!userPreferences.getValue().get(e._2()._1()).contains(e._2()._2())) notGen++; 
			return new Tuple2<String, Tuple2<Integer, Integer>>(e._2()._1(), new Tuple2<Integer, Integer>(notGen, 1));
		}).reduceByKey((e1, e2) -> {
			return new Tuple2<Integer, Integer>(e1._1()+e2._1(), e1._2()+e2._2());
		}).mapToPair(e -> {
			return new Tuple2<String, Float>(e._1(), (float) e._2()._1()/e._2()._2());
		}).filter(e -> {
			if(e._2 > threshold) return true;
			else return false;
		});
		
		misleadingRDD.saveAsTextFile(outputPath);
		
		// Close the Spark context
		sc.close();
	}
}
