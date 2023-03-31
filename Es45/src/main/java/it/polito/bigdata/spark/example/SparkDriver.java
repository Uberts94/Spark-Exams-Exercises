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
		

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Es45").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Es45").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder 1
		JavaRDD<String> input1RDD = sc.textFile(inputPath1);

		// Read the content of the input file/folder 2
		JavaRDD<String> input2RDD = sc.textFile(inputPath2);
		
		// Read the content of the input file/folder 3
		JavaRDD<String> input3RDD = sc.textFile(inputPath3);
		
		// Mapping to pair input3RDD
		JavaPairRDD<String, String> mappedPref = input3RDD.mapToPair(line -> {
			String[] userPref = line.split(",");
			return new Tuple2<String, String>(userPref[0], userPref[1]);
		});
		
		// HashMap<User, List<Preferred Genres>
		HashMap<String, List<String>> preferences = new HashMap<String, List<String>>();
		
		// Updating HashMap
		for(Tuple2<String, String> entry : mappedPref.collect()) {
			if(!preferences.containsKey(entry._1())) {
				List<String> genres = new ArrayList<String>();
				genres.add(entry._2());
				preferences.put(entry._1(), genres);
			} else {
				List<String> genres = preferences.get(entry._1());
				genres.add(entry._2());
				preferences.put(entry._1(), genres);
			}
		}
		
		// Creating a Broadcast map
		final Broadcast<HashMap<String, List<String>>> userPreferences = sc.broadcast(preferences);
		
		// Tuple2<UserID, Tuple2<Movie, Genre>
		JavaPairRDD <String, Tuple2<String, String>> watchedMoviesRDD = input1RDD.mapToPair(line -> {
			String[] movie = line.split(",");
			// Tuple2<Movie, UserID>
			return new Tuple2<String, String>(movie[1], movie[0]);
		}).join(input2RDD.mapToPair(line -> {
			String[] movie = line.split(",");
			// Tuple2<Movie, Genre>
			return new Tuple2<String, String>(movie[0], movie[2]);
		})).mapToPair(e -> {
			// Tuple2<UserID, Tuple2<Movie, Genre>
			return new Tuple2<String, Tuple2<String, String>>(e._2()._1(), new Tuple2<String, String>(e._1(), e._2()._2()));
		});
		
		// Tuple2<UserID, notPreferred/total>
		JavaPairRDD<String, Float> misleadingUsersRDD = watchedMoviesRDD.mapToPair(e -> {
			int notGen = 0;
			if(!userPreferences.getValue().get(e._1()).contains(e._2()._2())) notGen++; 
			return new Tuple2<String, Tuple2<Integer, Integer>>(e._1(), new Tuple2<Integer, Integer>(notGen, 1));
		}).reduceByKey((e1, e2) -> {
			return new Tuple2<Integer, Integer>(e1._1()+e2._1(), e1._2()+e2._2());
		}).filter(e -> {
			if(e._2()._1() >= 5) return true;
			else return false;
		}).mapToPair(e -> {
			return new Tuple2<String, Float>(e._1(), (float) e._2()._1()/e._2()._2());
		}).filter(e -> {
			if(e._2 > threshold) return true;
			else return false;
		});

		JavaPairRDD<String, String> misleadingUsGenre = misleadingUsersRDD.join(watchedMoviesRDD).filter(movie -> {
			if(userPreferences.getValue().get(movie._1()).contains(movie._2()._2()._2())) return false;
			else return true;
		}).mapToPair(movie -> {
			return new Tuple2<String, String>(movie._1(), movie._2()._2()._2());
		});
		
		misleadingUsGenre.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
