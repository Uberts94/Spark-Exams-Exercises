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
		String outputPath;
		
		inputPath1=args[0];
		inputPath2=args[1];
		outputPath=args[2];


	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Es42").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Es42").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		JavaRDD<String> input1RDD = sc.textFile(inputPath1);
		JavaRDD<String> input2RDD = sc.textFile(inputPath2);
		
		// Questions
		JavaPairRDD <String, String> questionsRDD = input1RDD.mapToPair(line -> {
			String[] question = line.split(",");
			return new Tuple2<String, String>(question[0], question[2]);
		});
		
		// Answers
		JavaPairRDD <String, String> answersRDD = input2RDD.mapToPair(line -> {
			String[] answer = line.split(",");
			return new Tuple2<String, String>(answer[1], answer[3]);
		});

		JavaPairRDD <String, Tuple2<String, List<String>>> outputRDD = questionsRDD.join(answersRDD).mapToPair(e -> {
			List<String> answer = new ArrayList<String>();
			answer.add(e._2()._2());
			return new Tuple2<String, Tuple2<String, List<String>>>(e._1(), new Tuple2<String, List<String>>(e._2()._1(), answer));
		}).reduceByKey((e1, e2) -> {
			if(!e2._2().isEmpty()) e1._2.add(e2._2.get(0));
			return e1;
		});
		
		outputRDD.saveAsTextFile(outputPath);
		
		// Close the Spark context
		sc.close();
	}
}
