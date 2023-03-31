package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

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


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Es46").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		JavaPairRDD<String, Tuple2<String, Float>> readingsRDD = inputRDD.flatMapToPair(line -> {
			List<Tuple2<String, Tuple2<String, Float>>> list = new ArrayList<Tuple2<String, Tuple2<String, Float>>>();
			Tuple2<String, Float> current;
			
			String[] entry = line.split(",");
			current = new Tuple2<String, Float>(entry[0], Float.parseFloat(entry[1]));
			
			list.add(new Tuple2<String, Tuple2<String, Float>>(entry[0], current));
			
			Tuple2<String, Float> prec1 = new Tuple2<String, Float>(""+(Integer.parseInt(entry[0])-60), Float.parseFloat(entry[1]));
			Tuple2<String, Float> prec2 = new Tuple2<String, Float>(""+(Integer.parseInt(entry[0])-120), Float.parseFloat(entry[1]));
			
			list.add(new Tuple2<String, Tuple2<String, Float>>(entry[0], prec1));
			list.add(new Tuple2<String, Tuple2<String, Float>>(entry[0], prec2));
			
			return list.iterator();
		});
		
		JavaPairRDD<String, Iterable<Tuple2<String, Float>>> timestampsWindowsRDD = readingsRDD.groupByKey();
		
		JavaRDD<Iterable<Tuple2<String, Float>>> windowsRDD = timestampsWindowsRDD.values();
		
		JavaRDD<Iterable<Tuple2<String, Float>>> selectedRDD = windowsRDD.filter(list -> {
			HashMap<Integer, Float> timestamps = new HashMap<Integer, Float>();
			
			int min = Integer.MAX_VALUE;
			
			for(Tuple2<String, Float> element : list) {
				timestamps.put(Integer.parseInt(element._1()), element._2());
				if(Integer.parseInt(element._1()) < min) min = Integer.parseInt(element._1());
			}
			
			boolean increasing = true;
			
			if(timestamps.size() == 3) {
				for(int ts = min + 60; ts <= min + 120 && increasing == true; ts += 60)
					if(timestamps.get(ts) <= timestamps.get(ts-60)) increasing = false;
			} else increasing = false;
			
			return increasing;
		});		
		
		selectedRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
