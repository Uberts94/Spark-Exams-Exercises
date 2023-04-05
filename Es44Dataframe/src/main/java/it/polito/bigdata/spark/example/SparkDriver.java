package it.polito.bigdata.spark.example;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
	
public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		String inputPath1;
		String inputPath2;
		String outputPath;
		float threshold;
		
		inputPath=args[0];
		inputPath1=args[1];
		inputPath2=args[2];
		outputPath=args[3];
		threshold=Float.parseFloat(args[4]);
		
		SparkSession ss = SparkSession.builder().master("local").appName("Es32 - dataset").getOrCreate();
		
		DataFrameReader dfr = ss.read().option("delimiter", ",").format("csv").option("header", false)
								.option("inferSchema", true);
		
		Dataset<Row> watchedMovies = dfr.load(inputPath).selectExpr("_c0 as userId", "_c1 as movieId");
		
		Dataset<Row> preferences = dfr.load(inputPath1).selectExpr("_c0 as userId", "_c1 as genre");
		
		Dataset<Row> movies = dfr.load(inputPath2).selectExpr("_c0 as movie", "_c2 as genre");
		
		HashMap<String, List<String>> userPref = new HashMap<String, List<String>>();
		
		preferences.collectAsList().forEach(p -> {
			List<String> genres;
			String userid = new String(p.getString(0));
			if(userPref.containsKey(userid)) {
				genres = userPref.get(userid);
				genres.add(p.getString(1));
				userPref.put(userid, genres);
			} else {
				genres = new ArrayList<String>();
				genres.add(p.getString(1));
				userPref.put(userid, genres);
			}
		});
		
		Dataset<Row> watchedMovieGen = watchedMovies.join(movies, watchedMovies.col("movieId").equalTo(movies.col("movie")))
					.select("userId", "movie", "genre");
		
		Dataset<Row> totalByUserId = watchedMovieGen.groupBy("userId").count().selectExpr("userId", "count as totalMovies");
		
		Dataset<Row> notGenre = watchedMovieGen.filter(row -> {
			if(!userPref.get(row.getAs("userId")).contains(row.getAs("genre"))) return true;
			else return false;
		}).groupBy("userId").count().selectExpr("userId as user", "count as notGenre");
		
		Dataset<Row> misleadingProfile = notGenre.join(totalByUserId, notGenre.col("user").equalTo(totalByUserId.col("userId")))
											.select("user", "notGenre", "totalMovies");
		
		misleadingProfile.selectExpr("user", "notGenre/totalMovies as percentage")
		.filter(row -> {
			if(row.getDouble(row.fieldIndex("percentage")) > threshold) return true;
			else return false;
		})
		.javaRDD().coalesce(1).saveAsTextFile(outputPath);
		
		// Close the Spark context
		ss.close();
	}
}
