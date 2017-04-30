package core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import utils.Parse;
import utils.Tweet;

public class TweetInvertedIndex {
	
	private static String inputFilePath;
	private static String outputFolderPath;
	
	public TweetInvertedIndex(String inFile, String outPath) {
		this.inputFilePath = inFile;
		this.outputFolderPath = outPath;
	}
	
	public static void main(String[] args) {
		
		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}
		
		TweetInvertedIndex tii = new TweetInvertedIndex(args[0], args[1]);
		tii.run();
	}

	private void run() {
		
		SparkConf conf = new SparkConf().setAppName(this.getClass().getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(inputFilePath);

		JavaRDD<Tweet> tweets = data.map(line -> Parse.parseJsonToTweet(line));
		
		JavaRDD<List<Tuple2<String,Tweet>>> hashtagTweetList = 
				tweets
				.map(tweet ->{
					String text = tweet.getText().replace("\n"," ");
					tweet.setText(text);
					
					List<String> words = Arrays.asList(text.split(" "));
					List<Tuple2<String,Tweet>> result = new ArrayList<>();
					Set<String> hashtags = new HashSet<String>();
					
					for(String word : words) {
						if(word.startsWith("#") && word.length() > 1) {
							hashtags.add(word);
						}
					}
						
					for(String hashtag : hashtags) {
						Tuple2<String,Tweet> item = new Tuple2<>(hashtag,tweet);
						result.add(item);
					}
				
					return result;
				});
		
		List<Tuple2<String,Tweet>> allHashtagTweetList = hashtagTweetList.reduce((x,y) -> {
			x.addAll(y);
			return x;
		});
		
		//JavaPairRDD<String,Tuple1<Tweet>> hashtagTweetPairRDD =
		JavaPairRDD<String,Tweet> hashtagTweetPairRDD = 
				sc.parallelize(allHashtagTweetList)
				.mapToPair(line -> {
					String key = line._1();
					Tweet value = line._2();
					
					//return new Tuple2<>(key,new Tuple1<>(value));
					return new Tuple2<>(key,value);
				});
		
		//JavaPairRDD<String,Iterable<Tuple1<Tweet>>> invertedIndex =
		JavaPairRDD<String,Iterable<Tweet>> invertedIndex =
				hashtagTweetPairRDD
				.groupByKey()
				.sortByKey();
		
		invertedIndex.saveAsTextFile(outputFolderPath);
		
		sc.close();
		sc.stop();
	}
}
