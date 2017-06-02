package it.uniroma3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class TopNTwitterHashTag {

	public static void main(String[] args) {
		if (args.length < 5) {
			System.err.println("Usage: TopNTwitterHashTag <consumer key>" +
					" <consumer secret> <access token> <access token secret> <top hashtags number> [<filters>]");
			System.exit(1);
		}
		
		//StreamingExamples.setStreamingLogLevels();
	    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
	    if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
	      Logger.getRootLogger().setLevel(Level.WARN);
	    }

	    String consumerKey = args[0];
	    String consumerSecret = args[1];
	    String accessToken = args[2];
	    String accessTokenSecret = args[3];
	    int topHashtagNum = Integer.parseInt(args[4]);
	    String[] filters = Arrays.copyOfRange(args, 5, args.length);

	    // Set the system properties so that Twitter4j library used by Twitter stream
	    // can use them to generate OAuth credentials
	    System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
	    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
	    System.setProperty("twitter4j.oauth.accessToken", accessToken);
	    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

	    SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterHashTagJoinSentiments");

	    // check Spark configuration for master URL, set it to local if not configured
	    if (!sparkConf.contains("spark.master")) {
	      sparkConf.setMaster("local[2]");
	    }

	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
	    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);
	    
	    JavaPairDStream<String, Tuple2<Integer,List<Status>>> hashtagOneStatus = 
	    		stream.flatMapToPair(status -> {
	    			
	    			List<Tuple2<String,Tuple2<Integer,List<Status>>>> result = new ArrayList<>();
	    			List<String> words = Arrays.asList(status.getText().split(" "));
	    			Set<String> hashtags = new HashSet<>(); //remove same hashtags in the same tweet
	    			
	    			for(String word : words) {
	    				if(word.startsWith("#") && word.length() > 1) {
	    					hashtags.add(word.substring(1)); 
	    				}
	    			}
	    			
	    			for(String hashtag : hashtags) {
	    				List<Status> list = new ArrayList<>();
	    				list.add(status);
	    				
	    				Tuple2<Integer,List<Status>> value = new Tuple2<>(1,list);
	    				Tuple2<String,Tuple2<Integer,List<Status>>> line = new Tuple2<>(hashtag,value);
    					result.add(line);
	    			}
	    			
	    			return result.iterator();
	    		});
	    
	    JavaPairDStream<String, Tuple2<Integer,List<Status>>> hashtagCount =
	    		hashtagOneStatus
	    		.reduceByKeyAndWindow((x,y) -> {
	    			
	    			int sum = x._1() + y._1();
	    			x._2().addAll(y._2());
	    			return new Tuple2<Integer,List<Status>>(sum,x._2());
	    			
	    		}, new Duration(10000));
	    
	    JavaPairDStream<Integer,Tuple2<String,List<Status>>> hashtagPreTopN =
	    		hashtagCount
	    		.mapToPair(line -> {
	    			String hashtag = line._1();
	    			int occNum = line._2()._1();
	    			List<Status> list = line._2()._2();
	    			
	    			Tuple2<String,List<Status>> value = new Tuple2<>(hashtag,list);
	    			Tuple2<Integer,Tuple2<String,List<Status>>> result = new Tuple2<>(occNum,value);
	    			
	    			return result;
	    		});
	    
	    JavaPairDStream<Integer,Tuple2<String,List<Status>>> hashtagSorted =
	    		hashtagPreTopN.transformToPair(rdd -> {
	    			return rdd.sortByKey(false);
	    		});
	    
	    
	    // Print top N hashtags with rispettive tweets and number of occurrences 
	    hashtagSorted.foreachRDD(rdd -> {
	    	List<Tuple2<Integer,Tuple2<String,List<Status>>>> topList = rdd.take(topHashtagNum);
	    	
	    	System.out.println(
	    			String.format("\nTop %s hashtags in last 10 seconds (%s total):",
	    					topHashtagNum, rdd.count())
	    			);
	    	
	    	for(Tuple2<Integer,Tuple2<String,List<Status>>> foo : topList) {
	    		int num = foo._1();
	    		String hashtag = foo._2()._1();
	    		List<Status> statusList = foo._2()._2();
	    		System.out.println(
	    				String.format("\nHashtag: %s\nOccurrences: %s", hashtag, num)
	    				);
	    		
	    		System.out.println("\nTweets: ");
	    		int count = 0;
	    		for(Status stat : statusList) {
	    			count++;
	    			System.out.println(
	    					String.format("%s. %s", count, stat)
	    					);
	    		}
	    	}
	    });
	    
	    jssc.start();
	    try {
	      jssc.awaitTermination();
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    }
	}
}
