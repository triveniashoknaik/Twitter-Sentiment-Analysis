package com.rackspace.spark;


import org.apache.log4j.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.util.*;


import java.lang.Boolean;

/**
 *
 * Predicts the sentiment of each tweet filtered by the keywords mentioned in selection.txt.
 * Sentiment is predicted based on comparing the words in a tweet with bag of negative and positive
 * words. The final sentiment (positive/negative) is assigned based on positive and negative scores
 * assigned to it. The results are written in hdfs in twitter_data/sentiment directory.
 *
 * Build instructions:
 *
 * cd TwitterSentimentSparkDemo
 * mvn package
 *
 * This will create target folder with compiled jar: TwitterSentimentAnalysis-0.0.1.jar.
 * Change directory to target to submit the job with following commands:
 *
 * spark-submit --class com.rackspace.spark.TwitterSentiment --master yarn-cluster --num-executors 2
 * --driver-memory 1g --executor-memory 1g --executor-cores 1 TwitterSentimentAnalysis-0.0.1.jar
 * consumerKey consumerSecret accessToken accessTokenSecret hdfs_output_path
 *
 * Notes:
 * hdfs_output_path should be of this format: hdfs://nodename/user/username
 *
 */

public class TwitterSentiment {

    static {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR);
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Twitter Sentiment Analysis");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10000));

        System.setProperty("twitter4j.oauth.consumerKey", args[0]);
        System.setProperty("twitter4j.oauth.consumerSecret", args[1]);
        System.setProperty("twitter4j.oauth.accessToken", args[2]);
        System.setProperty("twitter4j.oauth.accessTokenSecret", args[3]);
        final String hdfs_output_path = args[4];

        //Prepares set of keywords by reading selection.txt file
        List<String> queryStrings = QueryWords.getWords();
        final HashSet<String> keywords = new HashSet<String>(queryStrings);

        //Creates twitter Stream
        JavaReceiverInputDStream stream = TwitterUtils.createStream(ssc);

        //Gets new Stream of RDD of the form (tweetID, tweet)
        JavaPairDStream<Long, String> tweets = stream.mapToPair(new TwitterFilterFunction());

        //Gets tweet that contains search terms
        JavaPairDStream<Long, String> filtered = tweets.filter(
                new Function<Tuple2<Long, String>, Boolean>() {
                    private static final long serialVersionUID = 42l;

                    @Override
                    public Boolean call(Tuple2<Long, String> tweet) {
                        if (tweet != null && !tweet._2().isEmpty() && tweet._1() != null) {
                            for (String key : keywords) {
                                if (tweet._2().contains(key))
                                    return true;
                            }
                        }
                        return false;
                    }
                }
        );

        // Apply text filter to remove different languages/unwanted symbols
        // outputs the stream in the format (tweetID, tweet, filteredTweet)
        JavaDStream<Tuple3<Long, String, String>> tweetsFiltered = filtered.map(new TextFilter());

        //remove the stop words from each tweet and output the stream in the format (tweetID, tweet, filteredTweet)
        tweetsFiltered = tweetsFiltered.map(new Function<Tuple3<Long, String, String>, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> call(Tuple3<Long, String, String> tweet) throws Exception {
                String filterText = tweet._3();

                List<String> stopWords = StopWords.getWords();
                for (String word : stopWords) {
                    filterText = filterText.replaceAll("\\b" + word + "\\b", "");
                }
                return new Tuple3<Long, String, String>(tweet._1(), tweet._2(), filterText);
            }
        });

        //get positive score ((tweetID, tweet, filteredTweet), posScore)
        JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets = tweetsFiltered.
        mapToPair(new PairFunction<Tuple3<Long, String, String>, Tuple2<Long, String>, Float>() {
            @Override
            public Tuple2<Tuple2<Long, String>, Float> call(Tuple3<Long, String, String> tweet) throws Exception {
                String text = tweet._3();
                Set<String> posWords = PositiveWords.getWords();
                String[] words = text.split(" ");
                int numWords = words.length;
                int numPosWords = 0;
                for (String word : words) {
                    if (posWords.contains(word))
                        numPosWords++;
                }
                return new Tuple2<Tuple2<Long, String>, Float>(
                        new Tuple2<Long, String>(tweet._1(), tweet._2()),
                        (float) numPosWords / numWords
                );
            }
        });

        //get negative score ((tweetID, tweet, filteredTweet), negScore)
        JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets = tweetsFiltered.
                mapToPair(new PairFunction<Tuple3<Long, String, String>, Tuple2<Long, String>, Float>() {
                    @Override
                    public Tuple2<Tuple2<Long, String>, Float> call(Tuple3<Long, String, String> tweet) throws Exception {
                        String text = tweet._3();
                        Set<String> negWords = NegativeWords.getWords();
                        String[] words = text.split(" ");
                        int numWords = words.length;
                        int numNegWords = 0;
                        for (String word : words) {
                            if (negWords.contains(word))
                                numNegWords++;
                        }
                        return new Tuple2<Tuple2<Long, String>, Float>(
                                new Tuple2<Long, String>(tweet._1(), tweet._2()),
                                (float) numNegWords / numWords
                        );
                    }
                });

        //Perform join of positiveTweet and negativeTweet
        JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> joined =
                positiveTweets.join(negativeTweets);

        //Transform into 4 element tuple for simplification.
        //Outputs the new stream in the format (tweetID, tweet, posScore, negScore)
        JavaDStream<Tuple4<Long, String, Float, Float>> scoredTweets =
                joined.map(new Function<Tuple2<Tuple2<Long, String>,
                        Tuple2<Float, Float>>,
                        Tuple4<Long, String, Float, Float>>() {
                    private static final long serialVersionUID = 42l;

                    @Override
                    public Tuple4<Long, String, Float, Float> call(
                            Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>> tweet) {
                        return new Tuple4<Long, String, Float, Float>(
                                tweet._1()._1(),
                                tweet._1()._2(),
                                tweet._2()._1(),
                                tweet._2()._2());
                    }
                });

        //Filter out neutral/unwanted results
        JavaDStream<Tuple4<Long, String, Float, Float>> filteredScoredTweets = scoredTweets.
                filter(new Function<Tuple4<Long, String, Float, Float>, Boolean>() {
                    @Override
                    public Boolean call(Tuple4<Long, String, Float, Float> scored_tweet) throws Exception {
                        return (scored_tweet._3() > scored_tweet._4() ||
                                scored_tweet._3() < scored_tweet._4() ||
                                ((scored_tweet._3() == scored_tweet._4()) &&
                                        (scored_tweet._3() > 0.0 && scored_tweet._4() > 0.0)));
                    }
                });

        //Identify winning sentiment and assign it to result
        //Outputs the new stream in the format (tweetID, tweet, posScore, negScore, sentiment)
        JavaDStream<Tuple5<Long, String, Float, Float, String>> result =
                filteredScoredTweets.map(new ScoreTweetsFunction());

        //Write the result to HDFS
        result.foreach(new Function2<JavaRDD<Tuple5<Long, String, Float, Float, String>>, Time, Void>() {
            @Override
            public Void call(JavaRDD<Tuple5<Long, String, Float, Float, String>> tuple5JavaRDD, Time time)
                    throws Exception {
                if (tuple5JavaRDD.count() <= 0) return null;
                tuple5JavaRDD.saveAsTextFile(hdfs_output_path + "/twitter_data/sentiment/sentiment"
                        + "_" + time.milliseconds());
                return null;
            }
        });

        result.print();
        ssc.start();
        ssc.awaitTermination();
    }
}

