package com.cigna.analytics.spark;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does analyzes IDOL queries.
 *
 */

public final class DirectoryIDOLLogsStreamingProcessor {

	private DirectoryIDOLLogsStreamingProcessor() {
	}

	public static void main(String[] args) {
		/*
		if (args.length < 4) {
			System.err.println("Usage: DirectoryIDOLLogsStreamingProcessor <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}
		 */

		System.setProperty("hadoop.home.dir", "c:\\winutil\\");
		final URLDecoder url = new URLDecoder();

		//SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkKafkaReceiver");
		SparkConf sparkConf = new SparkConf().setAppName("SparkKafkaReceiver");
		// Create the context with a 1 second batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));

		int numThreads = Integer.parseInt("2");
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = "idol".split(",");
		for (String topic: topics) {
			topicMap.put(topic, numThreads);
		}

		JavaPairReceiverInputDStream<String, String> messages =
				KafkaUtils.createStream(jssc, "cilidolp0007.sys.cigna.com:2181", "test-consumer-group", topicMap);

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<String> textLog = lines.filter(
				new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String x) { 
						return (x.contains("combine=fieldcheck") && x.contains("&start=1") && x.contains("a=query")); }
				}
				);

		// Load a text file and convert each line to a Java Bean.
		JavaDStream<String> log = textLog.map(
				new Function<String, String>() {

					private static final long serialVersionUID = 1L;

					@SuppressWarnings("deprecation")
					@Override
					public String call(String logline) {

						int name=0;
						String query="";
						name=logline.indexOf("a=query");
						query="/"+url.decode(logline.substring(name));
						query=query.substring(0, query.length()-14);

						return query;
					}
				});


		JavaPairDStream<String, Integer> textLogDstream = log.mapToPair(str -> new Tuple2<String,Integer>(str, 1));

		// Reduce function adding two integers, defined separately for clarity
		Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		};

		// Reduce last 30 seconds of data, every 10 seconds
		JavaPairDStream<String, Integer> windowedHitCounts = textLogDstream.reduceByKeyAndWindow(reduceFunc, Durations.seconds(43200), Durations.seconds(21600));

		//swap hit counts to be able to sort it
		JavaPairDStream<Integer, String> swappedHitCounts = windowedHitCounts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, String> call(
							Tuple2<String, Integer> in) {
						return in.swap();
					}
				});

		//sort based on count of each query string.
		JavaPairDStream<Integer, String> sortedHitCounts = swappedHitCounts
				.transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {

					private static final long serialVersionUID = 1L;

					public JavaPairRDD<Integer, String> call(
							JavaPairRDD<Integer, String> in) throws Exception {
						return in.sortByKey(false);
					}
				});

		// Get the sorted hit queries
		JavaDStream<String> logs = sortedHitCounts.map(new Function<Tuple2<Integer, String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, String> tuple2) {
				return tuple2._2();
			}
		});


		//process hits
		logs.foreachRDD(new IDOLDelegate());


		jssc.start();
		jssc.awaitTermination();


	}
}
