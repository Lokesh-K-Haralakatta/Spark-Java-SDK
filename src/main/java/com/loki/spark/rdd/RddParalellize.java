package com.loki.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

/* Sample Spark Java Module to illustrate creation of RDD using Parallelize method 
 * Methods to perform various transformations and actions are also included in this module
 */

public class RddParalellize {
	private static Logger log = Logger.getLogger(RddParalellize.class);
	
	private static JavaSparkContext jsc;
	
	public RddParalellize(SparkConf conf) {
		//Construct Java Spark Context
		try {
			jsc = new JavaSparkContext(conf);
		} catch(Exception e) {
			log.error("Exception caught during Spark Context creation");
			log.error(ExceptionUtils.getStackTrace(e));
		}
	}
	
	/*
	 * Method uses parallelize() to create Distributed Data set
	 * Sets minimum of 2 partitions for sum computation
	 * Applies filter transformation to select even numbers
	 * Applies reduce action to compute even numbers sum
	 */
	public Integer computeEvenSum(List<Integer> list) {
		Integer evenSum = null;
		try {
			//Create parallel distributed set from list
			JavaRDD<Integer> listRdd = jsc.parallelize(list);

			//Define 2 partitions to compute the required sum
			listRdd.repartition(2);

			//Filter out all odd numbers present in the given RDD
			JavaRDD<Integer> evensRdd = listRdd.filter(x -> ((x%2) == 0));
			
			//Compute sum of all evens using reduce
			evenSum = evensRdd.reduce((a,b) -> a+b);
		} catch(Exception e) {
			log.error("Exception caught during computeEvenSum");
			log.error(ExceptionUtils.getStackTrace(e));
		}
		return evenSum;
	}
	
	/*
	 * Method uses parallelize() to create Distributed Data set
	 * Sets minimum of 2 partitions for sum computation
	 * Applies filter transformation to select odd numbers
	 * Applies reduce action to compute odd numbers sum
	 */
	public Integer computeOddSum(List<Integer> list) {
		Integer oddSum = null;
		try {
			//Create parallel distributed set from list
			JavaRDD<Integer> listRdd = jsc.parallelize(list);

			//Define 2 partitions to compute the required sum
			listRdd.repartition(2);

			//Filter out all numbers numbers present in the given RDD
			JavaRDD<Integer> oddsRdd = listRdd.filter(x -> ((x%2) != 0));
			
			//Compute sum of all odds using reduce
			oddSum = oddsRdd.reduce((a,b) -> a+b);
		} catch(Exception e) {
			log.error("Exception caught during computeOddSum");
			log.error(ExceptionUtils.getStackTrace(e));
		}
		return oddSum;
	}

	/*
	 * Method uses parallelize() to create Distributed Data set
	 * Sets minimum of 2 partitions for count computation
	 * Applies map transformation to initialize each item as 1
	 * Applies reduce by key transformation to compute each item count
	 * Applies collectAsMap to collect the result and return
	 */
	public Map<Integer,Integer> computeEachItemCount(List<Integer> list) {
		Map<Integer,Integer> countMap = new HashMap<>(list.size());
		try {
			//Create parallel distributed set from list
			JavaRDD<Integer> listRdd = jsc.parallelize(list);

			//Define 2 partitions to compute the required sum
			listRdd.repartition(2);

			//Map each item in RDD to pair of (x,1)
			JavaPairRDD<Integer,Integer> mapRdd = listRdd.mapToPair(x -> new Tuple2<>(x,1));
			
			//Apply ReduceByKey to count each of item in mapRdd
			JavaPairRDD<Integer,Integer> reduceRdd = mapRdd.reduceByKey((a,b) -> a+b);
			
			//Collect result into Map
			countMap = reduceRdd.collectAsMap();
		} catch(Exception e) {
			log.error("Exception caught during computeEachItemCount");
			log.error(ExceptionUtils.getStackTrace(e));
		}
		return countMap;
	}
	
	/*
	 * Method uses parallelize() to create Distributed Data set
	 * Sets minimum of 2 partitions for count computation
	 * Applies flat map transformation to split digits as separate items
	 * Applies map transformation to pair each digit with 1 
	 * Applies reduce by key transformation to compute digits count
	 * Applies collectAsMap action to collect the result as Map
	 */
	public Map<Integer,Integer> computeEachDigitCount(List<Integer> list) {
		Map<Integer,Integer> digitsCountMap = new HashMap<>();
		try {
			//Create parallel distributed set from list
			JavaRDD<Integer> listRdd = jsc.parallelize(list);

			//Define 2 partitions to compute the required sum
			listRdd.repartition(2);
			
			//Apply flapMap on each number to separate out digits
			JavaRDD<Integer> digitsRdd = listRdd.flatMap(x -> {
				List<Integer> digits = new ArrayList<>();
				while(x != 0) {
					digits.add(x%10);
					x /= 10;
				}
				return digits.iterator();
			});

			//Map each item in digitsRDD to pair of (x,1)
			JavaPairRDD<Integer,Integer> mapRdd = digitsRdd.mapToPair(x -> new Tuple2<>(x,1));
			
			//Apply ReduceByKey to count each of item in mapRdd
			JavaPairRDD<Integer,Integer> reduceRdd = mapRdd.reduceByKey((a,b) -> a+b);
			
			//Collect result into Map
			digitsCountMap = reduceRdd.collectAsMap();
		} catch(Exception e) {
			log.error("Exception caught during computeDigitsCount");
			log.error(ExceptionUtils.getStackTrace(e));
		}
		return digitsCountMap;
	}
	
	/*
	 * Method uses parallelize() to create Distributed Data set
	 * Sets minimum of 2 partitions for count computation
	 * Applies flat map transformation to split digits as separate items
	 * Applies map transformation to pair each digit with 1 
	 * Applies groupByKey transformation to group all digits for key into sequence
	 * Applies mapToPair transformation to compute sum of all digits for each key
	 * Applies saveAsTextFile action to save results into given out file
	 */
	public boolean computeEachDigitSum(List<Integer> list, final String outFilePath) {
		Boolean writeStatus = false;
		try {
			//Create parallel distributed set from list
			JavaRDD<Integer> listRdd = jsc.parallelize(list);

			//Define 2 partitions to compute the required sum
			listRdd.repartition(2);
			
			//Apply flapMap on each number to separate out digits
			JavaRDD<Integer> digitsRdd = listRdd.flatMap(x -> {
				List<Integer> digits = new ArrayList<>();
				while(x != 0) {
					digits.add(x%10);
					x /= 10;
				}
				return digits.iterator();
			});

			//Map each item in digitsRDD to pair of (x,1)
			JavaPairRDD<Integer,Integer> mapRdd = digitsRdd.mapToPair(x -> new Tuple2<>(x,1));
			
			//Apply GroupByKey to group each of item in to a sequence from mapRdd
			JavaPairRDD<Integer,Iterable<Integer>> grpRdd = mapRdd.groupByKey();
			
			//Apply Map to compute sum of all items in each group
			//Save results to given output file
			grpRdd.mapToPair(tuple2 -> {
											Integer sum = 0;
											Integer key = tuple2._1;
											Iterable<Integer> seq = tuple2._2;
											for(int x: seq)
												sum += x*key;
											return new Tuple2<>(key,sum);
								}).saveAsTextFile(outFilePath);
			writeStatus = true;
		} catch(Exception e) {
			log.error("Exception caught during computeEachDigitSum");
			log.error(ExceptionUtils.getStackTrace(e));
		}
		return writeStatus;
	}

  }