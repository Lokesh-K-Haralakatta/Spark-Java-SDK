package com.loki.spark.rdd.test;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.loki.spark.rdd.RddParalellize;
import com.loki.spark.test.util.TestUtilMethods;

/*
 * Test Class to validate results from RddParallelize Operations
 * Supported JVM properties to override default values
 * -Denv=cluster to run tests on cluster environment
 * 		-DclusterURL=url to specify cluster to run against when env=cluster
 * 		-DsparkHome=sparkHomePath to specify Spark Installation Path on worker nodes when env=cluster
 * 		-DjarName=spark-java-jar expected to be present in target directory when env=cluster
 * -Denv=local to run tests on local environment
 */
public class RddParalellizeTest {
	private static final Logger log = Logger.getLogger(RddParalellizeTest.class.getName());
	private static final String APP_NAME = "RddParalellizeOps"; 
	
	private static RddParalellize rddOps = null;
	
	@BeforeClass
	public static void setUpBeforeClass() {
		try {
			//Get Spark Conf Instance based on given properties
			SparkConf conf = TestUtilMethods.buildSparkConfInstance(APP_NAME);
			//Build RddParalellize Instance using Spark Conf Instance
			rddOps = new RddParalellize(conf);
		} catch(Exception e) {
			log.error("Exception caught during setup");
			log.error(ExceptionUtils.getStackTrace(e));
			assert(false);
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		
	}

	@Test
	public void testComputeEvenSum() {
		try {
			//Define list of numbers
			List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9);

			//Compute sum of even numbers present in parallel list
			Integer evenSum = rddOps.computeEvenSum(list);

			//Validate sum of even numbers in list
			assertEquals(evenSum, new Integer(20));
		} catch(Exception e) {
			log.error("Exception caught during testing ComputeEvenSum");
			log.error(ExceptionUtils.getStackTrace(e));
			assert(false);
		}
	}

	@Test
	public void testComputeOddSum() {
		try {
			//Define list of numbers
			List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9);

			//Compute sum of odd numbers present in parallel list
			Integer oddSum = rddOps.computeOddSum(list);

			//Validate sum of odd numbers in list
			assertEquals(oddSum, new Integer(25));
		} catch(Exception e) {
			log.error("Exception caught during testing ComputeOddSum");
			log.error(ExceptionUtils.getStackTrace(e));
			assert(false);
		}
	}

	@Test
	public void testComputeEachItemCount() {
		try {
			//Define list of numbers
			List<Integer> list = Arrays.asList(11,12,13,14,15,16,17,18,19,23,11,24,13,15,14,16,17,18);

			//Compute count of each item present in parallel list
			Map<Integer,Integer> countMap = rddOps.computeEachItemCount(list);

			//Validate count of item in countMap matches with as present in provided list of numbers
			assertTrue("",countMap.get(11) == 2);
			assertTrue("",countMap.get(12) == 1);
			assertTrue("",countMap.get(16) == 2);
			assertTrue("",countMap.get(23) == 1);
		} catch(Exception e) {
			log.error("Exception caught during testing ComputeEachItemCount");
			log.error(ExceptionUtils.getStackTrace(e));
			assert(false);
		}
	}

	@Test
	public void testComputeEachDigitCount() {
		try {
			//Define list of numbers
			List<Integer> list = Arrays.asList(1234,34125,4534);

			//Compute each digit count present in items from parallel list
			Map<Integer,Integer> countMap = rddOps.computeEachDigitCount(list);

			//Validate count of item in countMap matches with expected digit count
			assertTrue("",countMap.get(1) == 2);
			assertTrue("",countMap.get(2) == 2);
			assertTrue("",countMap.get(3) == 3);
			assertTrue("",countMap.get(4) == 4);
			assertTrue("",countMap.get(5) == 2);
		} catch(Exception e) {
			log.error("Exception caught during testing ComputeEachDigitCount");
			log.error(ExceptionUtils.getStackTrace(e));
			assert(false);
		}
	}

	@Test
	public void computeEachDigitSum() {
		try {
			//Remove digitSum output directory if already exists
			String digitSumOutput = "digitSum";
			assert(TestUtilMethods.deleteOutputDirectory(digitSumOutput));
			
			//Define list of numbers
			List<Integer> list = Arrays.asList(1234,34125,4534);

			//Compute each digit sum present in items from parallel list
			//Result is written to given output directory
			Boolean status = rddOps.computeEachDigitSum(list,digitSumOutput);

			//Validate result write status is true
			assert(status);
			//Validate digitSumOutput directory created
			assert(Files.exists(Paths.get(digitSumOutput)));
		} catch(Exception e) {
			log.error("Exception caught during testing ComputeEachDigitSum");
			log.error(ExceptionUtils.getStackTrace(e));
			assert(false);
		}
	}

}
