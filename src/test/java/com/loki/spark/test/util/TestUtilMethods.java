package com.loki.spark.test.util;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;

/*
 * Test Utility Class to hold static utility methods used by various test cases
 */
public class TestUtilMethods {
	private static final Logger log = Logger.getLogger(TestUtilMethods.class.getName());
	//Define default Spark properties
	//Update these properties according to your needs
	private static final String SPARK_CLUSTER = "spark://vibhav-ubuntu-desktop:7077";
	private static final String SPARK_HOME = "/opt/spark-3.1.1-bin-hadoop2.7"; 
	private static final String LOCAL_CLUSTER = "local[*]";
	private static final String JAR_NAME = "spark-java-sdk-0.0.1-SNAPSHOT.jar";
	
	//Method to build Spark Conf Instance based on given environment and properties 
	public static SparkConf buildSparkConfInstance(final String appName) {
		SparkConf conf = null;
		try {
			//Check Environment mode specified or not
			Optional<String> envOptional = Optional.fromNullable(System.getProperty("env"));
			//Build Spark Conf Instance based on runtime env specified
			if(envOptional.isPresent() && envOptional.get().equalsIgnoreCase("cluster")) {
				log.info("Building Spark Conf Instance with below properties:");
				//Get Cluster URL
				Optional<String> clusterOptional = Optional.fromNullable(System.getProperty("clusterURL"));
				String clusterURL = clusterOptional.isPresent()?clusterOptional.get():SPARK_CLUSTER;
				log.info("Cluster URL: "+clusterURL);
				//Get Spark Home
				Optional<String> sparkHomeOptional = Optional.fromNullable(System.getProperty("sparkHome"));
				String sparkHome = sparkHomeOptional.isPresent()?clusterOptional.get():SPARK_HOME;
				log.info("Spark Home: "+sparkHome);
				//Get Jar name and path
				Optional<String> jarOptional = Optional.fromNullable(System.getProperty("jarName"));
				String jarName = jarOptional.isPresent()?jarOptional.get():JAR_NAME;
				String[] jarPath = { "target/"+ jarName };
				log.info("Jar Path: "+jarPath[0]);

				//Build Spark Conf instance using above properties
				conf = new SparkConf().setAppName(appName)
						.setMaster(clusterURL)
						.setSparkHome(sparkHome)
						.setJars(jarPath);
				log.info("Built Spark Conf instance for CLUSTER Environment" );
			}
			else {
				//Build Spark COnf instance for local execution
				conf = new SparkConf().setAppName(appName)
						.setMaster(LOCAL_CLUSTER);
				log.info("Built Spark Conf instance for LOCAL Environment" );
			}
		} catch(Exception e) {
			log.error("Exception caught during Spark Conf Instance build");
			log.error(ExceptionUtils.getStackTrace(e));
		}
		return conf;
	}
	
	public static boolean deleteOutputDirectory(final String dirPath) {
		try {
			//Walk through directory structure and delete each file in it
			Files.walk(Paths.get((dirPath)))
			     .sorted(Comparator.reverseOrder())
			     .map(Path::toFile)
			     .forEach(File::delete);
			
			return true;
		} catch(Exception e) {
			log.error("Exception caught while deleting directory: "+dirPath);
			log.error(ExceptionUtils.getStackTrace(e));
			return false;
		}
	}
}
