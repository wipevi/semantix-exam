package com.exam;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class app {

	public static void main(String[] args) {

		// Spark configuration
		SparkConf conf = new SparkConf().setMaster("local").setAppName("exam");
		JavaSparkContext ctx = new JavaSparkContext(conf);
			
		// Load text file into RDD according of month extraction
		JavaRDD<String> lineRDD = ctx.textFile("C:/Users/villa/Downloads/Desafio/access_log_*95");
		
		
		// Group lines based on host info
		JavaPairRDD<String, Integer> groupByHostRDD = lineRDD
				.mapToPair(s -> new Tuple2<String, Integer>(s.split(" ")[0], 1)); 
		
		// Calculate the number of distinct																					// hosts
		JavaPairRDD<String, Integer> hostGroupedRDD = groupByHostRDD.reduceByKey((x, y) -> x + y);

		System.out.println("# unique hosts: " + hostGroupedRDD.count());
		
		
		
		// --------------------------------------------------------------------------------

		// Filter the total value of error 404 in http request log = 20901
		JavaRDD<String> errorFilteredRDD = lineRDD.filter(s -> s.contains(" 404 "));

		System.out.println("qtde linhas filtradas " + errorFilteredRDD.count());

		
		
		// --------------------------------------------------------------------------------

		// Top 5 URLs with 404 error 
		JavaPairRDD<String, Integer> groupByerrorRDD = errorFilteredRDD
				.mapToPair(s -> new Tuple2<String, Integer>(s.split(" ")[0], 1));
		
		// Calculate the number of distinct hosts
		JavaPairRDD<String, Integer> errorGroupedRDD = groupByerrorRDD.reduceByKey((x, y) -> x + y);

		// Invert the values of key/value to value/key, it's needed to sort by value which represents top 5 hosts 
		JavaPairRDD<Integer, String> invertedRDD = errorGroupedRDD.mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1));
		List<Tuple2<Integer, String>> sortedList = invertedRDD.sortByKey(false).take(5);

		for (Tuple2<Integer, String> list : sortedList) {
			System.out.println("host = " + list._2());
			System.out.println("total error 404 = " + list._1());
		}
		
		/* Result: 
			host = hoohoo.ncsa.uiuc.edu
			total error 404 = 251
			host = piweba3y.prodigy.com
			total error 404 = 157
			host = jbiagioni.npt.nuwc.navy.mil
			total error 404 = 132
			host = piweba1y.prodigy.com
			total error 404 = 114
			host = www-d4.proxy.aol.com
			total error 404 = 91
		 */

		// --------------------------------------------------------------------------------

		// Total bytes 

		
		ctx.close();

	}

}
