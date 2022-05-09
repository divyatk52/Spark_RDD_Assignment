package Spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class Spark_Rdd_GroupbyOrderby {
 public static void main(String[] args) throws Exception {
		 
		 System.setProperty("hadoop.home.dir", "D:\\winutils");
		 SparkConf conf= new SparkConf().setAppName("count").setMaster("local[*]");
		 JavaSparkContext sc= new JavaSparkContext(conf);
		
		//VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
		//1,2017-07-01 00:06:25,2017-07-01 00:10:50,1,1.20,1,N,249,90,1,5.5,0.5,0.5,1.35,0,0.3,8.15
			
		JavaRDD<String> trip = sc.textFile("in/trip_yellow_taxi.data"); 
		
		JavaRDD<String> lines = trip.filter(new Function<String, Boolean>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			
			public Boolean call(String s) throws Exception {
			
			if(s == null || s.trim().length() < 10 || s.startsWith("VendorID")) {
			
			return false;
			
			}
			else  
			return true;
		
			}
			
			});
		
		long count =lines.count();
		System.out.println(count);
		JavaPairRDD<Integer, Integer> pairRdd = lines.mapToPair(
				data -> {
					
					List<String> dataList = new ArrayList<String>();
					dataList = Arrays.asList(data.split(","));
					return new Tuple2<Integer, Integer>(Integer.parseInt(dataList.get(9)), 1);	
					
				}
				);
		long count1 =pairRdd.count();
		System.out.println(count1);
		
		//pairRdd.foreach(x -> System.out.println(x._1+":"+x._2));
		
		JavaPairRDD<Integer, Iterable<Integer>> groupedRdd = pairRdd.groupByKey();
		System.out.println(groupedRdd.count());
		//groupedRdd.foreach(x -> System.out.println(x._1+":"+x._2));
		JavaPairRDD<Integer, Integer> reducedRdd = pairRdd.reduceByKey((x,y)->x+y);
		System.out.println(reducedRdd.count());
		
		JavaPairRDD<Integer, Integer> sortedRdd = reducedRdd.sortByKey(new TupleSorter());
		sortedRdd.foreach(x -> System.out.println(x._1+":"+x._2));
		sc.close();
		
 }


}
