package Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Spark_RDD_SingleRecord {
	public static void main(String[] args) throws Exception {
		 
		 System.setProperty("hadoop.home.dir", "D:\\winutils");
		 SparkConf conf= new SparkConf().setAppName("count").setMaster("local[*]");
		 JavaSparkContext sc= new JavaSparkContext(conf);
		
		//VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
		//1,2017-07-01 00:06:25,2017-07-01 00:10:50,1,1.20,1,N,249,90,1,5.5,0.5,0.5,1.35,0,0.3,8.15
			
		JavaRDD<String> trip = sc.textFile("in/trip_yellow_taxi.data"); 
		
		JavaRDD<String> filtereddataRDD = trip.filter(new Function<String, Boolean>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			
			public Boolean call(String s) throws Exception {
			
			if(s == null || s.trim().length() < 1) {
			
			return false;
			
			}else
			{
				String[] rec = s.split(",");
				if(rec[0].equals("2") && rec[1].equals("2017-10-01 00:15:30") && rec[2].equals("2017-10-01 00:25:11") && rec[3].equals("1") && rec[4].equals("2.17") )
			
					return true;
			else 
					return false;
			}
			}
			
			});
		
		
		filtereddataRDD.foreach(x -> System.out.println(x));
		
		long countRDD =filtereddataRDD.count();
		System.out.println(countRDD);
		sc.close();
		 }
	


}
