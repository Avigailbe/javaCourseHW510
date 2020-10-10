package home_work510;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;


public class TaxiService implements java.io.Serializable{
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("taxi");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rawDataRdd = sc.textFile("data/taxi_orders.txt");

        //print each line of file
        List<String> list =rawDataRdd.collect();
        list.forEach(System.out::println);

        //count lines in file
        System.out.println("The number of lines in the file are: "+rawDataRdd.count());

        //get lines with boston
        JavaPairRDD<String,Long> bostonDataRdd = rawDataRdd.map(line->line.split(" "))
                .mapToPair(s -> new Tuple2<String, Long>(s[1],Long.parseLong(s[2])))
                .filter(string ->(string._1.equalsIgnoreCase("boston")));

        JavaPairRDD<String,Long> boston10Rdd = bostonDataRdd.filter(stringLongTuple2 -> (stringLongTuple2._2>10));
        System.out.println("The amount of trips to Boston longer than 10KM is: "+boston10Rdd.count());

        //get sum of km for lines with boston
        double sum = bostonDataRdd.mapToDouble(stringLongTuple2 -> stringLongTuple2._2).sum();
        System.out.println("sum of km for all trips to boston "+sum);

        //names of 3 drivers with ****max total kilometers
        JavaPairRDD<Long, Long> driverRdd = rawDataRdd.map(line->line.split(" "))
                .mapToPair(s -> new Tuple2<Long, Long>(Long.parseLong(s[0]),Long.parseLong(s[2])))
                .reduceByKey((l1,l2)->l1+l2)
                .mapToPair(l -> l.swap())
                .sortByKey(false)
                .mapToPair(x->x.swap());
        //.sortByKey(false);
        JavaRDD<String> rawNamesRdd = sc.textFile("data/drivers.txt");

        //get names of drivers
        JavaPairRDD<Long,String> namesDataRdd = rawNamesRdd.map(line->line.split(", "))
                .mapToPair(s -> new Tuple2<Long, String>(Long.parseLong(s[0]),s[1]));

        //join names with data and print
        JavaPairRDD<Long, Tuple2<Long, String>> combined = driverRdd.join(namesDataRdd);
        combined.take(3).forEach(data ->{
            System.out.println(data._1()+" - sum of km is:"+data._2._1+" for driver: "+data._2._2 );
        });

        //todo
        //Count number of lines
        //Count amount of trips to Boston longer than 10 km
        //Calculate sum of all kilometers trips to Boston
        //Write names of 3 drivers with max total kilometers in this day(sort top to down)

    }
}
