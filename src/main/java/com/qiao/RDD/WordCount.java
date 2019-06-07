package com.qiao.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("wordCount").setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<String> textFile = jsc.textFile("data/wordCount/input");
//        JavaRDD<String> filter = textFile.filter(s -> s.contains("h"));

        JavaRDD<String> flagMap = textFile.flatMap(s -> {
            ArrayList<String> strings = new ArrayList<>();
            String[] s1 = s.split(" ");
            for (int i = 0; i < s1.length; i++) {
                strings.add(s1[i]);
            }
            return strings.iterator();
        });
//        JavaRDD<String> filter = flagMap.filter(
//                s -> s.indexOf('H') != -1
//        );

//        JavaRDD<Tuple2<String, Integer>> map = flagMap.map(s -> new Tuple2<>(s, 1));


        JavaPairRDD<String, Integer> pairRDD = flagMap.mapToPair(s -> new Tuple2<>(s, 1));
        // JavapairRdd reduce = flagMap.map()

//        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupBy = map.groupBy(s -> s._1);
//
//        groupBy.reduceByKey((x , y) -> x + y);

        JavaPairRDD reduce = pairRDD.reduceByKey((x , y) -> x + y);

        List<Tuple2<String, Iterable<Tuple2<String, Integer>>>> collect = reduce.collect();

        collect.forEach(pair -> System.out.println(pair._1 + " : " + pair._2));

        System.out.println(collect);
        jsc.close();

    }

}
