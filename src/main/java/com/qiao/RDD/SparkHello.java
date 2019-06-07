package com.qiao.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Iterator;

public class SparkHello {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.setLogLevel("ERROR");

        JavaRDD<String> textFile = jsc.textFile("data/wordCount/input");

        JavaRDD<String> filter = textFile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v) throws Exception {
                return v.contains("Hello");
            }
        });

        JavaRDD<Object> flatMap = filter.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public Iterator<Object> call(String s) throws Exception {
                ArrayList<Object> stringList = new ArrayList<>();
                String[] split = s.split(" ");
                for (int i = 0; i < split.length; i++) {
                    stringList.add(split[i]);
                }
                return stringList.iterator();
            }
        });

//        JavaRDD<Tuple2<String, Integer>> map = flatMap.map(new Function<String, Tuple2<String, Integer>>() {
//            public Tuple2<String, Integer> call(String v1) throws Exception {
//                return new Tuple2<String, Integer>(v1, 1);
//            }
//        });
    }

}
