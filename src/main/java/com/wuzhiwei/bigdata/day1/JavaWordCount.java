package com.wuzhiwei.bigdata.day1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaWordCount {

    public static void main(String[] args) {

        // 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        // 创建SparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        // 指定以后从哪里开始读入数据
        JavaRDD<String> lines = jsc.textFile(args[0]);
        // wc RDD
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });


        // 由于：java 接口只实现了sortByKey，所以需要先把 reduce 的结果，kv对互换

        JavaPairRDD<Integer, String> swapped = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> reduceRes) throws Exception {
//                return new Tuple2<>(reduceRes._2, reduceRes._1);
                return reduceRes.swap();
            }
        });

        JavaPairRDD<Integer, String> sorted = swapped.sortByKey();

        // 再将sorted 的执行 Tuple 结果 kv，执行swap
        JavaPairRDD<String, Integer> swapFromSorted = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> sorted) throws Exception {
                return sorted.swap();
            }
        });

        swapFromSorted.saveAsTextFile(args[1]);

        jsc.stop();



    }
}
