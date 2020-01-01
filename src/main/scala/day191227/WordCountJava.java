package day191227;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * java版本的wordcount
 */
public class WordCountJava {
    public static void main(String[] args) {
        //获取spark环境
        SparkConf sparkConf = new SparkConf().setAppName("wordcountjava").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        //读取数据
        JavaRDD<String> javaRDD = javaSparkContext.textFile("E:\\BaiduNetdiskDownload\\data\\word.txt");
        //切分数据
        JavaRDD<String> words = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Arrays.asList(s1).iterator();
            }
        });

        JavaRDD<String> words1 = javaRDD.flatMap((FlatMapFunction<String, String>) s -> {
            String[] s1 = s.split(" ");
            return Arrays.asList(s1).iterator();
        });
        //拼接数据
        JavaPairRDD<String, Integer> javaPairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> javaPairRDD1 = words1.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        //合并数据
        JavaPairRDD<String, Integer> result = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaPairRDD<String, Integer> result1 = javaPairRDD1.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
        //打印数据
        System.out.println("----方式一-----");
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+","+stringIntegerTuple2._2);
            }
        });

        System.out.println("----方式二-----");
        result1.foreach(s -> {
            System.out.println(s);
        });
    }
}
