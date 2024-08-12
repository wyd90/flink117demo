package com.wyd.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> lines = env.readTextFile("input\\word.txt");

        FlatMapOperator<String, Tuple2<String, Long>> flatMaped = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] arr = line.split(" ");
                for (String word : arr) {
                    collector.collect(new Tuple2<String, Long>(word, 1L));
                }
            }
        });

        flatMaped.groupBy(0).sum(1).print();


//        List<Tuple2<String, Long>> collect = flatMaped.groupBy(new KeySelector<Tuple2<String, Long>, String>() {
//            public String getKey(Tuple2<String, Long> value) throws Exception {
//                return value.f0;
//            }
//        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
//            public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
//                return new Tuple2<String, Long>(t1.f0, t1.f1 + t2.f1);
//            }
//        }).collect();
//
//        for (Tuple2<String ,Long> v : collect) {
//            System.out.println(v);
//        }

    }
}
