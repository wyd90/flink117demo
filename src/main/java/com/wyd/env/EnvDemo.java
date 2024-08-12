package com.wyd.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class EnvDemo {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(conf);

        // 设置为批处理 （默认流处理）
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        env.readTextFile("input/word.txt")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] arr = line.split(" ");
                        for (String word : arr) {
                            collector.collect(new Tuple2<>(word, 1L));
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                }).sum(1).print();

        env.execute();

        env.executeAsync();
    }
}
