package com.wyd.window;

import com.wyd.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("hadoop102", 8888);

        SingleOutputStreamOperator<WaterSensor> watersenorDS = lines.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] arr = value.split(",");
                return new WaterSensor(arr[0], Long.valueOf(arr[1]), Integer.valueOf(arr[2]));
            }
        });

        SingleOutputStreamOperator<Integer> aggregate = watersenorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<WaterSensor, Tuple2<Integer, Integer>, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return new Tuple2<>(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(WaterSensor value, Tuple2<Integer, Integer> accumulator) {
                        accumulator.f0 += 1;
                        accumulator.f1 += value.getVc();
                        return accumulator;
                    }

                    @Override
                    public Integer getResult(Tuple2<Integer, Integer> accumulator) {
                        return accumulator.f1 / accumulator.f0;
                    }

                    // 这个方法只有在会话窗口时才会调用
                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return null;
                    }
                });

        aggregate.print();

        env.execute();
    }
}
