package com.wyd.watermark;

import com.wyd.bean.WaterSensor;
import com.wyd.window.MyEventTrigger;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CustomPeriodicWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(400L);
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<WaterSensor> watersenorDS = lines.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] arr = value.split(",");
                return new WaterSensor(arr[0], Long.valueOf(arr[1]), Integer.valueOf(arr[2]));
            }
        });

        SingleOutputStreamOperator<WaterSensor> waterSensorWithWMDS = watersenorDS.assignTimestampsAndWatermarks(new MyWatermarkStrategy());

        waterSensorWithWMDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        int sum = 0;
                        int cnts = 0;
                        for (WaterSensor element : elements) {
                            cnts ++;
                            sum += element.getVc();
                        }
                        String returnStr = "窗口开始时间：" + start + ", 窗口结束时间： " + end + ", 一共有" + cnts + "条数据，总水位为：" + sum;
                        out.collect(returnStr);
                    }
                }).print();

        env.execute();
    }
}
