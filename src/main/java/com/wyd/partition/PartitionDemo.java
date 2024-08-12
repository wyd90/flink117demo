package com.wyd.partition;

import com.wyd.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataGeneratorSource source = new DataGeneratorSource<WaterSensor>(new GeneratorFunction<Long, WaterSensor>() {

            private Random random = new Random();

            @Override
            public WaterSensor map(Long value) throws Exception {
                WaterSensor waterSensor = new WaterSensor("" + value, System.currentTimeMillis(), random.nextInt(100));
                return waterSensor;
            }
        }, Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(new TypeHint<WaterSensor>() {}));

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "demoSource")

                .partitionCustom(new MyPartitioner(), new KeySelector<WaterSensor, WaterSensor>() {
                    @Override
                    public WaterSensor getKey(WaterSensor value) throws Exception {
                        return value;
                    }
                }).print();

        env.execute();

    }
}
