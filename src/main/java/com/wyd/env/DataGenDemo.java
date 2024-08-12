package com.wyd.env;

import com.wyd.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

public class DataGenDemo {
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

        DataStreamSource demoSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "demoSource");

        demoSource.print();


        env.execute();
    }
}
