package com.wyd.sink;

import com.wyd.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Random;

public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 必须做checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

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

        DataStreamSource mysource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysource");

        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("D:\\flink1.17\\tmp"), new SimpleStringEncoder<>())
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("wyd")
                                .withPartSuffix(".log")
                                .build()
                )
                // 按目录分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.of("Asia/Shanghai")))
                // 文件滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(3600))
                                .withMaxPartSize(new MemorySize(1024 * 1024 * 1024))
                                .build()
                ).build();

        mysource.global().sinkTo(fileSink);

        env.execute();
    }
}
