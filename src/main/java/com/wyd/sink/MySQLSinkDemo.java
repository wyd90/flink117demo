package com.wyd.sink;

import com.wyd.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class MySQLSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 必须做checkpoint
//        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<WaterSensor> source = new DataGeneratorSource<WaterSensor>(new GeneratorFunction<Long, WaterSensor>() {

            private Random random = new Random();

            @Override
            public WaterSensor map(Long value) throws Exception {
                WaterSensor waterSensor = new WaterSensor("" + value, System.currentTimeMillis(), random.nextInt(100));
                return waterSensor;
            }
        }, Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(new TypeHint<WaterSensor>() {}));

        DataStreamSource<WaterSensor> wssource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "wssource");

        SinkFunction<WaterSensor> jdbcSink = JdbcSink.<WaterSensor>sink(
                "insert into water_sensor(id,ts,vc) values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        wssource.addSink(jdbcSink);

        env.execute();
    }
}
