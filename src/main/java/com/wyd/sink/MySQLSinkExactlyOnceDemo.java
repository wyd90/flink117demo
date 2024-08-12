package com.wyd.sink;

import com.mysql.cj.jdbc.MysqlXADataSource;
import com.wyd.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class MySQLSinkExactlyOnceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 必须做checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

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

        SinkFunction<WaterSensor> jdbcSink = JdbcSink.<WaterSensor>exactlyOnceSink(
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
                        .withMaxRetries(0)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                .withTransactionPerConnection(true)
                .build(),
                () -> {
                    MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSource();
                    xaDataSource.setUrl("jdbc:mysql://hadoop102:3306/test");
                    xaDataSource.setUser("root");
                    xaDataSource.setPassword("123456");
                    return xaDataSource;
                }
        );

        wssource.addSink(jdbcSink);

        env.execute();
    }
}
