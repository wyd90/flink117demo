package com.wyd.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDCSQL {
    public static void main(String[] args) {

        //第一种创建方式
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                //.inBatchMode()
//                .build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //第二种创建方式

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("" +
                        "create table t1(\n" +
                        "    id string primary key NOT ENFORCED,\n" +
                        "    name string" +
                ") WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = 'hadoop102',\n" +
                        " 'port' = '3306',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = '123456',\n" +
                        " 'database-name' = 'test',\n" +
                        " 'table-name' = 't1'\n" +
                        ")");

//        Table table = tableEnv.sqlQuery("select * from t1");

        Table table = tableEnv.sqlQuery("select name,count(*) cnts from t1 group by name");
        table.execute().print();
    }
}
