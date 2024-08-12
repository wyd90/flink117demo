package com.wyd.partition;

import com.wyd.bean.WaterSensor;
import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<WaterSensor> {
    @Override
    public int partition(WaterSensor key, int numPartitions) {
        return (key.getId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
