package com.wyd.watermark;

import com.wyd.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;

public class MyWatermarkStrategy implements WatermarkStrategy<WaterSensor> {

    @Override
    public TimestampAssigner<WaterSensor> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        };
    }

    @Override
    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyBoundOutOfOrdernessGenerator(5000L);
    }

    public static class MyBoundOutOfOrdernessGenerator implements WatermarkGenerator<WaterSensor> {
        //延迟时间
        private Long delayTime;
        private Long maxTs;

        public MyBoundOutOfOrdernessGenerator(Long delayTime) {
            this.delayTime = delayTime;
            this.maxTs = Long.MIN_VALUE + delayTime + 1L;
        }

        // 每来一条数据就调用一次
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            // 更新最大时间戳
            maxTs = Math.max(eventTimestamp, maxTs);
//            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //发射水位线，默认200ms调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
//            System.out.println("调用onPeriodicEmit方法，生成watermark="+ (maxTs - delayTime - 1L));
        }
    }

}
