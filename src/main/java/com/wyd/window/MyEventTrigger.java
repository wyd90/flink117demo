package com.wyd.window;

import com.wyd.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class MyEventTrigger extends Trigger<WaterSensor, TimeWindow> {

    @Override
    public TriggerResult onElement(WaterSensor element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            ReducingState<Integer> partitionedState = ctx.getPartitionedState(new ReducingStateDescriptor<Integer>("count", new ReduceFunction<Integer>() {
                @Override
                public Integer reduce(Integer value1, Integer value2) throws Exception {
                    return value1 + value2;
                }
            }, Integer.class));
            partitionedState.add(1);
            Integer cnts = partitionedState.get();

            if (cnts >= 5L) {
                partitionedState.clear();
                return TriggerResult.FIRE;
            } else {
                return TriggerResult.CONTINUE;
            }
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(new ReducingStateDescriptor<Integer>("count", new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        }, Integer.class)).clear();
    }

    /**
     * 该触发器是否支持合并，由于Flink内部是基于taskManager上的slot进行分布式计算。需要把每一个task的状态进行合并
     * @return
     */
        @Override
        public boolean canMerge() {
            return true;
        }

    /**
     * 多个taskManager进程中的slot所维护的线程状态实现合并
     * 合并多个task中的状态数据
     *
     * @param window
     * @param ctx
     */
        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(windowMaxTimestamp);
            }
            ctx.mergePartitionedState(new ReducingStateDescriptor<Integer>("count", new ReduceFunction<Integer>() {
                @Override
                public Integer reduce(Integer value1, Integer value2) throws Exception {
                    return value1 + value2;
                }
            }, Integer.class));
        }
}
