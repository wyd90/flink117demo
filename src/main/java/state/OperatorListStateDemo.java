package state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorListStateDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        lines.map(new MyCountMapFunction()).print();
    }

    public static class MyCountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {

        private ListState<Long> state;
        private Long count = 0L;

        @Override
        public Long map(String value) throws Exception {
            count ++;
            return count;
        }

        // 做checkpoint会调用，将本地变量持久化到checkpoint中
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(count);
        }

        // 程序恢复时调用，将数据从checkpoint加载到本地变量
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore()
//                    .getUnionListState(new ListStateDescriptor<Long>("state", Long.class))
                    .getListState(new ListStateDescriptor<Long>("state", Long.class));

            if (context.isRestored()) {
                for (Long c : state.get()) {
                    count += c;
                }
            }
        }
    }
}
