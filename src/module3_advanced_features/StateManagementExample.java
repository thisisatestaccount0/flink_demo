package module3_advanced_features;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 状态管理示例
 *
 * 该示例展示了如何使用Flink的Keyed State (ValueState) 来管理有状态的转换。
 * 它会模拟一个事件流，其中包含用户ID和事件类型，
 * 然后统计每个用户产生的事件数量。
 */
public class StateManagementExample {

    public static void main(String[] args) throws Exception {
        // 1. 获取Flink流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建一个数据源 (模拟用户事件流: (userId, eventType))
        DataStream<Tuple2<String, String>> eventStream = env.fromElements(
                Tuple2.of("user1", "click"),
                Tuple2.of("user2", "view"),
                Tuple2.of("user1", "purchase"),
                Tuple2.of("user3", "click"),
                Tuple2.of("user2", "click"),
                Tuple2.of("user1", "view")
        );

        // 3. 对数据流进行有状态的转换操作
        //    使用RichFlatMapFunction和ValueState来统计每个用户的事件数
        DataStream<Tuple2<String, Long>> userEventCountStream = eventStream
                .keyBy(value -> value.f0) // 按用户ID (f0) 分组
                .flatMap(new UserEventCounter());

        // 4. 定义一个数据汇 (例如：打印到控制台)
        userEventCountStream.print();

        // 5. 执行Flink作业
        env.execute("State Management Example");
    }

    /**
     * 自定义的RichFlatMapFunction，用于统计每个用户的事件数。
     */
    public static class UserEventCounter extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Long>> {
        // ValueState用于存储每个key（用户ID）的事件计数
        private transient ValueState<Long> eventCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 在open方法中初始化状态
            ValueStateDescriptor<Long> descriptor = 
                    new ValueStateDescriptor<>(
                            "eventCount", // 状态名称
                            Types.LONG,   // 状态类型
                            0L);          // 默认值
            eventCountState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Long>> out) throws Exception {
            // 获取当前key的状态值（事件计数）
            Long currentCount = eventCountState.value();
            
            // 更新计数
            currentCount++;
            eventCountState.update(currentCount);

            // 输出 (用户ID, 当前事件总数)
            out.collect(new Tuple2<>(value.f0, currentCount));
        }
    }
}