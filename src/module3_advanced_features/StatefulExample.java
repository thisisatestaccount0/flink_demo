package module3_advanced_features;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink状态管理示例
 * 演示如何使用键控状态(Keyed State)实现有状态的计算
 */
public class StatefulExample {
    
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 2. 从集合创建数据流(模拟传感器读数)
        DataStream<Tuple2<String, Double>> sensorReadings = env.fromElements(
            Tuple2.of("sensor1", 10.5),
            Tuple2.of("sensor1", 11.2),
            Tuple2.of("sensor2", 22.1),
            Tuple2.of("sensor1", 12.8),
            Tuple2.of("sensor2", 23.4)
        );
        
        // 3. 按键分组并使用状态计算每个传感器的平均值
        DataStream<Tuple2<String, Double>> avgReadings = sensorReadings
            .keyBy(0)  // 按传感器ID分组
            .flatMap(new AvgCalculator());
            
        // 4. 输出结果
        avgReadings.print();
        
        // 5. 执行作业
        env.execute("Flink Stateful Example");
    }
    
    /**
     * 平均值计算器 - 使用键控状态维护计数和总和
     */
    public static class AvgCalculator extends RichFlatMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
        
        // 声明状态变量
        private transient ValueState<Tuple2<Integer, Double>> sumState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor =
                new ValueStateDescriptor<>(
                    "average", // 状态名称
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {}), // 状态类型
                    Tuple2.of(0, 0.0) // 默认值
                );
            sumState = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void flatMap(Tuple2<String, Double> input, Collector<Tuple2<String, Double>> out) throws Exception {
            // 获取当前状态
            Tuple2<Integer, Double> currentSum = sumState.value();
            
            // 更新状态
            currentSum.f0 += 1; // 计数加1
            currentSum.f1 += input.f1; // 累加值
            sumState.update(currentSum);
            
            // 计算并输出当前平均值
            double avg = currentSum.f1 / currentSum.f0;
            out.collect(Tuple2.of(input.f0, avg));
        }
    }
}