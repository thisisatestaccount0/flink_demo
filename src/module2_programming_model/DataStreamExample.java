package module2_programming_model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink DataStream API 核心示例
 * 演示基本转换操作：map、filter、keyBy等
 */
public class DataStreamExample {
    
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 2. 从集合创建数据流
        DataStream<Integer> numbers = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        
        // 3. 转换操作 - map: 数值加倍
        DataStream<Integer> doubled = numbers
            .map(new MapFunction<Integer, Integer>() {
                @Override
                public Integer map(Integer value) throws Exception {
                    return value * 2;
                }
            });
            
        // 4. 转换操作 - filter: 筛选偶数
        DataStream<Integer> evens = doubled
            .filter(new FilterFunction<Integer>() {
                @Override
                public boolean filter(Integer value) throws Exception {
                    return value % 2 == 0;
                }
            });
            
        // 5. 输出结果
        evens.print();
        
        // 6. 执行作业
        env.execute("Flink DataStream API Example");
    }
}