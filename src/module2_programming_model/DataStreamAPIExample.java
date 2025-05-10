package module2_programming_model;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Flink DataStream API 示例
 * 
 * 该示例展示了如何创建一个简单的Flink流处理程序，
 * 从一个数据源读取数据，进行转换，然后打印到控制台。
 */
public class DataStreamAPIExample {

    public static void main(String[] args) throws Exception {
        // 1. 获取Flink流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建一个数据源 (例如：从一个集合中读取数据)
        DataStream<String> textStream = env.fromElements(
                "Hello Flink",
                "This is a DataStream API example",
                "Flink is a powerful stream processor"
        );

        // 3. 对数据流进行转换操作 (例如：将每个字符串转换为大写)
        DataStream<String> upperCaseStream = textStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        // 4. 定义一个数据汇 (例如：打印到控制台)
        upperCaseStream.print();

        // 5. 执行Flink作业
        env.execute("DataStream API Example");
    }
}