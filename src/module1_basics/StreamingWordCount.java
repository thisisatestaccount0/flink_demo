package module1_basics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * StreamingWordCount示例 - Flink流处理入门程序
 * 
 * 本示例演示了如何使用Flink的DataStream API实现流式单词计数功能
 * 展示了Flink作为流处理引擎的基本用法
 */
public class StreamingWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建数据流 - 这里使用socketTextStream方法从socket读取数据
        // 在实际运行时，需要使用netcat等工具提供数据：nc -lk 9999
        DataStream<String> text = env.socketTextStream("localhost", 9999);

        // 3. 转换操作 - 对数据流进行处理
        DataStream<Tuple2<String, Integer>> wordCounts = text
            // 将每行文本分割成单词，并输出(单词, 1)的二元组
            .flatMap(new Tokenizer())
            // 按照单词分组
            .keyBy(0)
            // 设置5秒的时间窗口
            .timeWindow(Time.seconds(5))
            // 对每个单词在窗口内的计数值求和
            .sum(1);

        // 4. 输出结果
        wordCounts.print();

        // 5. 执行程序
        System.out.println("开始执行Flink流处理作业");
        env.execute("Streaming Word Count");
    }

    /**
     * 分词器 - 实现FlatMapFunction接口
     * 将文本行拆分成单词，并为每个单词创建(单词,1)的二元组
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 将文本按空格分割成单词
            String[] words = value.toLowerCase().split("\\s+");
            
            // 输出每个单词和数字1组成的二元组
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}