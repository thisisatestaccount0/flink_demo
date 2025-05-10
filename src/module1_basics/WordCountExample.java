package module1_basics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink流处理基础示例 - WordCount
 * 演示如何从集合创建DataStream并进行基本转换操作
 */
public class WordCountExample {
    
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 2. 从集合创建数据流
        DataStream<String> text = env.fromElements(
            "Apache Flink是一个框架和分布式处理引擎",
            "用于在无边界和有边界数据流上进行有状态的计算"
        );
        
        // 3. 转换操作 - 分词、分组、求和
        DataStream<Tuple2<String, Integer>> wordCounts = text
            .flatMap(new Tokenizer())  // 分词
            .keyBy(0)                 // 按单词分组
            .sum(1);                  // 求和
            
        // 4. 输出结果(本地执行时打印到控制台)
        wordCounts.print();
        
        // 5. 执行作业
        env.execute("Flink Streaming WordCount");
    }
    
    /**
     * 分词器 - 将句子拆分为单词并计数
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