package module1_basics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * WordCount示例 - Flink批处理入门程序
 * 
 * 本示例演示了如何使用Flink的DataSet API实现单词计数功能
 * 这是学习Flink的第一个程序，类似于大数据领域的"Hello World"
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 1. 设置执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建数据集 - 这里使用fromElements方法直接创建DataSet
        DataSet<String> text = env.fromElements(
            "Apache Flink是一个框架和分布式处理引擎，用于在无边界和有边界数据流上进行有状态的计算",
            "Flink的设计目标是在任何规模上运行有状态的流处理应用程序",
            "Flink应用程序可以使用共享的状态，精确一次的状态一致性和事件时间处理语义"
        );

        // 3. 转换操作 - 对数据进行处理
        DataSet<Tuple2<String, Integer>> wordCounts = text
            // 将每行文本分割成单词，并输出(单词, 1)的二元组
            .flatMap(new Tokenizer())
            // 按照单词分组
            .groupBy(0)
            // 对每个单词的计数值求和
            .sum(1);

        // 4. 输出结果
        System.out.println("Word Count结果：");
        wordCounts.print();
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