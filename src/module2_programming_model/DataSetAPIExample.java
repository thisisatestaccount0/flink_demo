package module2_programming_model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Flink DataSet API 示例
 * 
 * 该示例展示了如何创建一个简单的Flink批处理程序，
 * 从一个数据源读取数据，进行转换，然后打印到控制台。
 * DataSet API 主要用于批处理场景。
 */
public class DataSetAPIExample {

    public static void main(String[] args) throws Exception {
        // 1. 获取Flink批处理执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建一个数据源 (例如：从一个集合中读取数据)
        DataSet<String> textDataSet = env.fromElements(
                "Apache Flink is a stream processing framework",
                "It also supports batch processing with the DataSet API",
                "This is a DataSet API example"
        );

        // 3. 对数据集进行转换操作
        // 3.1 Map: 将每个字符串转换为 (字符串, 长度) 的元组
        DataSet<Tuple2<String, Integer>> tupleDataSet = textDataSet.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, value.length());
            }
        });

        // 3.2 Filter: 过滤掉长度小于30的元组
        DataSet<Tuple2<String, Integer>> filteredDataSet = tupleDataSet.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return value.f1 >= 30;
            }
        });

        // 4. 定义一个数据汇 (例如：打印到控制台)
        System.out.println("Original DataSet:");
        textDataSet.print();

        System.out.println("\nTuple DataSet (String, Length):");
        tupleDataSet.print();

        System.out.println("\nFiltered DataSet (Length >= 30):");
        filteredDataSet.print();

        // DataSet API的执行是惰性的，print()会触发执行。
        // 或者可以使用 env.execute("DataSet API Example"); 来显式执行，但对于print()不是必需的。
    }
}