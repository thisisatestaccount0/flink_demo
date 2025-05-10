package module5_real_world_applications;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Flink 实战应用示例 - 实时数据仓库 (简化版)
 *
 * 该示例展示了如何使用Flink SQL构建一个简单的实时数据仓库场景。
 * 假设我们有一个订单事件流，我们希望实时统计每种商品的总销售额。
 */
public class RealTimeDataWarehouseExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 定义数据源 (模拟订单事件流)
        // 订单数据：(订单ID, 商品ID, 数量, 单价)
        tableEnv.executeSql("CREATE TABLE Orders (\n" +
                "  order_id STRING,\n" +
                "  product_id STRING,\n" +
                "  amount INT,\n" +
                "  price DOUBLE,\n" +
                "  proc_time AS PROCTIME() \n" + // 处理时间属性
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" + // 每秒生成1行数据
                "  'fields.order_id.length' = '10',\n" +
                "  'fields.product_id.kind' = 'random',\n" +
                "  'fields.product_id.min' = '1',\n" +
                "  'fields.product_id.max' = '3',\n" + // 假设有3种商品
                "  'fields.amount.kind' = 'random',\n" +
                "  'fields.amount.min' = '1',\n" +
                "  'fields.amount.max' = '5',\n" +
                "  'fields.price.kind' = 'random',\n" +
                "  'fields.price.min' = '10.0',\n" +
                "  'fields.price.max' = '100.0'\n" +
                ")");

        // 3. 定义实时聚合查询 (统计每种商品的总销售额)
        Table salesPerProduct = tableEnv.sqlQuery(
                "SELECT \n" +
                "  product_id, \n" +
                "  SUM(amount * price) AS total_sales \n" +
                "FROM Orders \n" +
                "GROUP BY product_id"
        );

        // 4. 定义数据汇 (将结果打印到控制台)
        // 对于动态表，需要将其转换为DataStream进行打印
        // toRetractStream 用于处理有更新（retraction）的聚合结果
        tableEnv.toRetractStream(salesPerProduct, Row.class).print();

        // 5. 执行作业
        // Flink SQL作业的执行通常由tableEnv.executeSql()触发，或者在将Table转换为DataStream并添加sink后由env.execute()触发。
        // 在这个例子中，因为我们使用了toRetractStream().print()，它本身会触发执行。
        // 如果是写入到外部系统，则需要env.execute()。
        // env.execute("Real-time Data Warehouse Example"); // 对于某些sink是需要的
    }
}