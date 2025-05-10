package module3_advanced_features;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Flink 窗口操作示例
 *
 * 该示例展示了如何使用Flink的窗口API对数据流进行窗口聚合。
 * 它会模拟一个简单的数据源，每秒生成一个带时间戳的事件，
 * 然后使用滚动窗口（Tumbling Window）对每5秒内的数据进行计数。
 */
public class WindowOperationsExample {

    public static void main(String[] args) throws Exception {
        // 1. 获取Flink流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建一个数据源 (模拟一个事件流，每秒产生一个事件)
        DataStream<Tuple2<String, Integer>> eventStream = env.addSource(
                new org.apache.flink.streaming.api.functions.source.SourceFunction<Tuple2<String, Integer>>() {
                    private volatile boolean isRunning = true;
                    private long counter = 0;

                    @Override
                    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                        while (isRunning) {
                            ctx.collect(new Tuple2<>("key", 1));
                            counter++;
                            Thread.sleep(1000); // 每秒发送一个事件
                        }
                    }

                    @Override
                    public void cancel() {
                        isRunning = false;
                    }
                });

        // 3. 对数据流进行窗口操作
        //    这里使用滚动窗口，窗口大小为5秒
        //    对窗口内的数据进行sum聚合
        DataStream<Tuple2<String, Integer>> windowedStream = eventStream
                .keyBy(value -> value.f0) // 按key分组，这里所有事件的key都是"key"
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 定义5秒的滚动窗口
                .sum(1); // 对元组的第二个字段（f1）进行求和

        // 4. 定义一个数据汇 (例如：打印到控制台)
        windowedStream.print();

        // 5. 执行Flink作业
        env.execute("Window Operations Example");
    }
}