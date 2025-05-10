package module3_advanced_features;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink 高级特性示例 - 事件时间 (Event Time) 处理
 *
 * 该示例展示了如何使用Flink的事件时间语义处理数据流。
 * 它会模拟一个带有事件时间戳的数据源，并使用Watermark来处理乱序事件，
 * 然后基于事件时间进行窗口聚合。
 */
public class EventTimeExample {

    public static void main(String[] args) throws Exception {
        // 1. 获取Flink流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 为了演示事件时间，通常需要设置时间特性
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // Deprecated in Flink 1.12+
        // 从Flink 1.12开始，默认的时间特性是事件时间，但WatermarkStrategy的分配是必需的

        // 2. 创建一个数据源 (模拟带有事件时间戳的事件流: (event, timestamp))
        DataStream<Tuple2<String, Long>> eventStream = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            private volatile boolean isRunning = true;
            private final String[] events = {"eventA", "eventB", "eventC"};
            private long baseTime = System.currentTimeMillis();

            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                int count = 0;
                while (isRunning && count < 15) { // Generate a few events
                    String event = events[count % events.length];
                    // Simulate some out-of-orderness and delays
                    long eventTime = baseTime + (count * 1000) - (count % 3 * 2000); // Introduce some disorder
                    ctx.collect(new Tuple2<>(event, eventTime));
                    System.out.println("Sent: (" + event + ", " + eventTime + ")");
                    count++;
                    Thread.sleep(500); // Emit events with some delay
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        // 3. 分配时间戳和Watermark
        //    这里使用BoundedOutOfOrdernessWatermarks，允许最大2秒的乱序
        DataStream<Tuple2<String, Long>> withTimestampsAndWatermarks = eventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((element, recordTimestamp) -> element.f1)
                );

        // 4. 对数据流进行基于事件时间的窗口操作
        //    这里使用5秒的滚动事件时间窗口
        //    统计每个窗口内不同事件的出现次数
        DataStream<Tuple2<String, Integer>> windowedStream = withTimestampsAndWatermarks
                .map(new MapFunction<Tuple2<String,Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Long> value) throws Exception {
                        return new Tuple2<>(value.f0, 1);
                    }
                })
                .keyBy(value -> value.f0) // 按事件类型 (f0) 分组
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) // 定义5秒的滚动事件时间窗口
                .sum(1); // 对元组的第二个字段（计数）进行求和

        // 5. 定义一个数据汇 (例如：打印到控制台)
        windowedStream.print();

        // 6. 执行Flink作业
        env.execute("Event Time Example");
    }
}