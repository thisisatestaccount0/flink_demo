package module4_fault_tolerance;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Flink 容错机制示例 - Checkpointing
 *
 * 该示例展示了如何启用Flink的检查点机制，以确保在发生故障时能够恢复状态并保证exactly-once语义。
 * 它模拟一个有状态的流处理作业，并配置了检查点。
 */
public class CheckpointingExample {

    public static void main(String[] args) throws Exception {
        // 1. 获取Flink流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 启用检查点机制
        // 每隔5000毫秒（5秒）启动一个检查点
        env.enableCheckpointing(5000);

        // 3. 配置重启策略
        // 固定延迟重启策略：尝试重启3次，每次重启之间延迟10秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.seconds(10) // 每次重启之间的延迟
        ));

        // 4. 创建一个有状态的数据源 (模拟一个计数器)
        DataStream<Long> countStream = env.addSource(new SourceFunction<Long>() {
            private volatile boolean isRunning = true;
            private long count = 0L;

            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                while (isRunning) {
                    // 使用synchronized块确保状态的一致性，特别是在检查点期间
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(count++);
                    }
                    Thread.sleep(1000); // 每秒发送一个计数
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        // 5. 对数据流进行有状态的转换操作 (例如：简单地将数字乘以2)
        // 这个操作本身是无状态的，但上游的Source是有状态的
        DataStream<Long> processedStream = countStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                // 模拟一个可能失败的操作
                if (value % 10 == 0 && value > 0) { // 每当count是10的倍数时，模拟一个异常
                    System.out.println("Simulating failure at count: " + value);
                    throw new RuntimeException("Simulated failure for testing checkpointing and recovery.");
                }
                return value * 2;
            }
        });

        // 6. 定义一个数据汇 (例如：打印到控制台)
        processedStream.print();

        // 7. 执行Flink作业
        env.execute("Checkpointing Example");
    }
}