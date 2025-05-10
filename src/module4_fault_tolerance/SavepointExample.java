package module4_fault_tolerance;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Flink 容错机制示例 - Savepoints
 *
 * 该示例展示了如何构建一个Flink作业，使其能够利用保存点进行状态的备份和恢复。
 * 保存点是手动触发的检查点，用于作业的升级、迁移或归档。
 * 
 * 注意：
 * 1. 触发保存点和从保存点恢复通常是通过Flink的命令行工具或REST API执行的，而不是在作业代码中。
 * 2. 为了使作业能够从保存点恢复，算子需要分配唯一的ID (uid)。
 */
public class SavepointExample {

    public static void main(String[] args) throws Exception {
        // 1. 获取Flink流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 启用检查点 (保存点依赖于检查点机制)
        env.enableCheckpointing(5000); 

        // 2. 创建一个有状态的数据源 (模拟一个持续生成数据的源)
        DataStream<Long> dataStream = env.addSource(new SourceFunction<Long>() {
            private volatile boolean isRunning = true;
            private long counter = 0L;

            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                while (isRunning) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(counter++);
                    }
                    Thread.sleep(1000); // 每秒发送一个数据
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).uid("source-uid"); // 为源算子分配唯一ID，对保存点很重要

        // 3. 对数据流进行有状态的转换操作
        //    这里使用RichMapFunction来维护一个简单的计数状态
        DataStream<String> processedStream = dataStream
                .keyBy(value -> value % 2) // 按奇偶性分组
                .map(new StatefulMapper())
                .uid("stateful-mapper-uid"); // 为Map算子分配唯一ID

        // 4. 定义一个数据汇 (例如：打印到控制台)
        processedStream.print().uid("print-sink-uid");

        // 5. 执行Flink作业
        System.out.println("Flink job started. To test savepoints:");
        System.out.println("1. Let the job run for a while to accumulate state.");
        System.out.println("2. Trigger a savepoint using Flink CLI: flink savepoint <jobId> [savepointDirectory]");
        System.out.println("3. Stop the job.");
        System.out.println("4. Resubmit the job from the savepoint: flink run -s <savepointPath> <jobJar> <mainClass>");
        
        env.execute("Savepoint Example Job");
    }

    /**
     * 一个简单的有状态的MapFunction，为每个key维护一个计数器。
     */
    public static class StatefulMapper extends RichMapFunction<Long, String> {
        private transient ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>("perKeyCounter", Types.LONG, 0L);
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public String map(Long value) throws Exception {
            Long currentCount = countState.value();
            currentCount++;
            countState.update(currentCount);
            return "Key: " + (value % 2) + ", Value: " + value + ", CountForThisKey: " + currentCount;
        }
    }
}