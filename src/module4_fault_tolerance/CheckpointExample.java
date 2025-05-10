package module4_fault_tolerance;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * Flink容错机制示例
 * 演示如何配置检查点和重启策略
 */
public class CheckpointExample {
    
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 2. 配置检查点(每10秒一次)
        env.enableCheckpointing(10000); // 检查点间隔10秒
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // 3. 配置状态后端(本地文件系统)
        env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));
        
        // 4. 配置重启策略(失败后最多重试3次，每次间隔10秒)
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(
                3, // 最大重试次数
                Time.of(10, TimeUnit.SECONDS) // 重试间隔
            )
        );
        
        // 5. 创建模拟数据源
        env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;
            private long count = 0;
            
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect("Event-" + (count++));
                    Thread.sleep(1000); // 每秒产生一个事件
                }
            }
            
            @Override
            public void cancel() {
                isRunning = false;
            }
        }).print();
        
        // 6. 执行作业
        env.execute("Flink Checkpoint Example");
    }
}