package module5_practical_cases;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Flink实时分析案例
 * 演示如何实现滑动窗口统计和实时告警
 */
public class RealTimeAnalysis {
    
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 2. 模拟实时数据源(用户行为日志)
        DataStream<String> logs = env.socketTextStream("localhost", 9999);
        
        // 3. 实时统计 - 每5秒统计一次过去10秒内的PV
        DataStream<Tuple2<String, Integer>> pvStats = logs
            .flatMap(new LogParser()) // 解析日志
            .keyBy(0) // 按页面分组
            .timeWindow(Time.seconds(10), Time.seconds(5)) // 滑动窗口:10秒大小,5秒滑动
            .sum(1); // 求和
            
        // 4. 实时告警 - 检测异常访问
        DataStream<String> alerts = logs
            .flatMap(new LogParser())
            .keyBy(0)
            .timeWindow(Time.seconds(5)) // 5秒滚动窗口
            .sum(1)
            .filter(value -> value.f1 > 100) // 阈值:5秒内访问超过100次
            .map(value -> "[告警] 页面 " + value.f0 + " 访问量异常: " + value.f1);
            
        // 5. 输出结果
        pvStats.print("PV统计");
        alerts.print("异常告警");
        
        // 6. 执行作业
        env.execute("Flink实时分析案例");
    }
    
    /**
     * 日志解析器 - 提取页面URL和访问次数
     */
    public static class LogParser implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 简单解析日志格式: "时间戳|用户ID|页面URL"
            String[] parts = value.split("\\|");
            if (parts.length >= 3) {
                out.collect(new Tuple2<>(parts[2], 1));
            }
        }
    }
}