package module5_practical_cases;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class RealTimeUserBehaviorAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000); // 开启Checkpoint

        // 1. 定义Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092") // 假设Kafka在本地运行
            .setTopics("user-behavior-topic")
            .setGroupId("flink-behavior-consumer")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            // .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed") // 如果需要事务性读取
            .build();

        DataStream<String> behaviorJsonStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 2. 解析JSON数据并转换为POJO
        DataStream<UserBehavior> behaviorStream = behaviorJsonStream
            .map(jsonString -> {
                // 实际项目中会使用JSON库如Jackson或Gson
                // 这里仅为示例，假设UserBehavior类和解析逻辑已定义
                // return objectMapper.readValue(jsonString, UserBehavior.class);
                // 简化示例：
                String[] parts = jsonString.split(",");
                if (parts.length == 4) {
                    try {
                         return new UserBehavior(parts[0], parts[1], parts[2], Long.parseLong(parts[3]));
                    } catch (NumberFormatException e) {
                        // Log and skip malformed record
                        System.err.println("Malformed record: " + jsonString);
                        return null;
                    }
                }
                return null; // 错误处理
            })
            .filter(behavior -> behavior != null);

        // 3. 实时统计每种行为类型的数量 (例如，每分钟)
        DataStream<Tuple2<String, Integer>> behaviorCounts = behaviorStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
            )
            .keyBy(UserBehavior::getBehaviorType)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new BehaviorCountAggregator());

        // 4. 输出结果 (例如打印到控制台或写入外部存储)
        behaviorCounts.print();

        env.execute("Real-time User Behavior Analysis Job");
    }

    // 假设的用户行为数据类
    public static class UserBehavior {
        private String userId;
        private String itemId;
        private String behaviorType; // e.g., "pv", "click", "cart", "buy"
        private long timestamp;

        // Jackson/Gson等库通常需要无参构造函数
        public UserBehavior() {}

        public UserBehavior(String userId, String itemId, String behaviorType, long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.behaviorType = behaviorType;
            this.timestamp = timestamp;
        }

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }

        public String getItemId() { return itemId; }
        public void setItemId(String itemId) { this.itemId = itemId; }

        public String getBehaviorType() { return behaviorType; }
        public void setBehaviorType(String behaviorType) { this.behaviorType = behaviorType; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        @Override
        public String toString() {
            return "UserBehavior{" +
                   "userId='" + userId + '\'' +
                   ", itemId='" + itemId + '\'' +
                   ", behaviorType='" + behaviorType + '\'' +
                   ", timestamp=" + timestamp +
                   '}';
        }
    }

    // 简单的聚合函数，统计行为数量
    public static class BehaviorCountAggregator implements AggregateFunction<UserBehavior, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(UserBehavior value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }
}