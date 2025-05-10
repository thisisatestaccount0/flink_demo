package module5_practical_cases;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.eventtime.WatermarkStrategy; // Added for assignTimestampsAndWatermarks

import java.util.List;
import java.util.Map;

public class CreditCardFraudDetection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // For easier debugging in local environment

        // 1. 定义信用卡交易事件流 (TransactionEvent)
        DataStream<TransactionEvent> transactionStream = env.fromElements(
            // 模拟数据，实际应来自Kafka等
            new TransactionEvent("user1", "card1", 10.0, "New York", System.currentTimeMillis()),
            new TransactionEvent("user1", "card1", 15.0, "London", System.currentTimeMillis() + 1000 * 60), // 1分钟后
            new TransactionEvent("user2", "card2", 20.0, "Paris", System.currentTimeMillis()),
            new TransactionEvent("user1", "card1", 5.0, "Tokyo", System.currentTimeMillis() + 1000 * 60 * 2), // 2分钟后
            new TransactionEvent("user2", "card2", 100.0, "Paris", System.currentTimeMillis() + 1000 * 60 * 3)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<TransactionEvent>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        // 2. 定义CEP模式
        Pattern<TransactionEvent, ?> fraudPattern = Pattern
            .<TransactionEvent>begin("firstTransaction")
            .where(new SimpleCondition<TransactionEvent>() {
                @Override
                public boolean filter(TransactionEvent event) {
                    return event.getAmount() < 50.0; // 例如，小额交易
                }
            })
            .next("secondTransaction") // chained from firstTransaction
            .where(new SimpleCondition<TransactionEvent>() {
                @Override
                public boolean filter(TransactionEvent event) {
                    return event.getAmount() < 50.0;
                }
            })
            .within(Time.minutes(5)); // 在5分钟内发生
            // The location check is better done in the select function

        // 3. 将模式应用于流
        PatternStream<TransactionEvent> patternStream = CEP.pattern(
            transactionStream.keyBy(TransactionEvent::getCardNumber), // 按卡号分区
            fraudPattern
        );

        // 4. 选择并处理匹配到的模式
        DataStream<String> alerts = patternStream.select(
            new PatternSelectFunction<TransactionEvent, String>() {
                @Override
                public String select(Map<String, List<TransactionEvent>> pattern) throws Exception {
                    TransactionEvent first = pattern.get("firstTransaction").get(0);
                    TransactionEvent second = pattern.get("secondTransaction").get(0);

                    // 检查地理位置是否不同
                    if (!first.getLocation().equals(second.getLocation())) {
                        return "Potential Fraud Detected for Card: " + first.getCardNumber() +
                               " - Transaction 1: " + first.getAmount() + " at " + first.getLocation() + " (Time: " + first.getTimestamp() + ")" +
                               " - Transaction 2: " + second.getAmount() + " at " + second.getLocation() + " (Time: " + second.getTimestamp() + ")";
                    }
                    return null; // 不符合复杂条件 (i.e., locations are the same)
                }
            }
        ).filter(alert -> alert != null);

        alerts.print();

        env.execute("Credit Card Fraud Detection Job");
    }

    // 信用卡交易事件类
    public static class TransactionEvent {
        private String userId;
        private String cardNumber;
        private double amount;
        private String location;
        private long timestamp;

        // No-arg constructor for POJO
        public TransactionEvent() {}

        public TransactionEvent(String userId, String cardNumber, double amount, String location, long timestamp) {
            this.userId = userId;
            this.cardNumber = cardNumber;
            this.amount = amount;
            this.location = location;
            this.timestamp = timestamp;
        }

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }

        public String getCardNumber() { return cardNumber; }
        public void setCardNumber(String cardNumber) { this.cardNumber = cardNumber; }

        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }

        public String getLocation() { return location; }
        public void setLocation(String location) { this.location = location; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        @Override
        public String toString() {
            return "TransactionEvent{" +
                   "userId='" + userId + '\'' +
                   ", cardNumber='" + cardNumber + '\'' +
                   ", amount=" + amount +
                   ", location='" + location + '\'' +
                   ", timestamp=" + timestamp +
                   '}';
        }
    }
}