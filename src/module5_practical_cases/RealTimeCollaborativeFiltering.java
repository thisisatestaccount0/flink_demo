package module5_practical_cases;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class RealTimeCollaborativeFiltering {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 模拟用户浏览行为数据源 (userId, itemId)
        DataStream<Tuple2<String, String>> userViewStream = env.fromElements(
            Tuple2.of("user1", "itemA"),
            Tuple2.of("user2", "itemB"),
            Tuple2.of("user1", "itemC"),
            Tuple2.of("user3", "itemA"),
            Tuple2.of("user2", "itemC"),
            Tuple2.of("user1", "itemB"),
            Tuple2.of("user3", "itemD")
        );

        // 1. 计算物品的共现次数 (Item Co-occurrence)
        // keyBy userId, then process pairs of items viewed by the same user in a session/window
        // 这里简化为处理所有历史数据，实际中会使用窗口

        // (itemA, itemB) -> count
        DataStream<Tuple3<String, String, Integer>> itemCooccurrence = userViewStream
            .keyBy(0) // Key by userId
            .flatMap(new CooccurrenceFlatMapFunction());

        // 聚合共现次数
        DataStream<Tuple3<String, String, Integer>> aggregatedCooccurrence = itemCooccurrence
            .keyBy(value -> value.f0 + "#" + value.f1) // Key by itemPair (ensure order for aggregation)
            .sum(2);

        aggregatedCooccurrence.print();

        // 2. 实时推荐 (简化示例)
        // 当用户浏览一个物品时，查找共现次数高的其他物品
        // 实际推荐系统会更复杂，需要结合用户历史、物品特征等
        // 此处仅打印共现矩阵，实际应用中会将其存储并用于查询

        env.execute("Real-time Collaborative Filtering Job");
    }

    // FlatMapFunction to generate item pairs from a user's view history
    public static class CooccurrenceFlatMapFunction extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {
        private transient ListState<String> itemsViewedState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("itemsViewed", String.class);
            itemsViewedState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, String> value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String currentItem = value.f1;
            Iterable<String> previousItems = itemsViewedState.get();
            if (previousItems != null) {
                for (String prevItem : previousItems) {
                    if (!prevItem.equals(currentItem)) {
                        // Ensure canonical order for pairs (item1, item2) where item1 < item2
                        // This helps in consistent aggregation later
                        if (prevItem.compareTo(currentItem) < 0) {
                            out.collect(Tuple3.of(prevItem, currentItem, 1));
                        } else {
                            out.collect(Tuple3.of(currentItem, prevItem, 1));
                        }
                    }
                }
            }
            itemsViewedState.add(currentItem);
        }
    }
}