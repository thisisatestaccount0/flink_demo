# Flink课堂笔记 - 模块2：编程模型

## 1. Flink编程模型概述

### 1.1 Flink的分层API

Flink提供了不同层次的API，以满足不同的需求：

- **低级API**：ProcessFunction
  - 提供对时间和状态的细粒度控制
  - 允许实现自定义的复杂业务逻辑
  - 编程复杂度较高

- **核心API**：DataStream API / DataSet API
  - 提供通用的数据处理原语，如map、filter、reduce等
  - 支持自定义函数和复杂的数据处理逻辑
  - 是Flink最常用的API

- **高级API**：Table API / SQL
  - 提供声明式的API，简化数据处理逻辑
  - 更接近传统的数据处理方式
  - 适合数据分析和ETL场景

### 1.2 编程模型的核心概念

- **数据源（Source）**：数据的输入点，如文件、消息队列等
- **转换（Transformation）**：数据处理逻辑，如map、filter、window等
- **数据接收器（Sink）**：数据的输出点，如文件、数据库等
- **执行环境（Environment）**：程序的入口点，负责作业的执行

## 2. DataStream API深入

### 2.1 DataStream的创建

DataStream可以通过多种方式创建：

```java
// 从集合创建
DataStream<String> dataStream = env.fromCollection(Arrays.asList("a", "b", "c"));

// 从文件创建
DataStream<String> dataStream = env.readTextFile("file:///path/to/file");

// 从Socket创建
DataStream<String> dataStream = env.socketTextStream("localhost", 9999);

// 使用自定义Source
DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<>(...));
```

### 2.2 DataStream转换操作

DataStream API提供了丰富的转换操作：

#### 基本转换

```java
// Map转换 - 一对一转换
DataStream<Integer> mapped = dataStream.map(s -> s.length());

// FlatMap转换 - 一对多转换
DataStream<String> flatMapped = dataStream.flatMap(
    (String s, Collector<String> out) -> {
        for (String word : s.split(" ")) {
            out.collect(word);
        }
    }
);

// Filter转换 - 过滤数据
DataStream<String> filtered = dataStream.filter(s -> s.startsWith("a"));
```

#### 聚合操作

```java
// KeyBy - 按键分组
KeyedStream<Tuple2<String, Integer>, Tuple> keyed = 
    dataStream.keyBy(0); // 按第一个字段分组

// Reduce - 归约操作
DataStream<Tuple2<String, Integer>> reduced = 
    keyed.reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));

// Aggregations - 聚合操作
DataStream<Tuple2<String, Integer>> summed = keyed.sum(1); // 对第二个字段求和
DataStream<Tuple2<String, Integer>> maxed = keyed.max(1);  // 对第二个字段求最大值
DataStream<Tuple2<String, Integer>> minBy = keyed.minBy(1); // 对第二个字段求最小值（保留原始记录）
```

#### 多流转换

```java
// Union - 合并多个同类型的流
DataStream<String> union = stream1.union(stream2, stream3);

// Connect - 连接两个不同类型的流
ConnectedStreams<String, Integer> connected = 
    stream1.connect(stream2);

// CoMap - 对连接的流进行映射
DataStream<String> coMapped = connected.map(
    // 处理第一个流的数据
    (String value) -> "Stream 1: " + value,
    // 处理第二个流的数据
    (Integer value) -> "Stream 2: " + value
);
```

### 2.3 窗口操作

窗口是处理无界流的核心概念，它将无界流切分成有界的数据集进行处理。

#### 窗口类型

- **时间窗口**：基于时间的窗口
  - 滚动时间窗口（Tumbling Window）
  - 滑动时间窗口（Sliding Window）
  - 会话窗口（Session Window）

- **计数窗口**：基于元素数量的窗口
  - 滚动计数窗口
  - 滑动计数窗口

#### 窗口示例

```java
// 滚动时间窗口 - 每5秒统计一次
DataStream<Tuple2<String, Integer>> result = dataStream
    .keyBy(0)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .sum(1);

// 滑动时间窗口 - 每1秒统计过去5秒的数据
DataStream<Tuple2<String, Integer>> result = dataStream
    .keyBy(0)
    .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
    .sum(1);

// 会话窗口 - 如果10秒内没有数据，则认为一个会话结束
DataStream<Tuple2<String, Integer>> result = dataStream
    .keyBy(0)
    .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
    .sum(1);
```

### 2.4 时间语义

Flink支持三种时间语义：

- **事件时间（Event Time）**：事件实际发生的时间，由数据本身携带
- **处理时间（Processing Time）**：数据被处理的时间，由处理系统的时钟决定
- **摄入时间（Ingestion Time）**：数据进入Flink的时间，由数据源的时钟决定

```java
// 设置时间语义为事件时间
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

// 设置时间语义为处理时间
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

// 设置时间语义为摄入时间
env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
```

### 2.5 水印（Watermark）

水印是Flink处理事件时间的核心机制，它用于处理乱序数据和延迟数据。

```java
// 使用固定延迟的水印
DataStream<Event> withTimestampsAndWatermarks = dataStream
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
    );

// 使用周期性水印生成器
DataStream<Event> withTimestampsAndWatermarks = dataStream
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(5)) {
        @Override
        public long extractTimestamp(Event event) {
            return event.getTimestamp();
        }
    });
```

### 2.6 侧输出（Side Output）

侧输出允许从一个操作中输出多个数据流，常用于处理延迟数据、异常数据等。

```java
// 定义侧输出标签
final OutputTag<Event> lateDataTag = new OutputTag<Event>("late-data"){};  

// 在窗口操作中使用侧输出
SingleOutputStreamOperator<Result> result = dataStream
    .keyBy(Event::getId)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .allowedLateness(Time.seconds(1))  // 允许1秒的延迟
    .sideOutputLateData(lateDataTag)   // 将延迟数据发送到侧输出
    .process(new MyProcessWindowFunction());

// 获取侧输出流
DataStream<Event> lateData = result.getSideOutput(lateDataTag);
```

## 3. DataSet API深入

### 3.1 DataSet的创建

DataSet可以通过多种方式创建：

```java
// 从集合创建
DataSet<String> dataSet = env.fromCollection(Arrays.asList("a", "b", "c"));

// 从文件创建
DataSet<String> dataSet = env.readTextFile("file:///path/to/file");

// 从CSV文件创建
DataSet<Tuple3<Integer, String, Double>> dataSet = env
    .readCsvFile("file:///path/to/file")
    .types(Integer.class, String.class, Double.class);
```

### 3.2 DataSet转换操作

DataSet API提供了丰富的转换操作：

#### 基本转换

```java
// Map转换
DataSet<Integer> mapped = dataSet.map(s -> s.length());

// FlatMap转换
DataSet<String> flatMapped = dataSet.flatMap(
    (String s, Collector<String> out) -> {
        for (String word : s.split(" ")) {
            out.collect(word);
        }
    }
);

// Filter转换
DataSet<String> filtered = dataSet.filter(s -> s.startsWith("a"));
```

#### 聚合操作

```java
// GroupBy - 按键分组
UnsortedGrouping<Tuple2<String, Integer>> grouped = 
    dataSet.groupBy(0); // 按第一个字段分组

// Reduce - 归约操作
DataSet<Tuple2<String, Integer>> reduced = 
    grouped.reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));

// Aggregations - 聚合操作
DataSet<Tuple2<String, Integer>> summed = grouped.sum(1); // 对第二个字段求和
DataSet<Tuple2<String, Integer>> maxed = grouped.max(1);  // 对第二个字段求最大值
DataSet<Tuple2<String, Integer>> minBy = grouped.minBy(1); // 对第二个字段求最小值（保留原始记录）
```

#### 多数据集操作

```java
// Join - 连接两个数据集
DataSet<Tuple2<String, Integer>> joined = 
    dataSet1.join(dataSet2)
    .where(0)  // dataSet1的连接键
    .equalTo(1) // dataSet2的连接键
    .with(new JoinFunction<...>() {...});

// Cross - 笛卡尔积
DataSet<Tuple2<String, Integer>> crossed = 
    dataSet1.cross(dataSet2);

// CoGroup - 协同分组
DataSet<Tuple3<String, Integer, Integer>> coGrouped = 
    dataSet1.coGroup(dataSet2)
    .where(0)
    .equalTo(1)
    .with(new CoGroupFunction<...>() {...});
```

### 3.3 迭代操作

Flink支持在数据集上进行迭代操作，常用于机器学习算法等场景。

```java
// 有界迭代 - 指定最大迭代次数
IterativeDataSet<Integer> iteration = initialDataSet.iterate(10); // 最多迭代10次

DataSet<Integer> iterationBody = iteration.map(new MapFunction<...>() {...});

DataSet<Integer> result = iteration.closeWith(
    iterationBody,
    iterationBody.filter(new FilterFunction<...>() {...}) // 终止条件
);

// 增量迭代 - 只处理发生变化的数据
DeltaIteration<Integer, Integer> deltaIteration = 
    initialDataSet.iterateDelta(initialWorkset, 10, 0);

DataSet<Integer> changes = /* 计算变化的数据 */;
DataSet<Integer> delta = /* 计算增量 */;

DataSet<Integer> result = deltaIteration.closeWith(delta, changes);
```

## 4. ProcessFunction详解

ProcessFunction是Flink的低级API，提供了对时间和状态的细粒度控制。

### 4.1 ProcessFunction的类型

- **ProcessFunction**：基本的处理函数，用于DataStream
- **KeyedProcessFunction**：用于KeyedStream，可以访问KeyedState
- **CoProcessFunction**：用于ConnectedStreams，处理两个输入流
- **ProcessWindowFunction**：用于窗口操作，一次处理整个窗口的数据
- **ProcessAllWindowFunction**：用于全局窗口操作

### 4.2 KeyedProcessFunction示例

```java
public class MonitorFunction extends KeyedProcessFunction<String, Event, Alert> {
    // 声明状态变量
    private ValueState<Long> lastEventState;
    
    @Override
    public void open(Configuration parameters) {
        // 初始化状态
        lastEventState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastEvent", Long.class));
    }
    
    @Override
    public void processElement(Event event, Context ctx, Collector<Alert> out) throws Exception {
        // 获取当前事件的时间戳
        long eventTime = event.getTimestamp();
        
        // 获取上一次事件的时间戳
        Long lastEventTime = lastEventState.value();
        
        // 更新状态
        lastEventState.update(eventTime);
        
        // 如果不是第一个事件，且两个事件的时间间隔超过阈值，则发出告警
        if (lastEventTime != null && eventTime - lastEventTime > 10000) {
            out.collect(new Alert(event.getId(), "Time interval too large"));
        }
        
        // 注册一个10秒后的定时器
        ctx.timerService().registerEventTimeTimer(eventTime + 10000);
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // 定时器触发时的逻辑
        out.collect(new Alert(ctx.getCurrentKey(), "No event received for 10 seconds"));
    }
}
```

### 4.3 ProcessFunction的关键特性

- **细粒度的事件处理**：可以访问每个事件的详细信息
- **状态管理**：可以使用Flink的状态API管理状态
- **定时器**：可以注册基于事件时间或处理时间的定时器
- **侧输出**：可以产生多个输出流

## 5. 小结与练习

### 5.1 小结

- Flink提供了分层的API，从低级的ProcessFunction到高级的Table API/SQL
- DataStream API是处理无界流的核心API，提供了丰富的转换操作和窗口操作
- DataSet API是处理有界数据集的API，提供了丰富的转换操作和迭代操作
- ProcessFunction是Flink的低级API，提供了对时间和状态的细粒度控制
- Flink的时间语义和水印机制是处理乱序数据和延迟数据的关键

### 5.2 练习

1. 实现一个使用滑动窗口的实时热门商品统计程序
2. 使用ProcessFunction实现一个简单的CEP（复杂事件处理）功能，如检测连续登录失败
3. 实现一个使用增量迭代的PageRank算法

---

**下一模块预告**：在下一个模块中，我们将学习Flink的高级特性，包括状态管理、检查点机制、保存点机制等，以及如何使用这些特性构建可靠的流处理应用。