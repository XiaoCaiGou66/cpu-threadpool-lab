# 优化后保证吞吐能力不变的前提下，明显降低 CPU 使用率

## 对线程池参数进行优化。优化后的配置如下：

```java
private static final int BIZ_CORE = 24;
private static final int BIZ_MAX = 24;
private static final int BIZ_QUEUE = 1000;
private static final String BIZ_POLICY = "caller_runs";

private static final int LOG_CORE = 2;
private static final int LOG_MAX = 4;
private static final int LOG_QUEUE = 2000;
private static final String LOG_POLICY = "discard";
```

1、业务线程池方面，将核心线程数从 32 调整为 24，最大线程数也设置为 24。根据前面的计算，70 QPS 下理论业务并发约为 15.4，设置 24 个业务线程既能覆盖正常压测需求，也能应对一定波动。同时相比原来的 32 个核心线程，减少了不必要的线程竞争和上下文切换。

2、业务线程池的拒绝策略仍然保留 caller_runs，因为业务任务属于核心链路，不能直接丢弃。当业务线程池压力过大时，使用 caller_runs 可以起到一定的反压作用，避免请求任务被直接丢弃。

3、同时，将日志拒绝策略从 caller_runs 改为 discard。原因是日志属于非核心链路，在高压场景下允许降级。如果日志线程池处理不过来，直接丢弃部分日志任务，避免日志任务占用业务线程，从而保护主业务吞吐能力。

## 优化前：

![优化前压测结果](./doc/before.png)
## 优化后
![优化后压测结果](./doc/after.png)


