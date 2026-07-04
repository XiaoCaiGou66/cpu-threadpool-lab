package com.example.cputhreadpoollab;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.management.OperatingSystemMXBean;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class AggregateService {
    // 建议阅读顺序：1) process -> 2) doProcess -> 3) asyncFullLog -> 4) metrics。
    // 先看主链路和日志背压，再看线程池构造与指标统计。
    // 调参入口：BIZ_*、LOG_*、MIN/MAX_LATENCY_MS。
    /*
     * 实验目标：
     * 1) 业务线程池处理主链路（解析/聚合/返回）
     * 2) 日志线程池处理 4 次完整 request/response 序列化与打印
     * 3) 学员只通过调线程池参数观察 CPU、延迟和拒绝变化
     */

    private static final Logger log = LoggerFactory.getLogger(AggregateService.class);
    private static final Logger auditLog = LoggerFactory.getLogger("auditLogger");

    private final ThreadPoolExecutor bizExecutor;
    private final ThreadPoolExecutor logExecutor;
    private final ObjectMapper objectMapper;


    // 默认是“未优化配置”，用于让学员先复现高 CPU，再自己改参数优化。
/*
    private static final int BIZ_CORE = 32;
    private static final int BIZ_MAX = 64;
    private static final int BIZ_QUEUE = 10000;
    private static final String BIZ_POLICY = "caller_runs";

    private static final int LOG_CORE = 16;
    private static final int LOG_MAX = 32;
    private static final int LOG_QUEUE = 50000;
    private static final String LOG_POLICY = "caller_runs";

    private static final int MIN_LATENCY_MS = 30;
    private static final int MAX_LATENCY_MS = 80;
*/

    //修改后参数

    private static final int BIZ_CORE = 24;
    private static final int BIZ_MAX = 24;
    private static final int BIZ_QUEUE = 1000;
    private static final String BIZ_POLICY = "caller_runs";

    private static final int LOG_CORE = 2;
    private static final int LOG_MAX = 4;
    private static final int LOG_QUEUE = 2000;
    private static final String LOG_POLICY = "discard";

    private static final int MIN_LATENCY_MS = 30;
    private static final int MAX_LATENCY_MS = 80;


    private final int minLatencyMs;
    private final int maxLatencyMs;

    private final AtomicLong total = new AtomicLong();
    private final AtomicLong success = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong bizReject = new AtomicLong();
    private final AtomicLong logReject = new AtomicLong();

    private final Deque<Long> latencyWindow = new ArrayDeque<>();
    private final int latencyWindowSize = 2000;

    public AggregateService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.minLatencyMs = MIN_LATENCY_MS;
        this.maxLatencyMs = MAX_LATENCY_MS;

        this.bizExecutor = buildExecutor("biz", BIZ_CORE, BIZ_MAX, BIZ_QUEUE, BIZ_POLICY, true);
        this.logExecutor = buildExecutor("log", LOG_CORE, LOG_MAX, LOG_QUEUE, LOG_POLICY, false);
    }

    public ResponseEntity<Map<String, Object>> process(Map<String, Object> body) {
        // 大报文校验：保证实验符合“重请求体”场景。
        String payload = String.valueOf(body.getOrDefault("payload", ""));
        if (payload.length() < 1024) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("code", 400, "msg", "payload length must be >= 1024"));
        }

        String traceId = String.valueOf(body.getOrDefault("traceId", UUID.randomUUID().toString()));
        String userId = String.valueOf(body.getOrDefault("userId", "unknown"));
        total.incrementAndGet();

        Future<Map<String, Object>> future;
        try {
            // 主链路明确走业务池，避免 Tomcat 工作线程直接承压。
            future = bizExecutor.submit(() -> doProcess(traceId, userId, payload));
        } catch (RejectedExecutionException e) {
            bizReject.incrementAndGet();
            failed.incrementAndGet();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("code", 503, "msg", "biz executor overloaded"));
        }

        try {
            Map<String, Object> resp = future.get();
            return ResponseEntity.ok(resp);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failed.incrementAndGet();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("code", 500, "msg", "interrupted"));
        } catch (ExecutionException e) {
            failed.incrementAndGet();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("code", 500, "msg", "internal error"));
        }
    }

    public Map<String, Object> metrics() {
        // 指标口径尽量简单，便于学员对比“调参前后”。
        Map<String, Object> data = new HashMap<>();
        data.put("cpuLoad", getCpuLoad());
        data.put("bizPool", poolSnapshot(bizExecutor, bizReject.get()));
        data.put("logPool", poolSnapshot(logExecutor, logReject.get()));

        Map<String, Object> request = new HashMap<>();
        request.put("total", total.get());
        request.put("success", success.get());
        request.put("failed", failed.get());
        request.put("p95Ms", p95());
        request.put("sampleSize", latencySize());
        data.put("request", request);
        return data;
    }

    private Map<String, Object> doProcess(String traceId, String userId, String payload) {
        long startNs = System.nanoTime();
        int checksum = calcChecksum(payload);
        int payloadBytes = payload.getBytes(StandardCharsets.UTF_8).length;

        // 固定 4 个下游交互；每次都必须记录完整请求和响应日志。
        for (String system : Arrays.asList("CRM", "USER", "STOCK", "PAY")) {
            Map<String, Object> req = new HashMap<>();
            req.put("traceId", traceId);
            req.put("userId", userId);
            req.put("system", system);
            req.put("payloadBytes", payloadBytes);
            req.put("checksum", checksum);

            Map<String, Object> resp = callDownstream(system, traceId, payloadBytes, checksum);
            // 每次下游交互都记录一次完整 request/response（总计 4 次）
            asyncFullLog(traceId, system, req, resp);
        }

        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
        recordLatency(elapsedMs);
        success.incrementAndGet();

        Map<String, Object> result = new HashMap<>();
        result.put("code", 0);
        result.put("msg", "ok");
        result.put("traceId", traceId);
        result.put("elapsedMs", elapsedMs);
        result.put("checksum", checksum);
        return result;
    }

    private Map<String, Object> callDownstream(String system, String traceId, int payloadBytes, int checksum) {
        int sleepMs = ThreadLocalRandom.current().nextInt(minLatencyMs, maxLatencyMs + 1);
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Map<String, Object> resp = new HashMap<>();
        resp.put("system", system);
        resp.put("traceId", traceId);
        resp.put("status", "OK");
        resp.put("payloadBytes", payloadBytes);
        resp.put("checksum", checksum);
        resp.put("costMs", sleepMs);
        return resp;
    }

    private void asyncFullLog(String traceId, String system, Object req, Object resp) {
        Runnable task = () -> {
            try {
                String reqJson = objectMapper.writeValueAsString(req);
                String respJson = objectMapper.writeValueAsString(resp);
                auditLog.info("[FULL] traceId={} system={} req={} resp={}", traceId, system, reqJson, respJson);
            } catch (JsonProcessingException e) {
                auditLog.error("serialize failed, traceId={}, system={}", traceId, system, e);
            }
        };

        try {
            logExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            // 日志不丢：日志池满时在当前线程执行（背压会反向作用到主链路）。
            logReject.incrementAndGet();
            task.run();
        }
    }

    private int calcChecksum(String payload) {
        int checksum = 0;
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        for (int round = 0; round < 20; round++) {    //20改为2
            for (byte b : bytes) {
                checksum = (checksum * 31 + (b & 0xff)) & 0x7fffffff;
            }
        }
        return checksum;
    }

    private ThreadPoolExecutor buildExecutor(
            String name, int core, int max, int queue, String policy, boolean isBizPool) {
        AtomicLong threadSeq = new AtomicLong(1);
        return new ThreadPoolExecutor(
                core,
                max,
                60L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queue),
                r -> {
                    Thread t = new Thread(r);
                    t.setName(name + "-pool-" + threadSeq.getAndIncrement());
                    return t;
                },
                (r, executor) -> {
                    if (isBizPool) {
                        bizReject.incrementAndGet();
                    } else {
                        logReject.incrementAndGet();
                    }

                    // caller_runs: 用调用线程兜底，体现“拒绝后退化执行”的影响。
                    if ("caller_runs".equalsIgnoreCase(policy)) {
                        if (!executor.isShutdown()) {
                            r.run();
                        }
                        return;
                    }
                    throw new RejectedExecutionException("task rejected in " + name + " pool");
                }
        );
    }

    private synchronized void recordLatency(long valueMs) {
        if (latencyWindow.size() >= latencyWindowSize) {
            latencyWindow.removeFirst();
        }
        latencyWindow.addLast(valueMs);
    }

    private synchronized long p95() {
        if (latencyWindow.isEmpty()) {
            return 0;
        }
        // 用窗口内样本近似 P95，足够支撑教学对比。
        Long[] arr = latencyWindow.toArray(new Long[0]);
        Arrays.sort(arr);
        int idx = (int) Math.ceil(arr.length * 0.95) - 1;
        idx = Math.max(0, Math.min(idx, arr.length - 1));
        return arr[idx];
    }

    private synchronized int latencySize() {
        return latencyWindow.size();
    }

    private Map<String, Object> poolSnapshot(ThreadPoolExecutor executor, long rejectCount) {
        Map<String, Object> m = new HashMap<>();
        m.put("core", executor.getCorePoolSize());
        m.put("max", executor.getMaximumPoolSize());
        m.put("poolSize", executor.getPoolSize());
        m.put("active", executor.getActiveCount());
        m.put("queueSize", executor.getQueue().size());
        m.put("taskCount", executor.getTaskCount());
        m.put("completed", executor.getCompletedTaskCount());
        m.put("rejectCount", rejectCount);
        return m;
    }

    private double getCpuLoad() {
        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        double load = osBean.getProcessCpuLoad();
        return load < 0 ? 0 : Math.round(load * 10000.0) / 100.0;
    }

    @PreDestroy
    public void shutdown() {
        log.info("shutdown executors");
        bizExecutor.shutdown();
        logExecutor.shutdown();
    }
}
