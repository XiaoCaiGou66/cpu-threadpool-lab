package com.example.cputhreadpoollab;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/aggregate")
public class AggregateController {

    private final AggregateService aggregateService;

    public AggregateController(AggregateService aggregateService) {
        this.aggregateService = aggregateService;
    }

    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> process(@RequestBody Map<String, Object> request) {
        // 控制器只做转发，复杂逻辑统一放在 Service，便于学员集中阅读。
        return aggregateService.process(request);
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        return aggregateService.metrics();
    }
}
