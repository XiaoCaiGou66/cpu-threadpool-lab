package com.example.cputhreadpoollab;

import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CpuThreadpoolLabApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(CpuThreadpoolLabApplication.class);
        // 把端口写到代码里，避免学员再看额外配置文件。
        app.setDefaultProperties(Map.of("server.port", "8081"));
        app.run(args);
    }
}
