package dev.faiyazsadi.scheduler.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;

@Configuration
@EnableAutoConfiguration(exclude = {
        JmxAutoConfiguration.class
})
public class JedisConfig {
    @Bean
    public JedisPool jedisPool() {
        String HOST_NAME = "127.0.0.1";
        int PORT = 6379;
        return new JedisPool(HOST_NAME, PORT);
    }
}
