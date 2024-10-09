package dev.faiyazsadi.scheduler.config;

import org.springframework.beans.factory.annotation.Value;
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
    @Value("${redis.host.name}")
    private String HOST_NAME;

    @Value("${redis.port.no}")
    private int PORT_NO;

    @Bean
    public JedisPool jedisPool() {
        return new JedisPool(HOST_NAME, PORT_NO);
    }
}
