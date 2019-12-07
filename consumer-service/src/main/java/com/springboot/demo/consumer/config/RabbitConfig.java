package com.springboot.demo.consumer.config;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    /**
     * 队列：TestQueue ,true 是否持久
     * @return
     */
    @Bean
    public Queue TestDirectQueue() {
        return new Queue("DefaultQueue",true);
    }
}
