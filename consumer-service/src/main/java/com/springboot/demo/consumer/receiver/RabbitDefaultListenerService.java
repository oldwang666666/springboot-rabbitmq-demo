package com.springboot.demo.consumer.receiver;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 默认交换机消费者
 * @Description
 * @Author longzhang.wang
 * @Since 1.0
 * @Date 2019/12/3
 */
@Component
@RabbitListener(queues = "DefaultQueue")
public class RabbitDefaultListenerService {

    @RabbitHandler
    public void getMessage(@Payload String message) {

        System.out.println("getMessage消费者收到消息  : " + message);
    }
}
