package com.springboot.demo.consumer.receiver;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 直连交换机消费者
 * @Description
 * @Author longzhang.wang
 * @Since 1.0
 * @Date 2019/12/3
 */
@Component
@RabbitListener(bindings = @QueueBinding(value = @Queue("DirectQueue")
        , exchange = @Exchange(value = "DirectExchange", type = ExchangeTypes.DIRECT)
        , key = "direct.key"))
public class RabbitDirectListenerService {

    @RabbitHandler
    public void getMessage(@Payload String message) {

        System.out.println("getMessage消费者收到消息  : " + message);
    }
}
