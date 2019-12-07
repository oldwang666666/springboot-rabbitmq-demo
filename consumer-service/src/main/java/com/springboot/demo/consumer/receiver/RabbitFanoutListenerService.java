package com.springboot.demo.consumer.receiver;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListeners;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 伞形交换机消费者
 * @Description
 * @Author longzhang.wang
 * @Since 1.0
 * @Date 2019/12/3
 */
@Component
@RabbitListeners({
        @RabbitListener(
                bindings = @QueueBinding(
                        value = @Queue("FanoutQueue-01"),
                        exchange = @Exchange(value = "FanoutExchange", type = ExchangeTypes.FANOUT),
                        key = "key.one")),

        @RabbitListener(
                bindings = @QueueBinding(
                        value = @Queue("FanoutQueue-02"),
                        exchange = @Exchange(value = "FanoutExchange", type = ExchangeTypes.FANOUT),
                        key = "key.two")),

        @RabbitListener(
                bindings = @QueueBinding(
                        value = @Queue("FanoutQueue-03"),
                        exchange = @Exchange(value = "FanoutExchange", type = ExchangeTypes.FANOUT),
                        key = "key.three")),
})
public class RabbitFanoutListenerService {

    @RabbitHandler
    public void getMessage(@Payload String msg, @Headers Map<String, Object> headers) {
        //headers.get(AmqpHeaders.CONSUMER_QUEUE) 打印的是QueueBinding中Queue的值
        System.out.println("接收" + headers.get(AmqpHeaders.CONSUMER_QUEUE) + "消费者的消息:" + msg);
    }
}
