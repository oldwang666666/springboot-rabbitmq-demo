package com.springboot.demo.consumer.receiver;

import com.rabbitmq.client.Channel;
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

    //这里只是demo所以直接抛出异常，实际业务要根据自己的业务进行异常捕获，使用basicAck、basicNack、basicReject等方法对消息进行处理，还有幂等处理
    @RabbitHandler
    public void getMessage(@Payload String msg, Channel channel, @Headers Map<String, Object> headers) throws Exception {

        long deliveryTag = (Long)headers.get(AmqpHeaders.DELIVERY_TAG);
        //headers.get(AmqpHeaders.CONSUMER_QUEUE) 打印的是QueueBinding中Queue的值
        System.out.println("幂等处理 & 业务处理  接收" + headers.get(AmqpHeaders.CONSUMER_QUEUE) + "消费者的消息:" + msg);
        //消息确认，deliveryTag:该消息的index，multiple：是否批量.true:将一次性ack所有小于deliveryTag的消息。
        channel.basicAck(deliveryTag, false);
    }
}
