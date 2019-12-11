package com.springboot.demo.consumer.receiver;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/*
主要方法介绍，参考文章：https://www.cnblogs.com/piaolingzxh/p/5448927.html   rabbitmq channel参数详解
------------------------------------------------------
1、channel.basicPublish

exchange:交换器
routingKey:路由键，#匹配0个或多个单词，*匹配一个单词，在topic exchange做消息转发用
mandatory:true:如果exchange根据自身类型和消息routeKey无法找到一个符合条件的queue，那么会调用basic.return方法将消息返还给生产者。false:出现上述情形broker会直接将消息扔掉
immediate:true:如果exchange在将消息route到queue(s)时发现对应的queue上没有消费者，那么这条消息不会放入队列中。当与消息routeKey关联的所有queue(一个或多个)都没有消费者时，该消息会通过basic.return方法返还给生产者。
BasicProperties:需要注意的是BasicProperties.deliveryMode，0:不持久化 1：持久化 这里指的是消息的持久化，配合channel(durable=true),queue(durable)可以实现，即使服务器宕机，消息仍然保留
body：要发送的消息body数组
PS：简单来说：mandatory标志告诉服务器至少将该消息route到一个队列中，否则将消息返还给生产者；immediate标志告诉服务器如果该消息关联的queue上有消费者，则马上将消息投递给它，如果所有queue都没有消费者，直接把消息返还给生产者，不用将消息入队列等待消费者了。
void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, BasicProperties props, byte[] body) throws IOException;
------------------------------------------------------
2、channel.basicAck();

deliveryTag:该消息的index
multiple:是否批量.true:将一次性ack所有小于deliveryTag的消息。
void basicAck(long deliveryTag, boolean multiple) throws IOException;
------------------------------------------------------
3、channel.basicNack();

deliveryTag:该消息的index
multiple：是否批量.true:将一次性拒绝所有小于deliveryTag的消息。
requeue：被拒绝的是否重新入队列
void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException;
------------------------------------------------------
4、channel.basicReject()

deliveryTag:该消息的index
requeue：被拒绝的是否重新入队列
PS:channel.basicNack 与 channel.basicReject 的区别在于basicNack可以拒绝多条消息，而basicReject一次只能拒绝一条消息
void basicReject(long deliveryTag, boolean requeue) throws IOException;
*/

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
    public void getMessage(@Payload String message, Channel channel, @Headers Map<String, Object> headers) throws Exception {

        long deliveryTag = (Long)headers.get(AmqpHeaders.DELIVERY_TAG);
        try {
            //业务逻辑处理......
            System.out.println("幂等处理 & 业务处理 getMessage消费者String收到deliveryTag : " + deliveryTag + ", 收到消息: " + message);
            //消息确认，deliveryTag:该消息的index，multiple：是否批量.true:将一次性ack所有小于deliveryTag的消息。
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            //deliveryTag:该消息的index，multiple：是否批量.true:将一次性拒绝所有小于deliveryTag的消息，requeue：被拒绝的是否重新入队列(false:不进入，true:进入)
            channel.basicNack(deliveryTag , false,true);
            throw new RuntimeException(e);
        }
    }

    @RabbitHandler
    public void getMessage(@Payload byte[] message, Channel channel, @Headers Map<String, Object> headers) throws Exception {

        long deliveryTag = (Long)headers.get(AmqpHeaders.DELIVERY_TAG);
        //做拒绝查看demo，没有私信队列，所以拒绝后会一直重发，方便查看rabbimq消息
        if(deliveryTag > 4) {
            System.out.println("deliveryTag:" + deliveryTag + "，---------消息拒绝 : " + (new String(message)));
            //消息拒绝deliveryTag:该消息的index，requeue：被拒绝的是否重新入队列(false:不进入，true:进入)
            channel.basicReject(deliveryTag, true);
//            channel.basicNack(deliveryTag , false,true);
        } else {
            System.out.println("幂等处理 & 业务处理 getMessage消费者byte收到deliveryTag : " + deliveryTag + "消费者byte收到消息  : " + (new String(message)));
            channel.basicAck(deliveryTag, false);
        }
    }
}
