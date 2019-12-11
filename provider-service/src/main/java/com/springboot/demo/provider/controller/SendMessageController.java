package com.springboot.demo.provider.controller;

import com.springboot.demo.provider.service.SendMessageService;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * rabbitmq生产控制层
 * @Description
 * @Author longzhang.wang
 * @Since 1.0
 * @Date 2019/12/3
 */
@RequestMapping("/rabbitmq")
@RestController
public class SendMessageController {

    //使用RabbitTemplate,来发送消息
    @Autowired
    RabbitTemplate rabbitTemplate;
    @Autowired
    private SendMessageService sendMessageService;

    /**
     * @MethodName Default 默认交换机 demo
     * @Description
     * @Author longzhang.wang
     * @Version V1.0.0
     * @Since 2019/12/11
     */
    @RequestMapping("/defaultSendMessage")
    public String defaultSendMessage() {
        String messageId = String.valueOf(UUID.randomUUID());
        String messageData = "Default 默认交换机 消息发送";
        String createTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Map<String,Object> map=new HashMap<>();
        map.put("messageId",messageId);
        map.put("messageData",messageData);
        map.put("createTime",createTime);

        System.out.println("发送消息：" + map.toString());
        rabbitTemplate.convertAndSend("DefaultQueue", map.toString());
        return "发送成功";
    }

   /**
    * @MethodName Direct 直连交换机 demo
    * @Description
    * @Author longzhang.wang
    * @Version V1.0.0
    * @Since 2019/12/11
    */
    @RequestMapping("/directSendMessage")
    public String directSendMessage() {
        String messageId = String.valueOf(UUID.randomUUID());
        String messageData = "Direct 直连交换机 消息发送";
        String createTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Map<String,Object> map=new HashMap<>();
        map.put("messageId",messageId);
        map.put("messageData",messageData);
        map.put("createTime",createTime);

        System.out.println("发送消息：" + map.toString());
        //(交换机, routingKey, 消息内容)
        rabbitTemplate.convertAndSend("DirectExchange","direct.key", map.toString());

        return "发送成功";
    }

    /**
     * @MethodName Fanout 伞形交换机 demo
     * @Description
     * @Author longzhang.wang
     * @Version V1.0.0
     * @Since 2019/12/11
     */
    @RequestMapping("/fanoutSendMessage")
    public String fanoutSendMessage() {
        String messageId = String.valueOf(UUID.randomUUID());
        String messageData = "Fanout 伞形交换机 消息发送";
        String createTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Map<String,Object> map=new HashMap<>();
        map.put("messageId",messageId);
        map.put("messageData",messageData);
        map.put("createTime",createTime);

        System.out.println("发送消息：" + map.toString());
        //(交换机, routingKey, 消息内容)
        rabbitTemplate.convertAndSend("FanoutExchange","none", map.toString());

        return "发送成功";
    }


    /**
     * @MethodName 消息确认模式 demo
     * @Description
     * @Author longzhang.wang
     * @Version V1.0.0
     * @Since 2019/12/11
     */
    @RequestMapping("/sendConfirmMessage")
    public String SendConfirmMessage() {
        String messageId = String.valueOf(UUID.randomUUID());
        String messageData = "SendConfirmMessage 消息发送";
        String createTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Map<String,Object> map=new HashMap<>();
        //业务主键
        map.put("messageId",messageId);
        map.put("messageData",messageData);
        map.put("createTime",createTime);

        sendMessageService.sendString(map.toString());
        return "发送成功";
    }
}
