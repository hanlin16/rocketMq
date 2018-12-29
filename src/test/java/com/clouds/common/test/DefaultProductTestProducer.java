package com.clouds.common.test;

import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.clouds.common.rocketmq.consumer.RocketMqConsumer;
import com.clouds.common.rocketmq.consumer.processor.MQConsumeMsgListenerProcessor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DefaultProductTestProducer {
	private static final Logger logger = LoggerFactory.getLogger(DefaultProductTestProducer.class);
	
	/**使用RocketMq的生产者*/
	@Autowired
	private DefaultMQProducer defaultMQProducer;
	
	/**
	 * 发送消息
	 * 
	 * 2018年3月3日 zhaowg
	 * @throws InterruptedException 
	 * @throws MQBrokerException 
	 * @throws RemotingException 
	 * @throws MQClientException 
	 */
	@Test
	public void send() throws MQClientException, RemotingException, MQBrokerException, InterruptedException{
		String msg = "demo msg test911";
		logger.info("开始发送消息："+msg);
		Message sendMsg = new Message("DemoTopic","demoTag",msg.getBytes());
		//默认3秒超时
		defaultMQProducer.setVipChannelEnabled(false);
		SendResult sendResult = defaultMQProducer.send(sendMsg);
		logger.info("消息发送响应信息："+sendResult.toString());
	}

	@Test
	public void TestRocketMqConsumer() throws Exception{
		InputStream is = getClass().getClassLoader().getResourceAsStream("rocketMq.properties");
		Properties properties = new Properties();
		properties.load(is);

		Map<String, Object> map = new HashMap<String, Object>((Map) properties);
		MQConsumeMsgListenerProcessor mqConsumeMsgListenerProcessor = new MQConsumeMsgListenerProcessor();
		map.put("mqMessageListenerProcessor",mqConsumeMsgListenerProcessor);
		map.put("messageModel", MessageModel.CLUSTERING);

		RocketMqConsumer rocketMqConsumer = new RocketMqConsumer(map);

//        rocketMqConsumer.shutdown();


	}
}
