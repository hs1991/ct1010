package com.sh.consumer;

import com.sh.utils.PropertityUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class HbaseConsumer {
    public static void main(String args[]) throws Exception{
        //获取kafka配置信息
        Properties properties = PropertityUtil.getPropertity();

        //创建kafka消费者并订阅主题
        KafkaConsumer<Object, Object> kafkaConsumer = new KafkaConsumer<Object, Object>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(properties.getProperty("kafka.topics")));
        HbaseDao hbaseDao = new HbaseDao();
        //循环拉取数据并打印
       try {
           while (true) {
               ConsumerRecords<Object, Object> consumerRecords = kafkaConsumer.poll(100);
               for (ConsumerRecord<Object, Object> consumerRecord : consumerRecords) {
                   System.out.println(consumerRecord.value());
                   //put
                  hbaseDao.put((String) consumerRecord.value());
               }
           }
       }
       finally {
           hbaseDao.close();
       }
    }
    }
