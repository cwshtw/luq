package com.bigdata.main;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class Main {
    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(Main.class);

        public static void main(String[] args) {
        Properties props = new Properties();
        // zookeeper 配置


        props.put("zookeeper.connect", "10.180.225.235:2181,10.180.225.236:2181,10.180.225.237:2181");//10.189.100.6:2181,10.189.100.7:2181,10.189.100.8:2181//10.180.225.235:2181,10.180.225.236:2181,10.180.225.237:2181
        // group 代表一个消费组props.put("bootstrap.servers", "10.180.225.235:9092,10.180.225.236:9092,10.180.225.237:9092");
        props.put("group.id", "group9_qs");
        // zk连接超时
        props.put("zookeeper.session.timeout.ms", "5000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("rebalance.max.retries", "5");
        props.put("rebalance.backoff.ms", "1200");
        props.put("auto.offset.reset", "smallest");
        //props.put("advertised.host.name", "qs");
        // 序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        String topicName="LH_YD_dataAnalysisPro";//"LH_YD_dataAnalysisPro";
        topicCountMap.put(topicName, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topicName).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()){
            String msg = new String(it.next().message());
            System.out.println(""+msg);
        }
    }
//    public static void main(String[] args) {
//        Properties props = new Properties();
//        String kafkaServer=args[0];
//        System.out.println(kafkaServer);
//        String topic=args[1];
//        String groupId=args[2];
//        String offset=args[3];
//        System.out.println(topic);
//        props.put("bootstrap.servers", kafkaServer);//kafkaTestt.jar
//        props.put("group.id", groupId);
//        props.put("auto.offset.reset", offset);
//        props.put("key.deserializer", StringDeserializer.class.getName());
//        props.put("value.deserializer", StringDeserializer.class.getName());
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Arrays.asList(groupId));
//        try {
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(1000);
//                for (ConsumerRecord<String, String> record : records)
//                    System.out.println(record.offset() + ": " + record.value());
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        } finally {
//            consumer.close();
//        }
//
//    }
}
