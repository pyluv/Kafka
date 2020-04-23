package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Classname MyProducer
 * @Description TODO
 * @Date 4/22/2020 3:08 PM
 * @Created by Administrator
 */
public class MyProducer {
    //  test topic分区是2 副本为3
    //  bin/kafka-topics.sh --zookeeper hadoop102:2181 --create --replication-factor 3 --partitions 2 -topic test
    //  bin/kafka-console-consumer.sh --zookeeper hadoop102:2181 --topic test --consumer.config config/consumer.properties
//消费数据根据Range机制，并发消费
//            massage1
//            massage3
//            massage5
//            massage7
//            massage9
//            massage0
//            massage2
//            massage4
//            massage6
//            massage8

    public static void main(String[] args) {
        //创建kafka生产者的配置信息
        Properties props = new Properties();
        //指定连接kafka集群
        //props.put("bootstrap.servers", "hadoop102:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        //ACK应答级别
        //props.put("acks", "all");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        //重试次数
        props.put("retries", 1);

        //批次大小 每个批次满了后存入RecordAccumulator缓冲区
        //只有数据积累到 batch.size 之后，sender 才会发送数据
        props.put("batch.size", 16384);

        //等待时间
        //如果数据迟迟未达到 batch.size，sender 等待 linger.time 之后就会发送数据
        props.put("linger.ms", 1);

        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);

        //key value的序列化类
        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2 构建拦截链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("interceptor.TimeInterceptor");
        interceptors.add("interceptor.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //发送数据
        for (int i =0; i<10; i++) {
            producer.send(new ProducerRecord<String, String>("test", "key","massage" + i));
        }

        //关闭资源 会调用interceptor中的close方法
        producer.close();

    }
}
