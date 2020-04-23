package producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Classname MyPartioner
 * @Description TODO
 * @Date 4/22/2020 4:47 PM
 * @Created by Administrator
 */
public class MyPartitioner implements Partitioner {


    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
