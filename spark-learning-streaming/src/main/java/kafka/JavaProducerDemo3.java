package kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class JavaProducerDemo3 {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic="JavaProducerDemo3";
        KafkaProducer<Integer,String> producer=new KafkaProducer<Integer, String>(props);
        List<PartitionInfo> partitions=producer.partitionsFor(topic);
        for(PartitionInfo pi:partitions){
            System.out.println(pi.toString());
        }

        //异步发送消息
        for(int i=0;i<10;i++){
            String message= UUID.randomUUID().toString();
            long starttime=System.currentTimeMillis();
            producer.send(new ProducerRecord<Integer, String>(topic,i,
                    message),new ProducerAckCallback(starttime,i,message));
        }
        producer.close();
    }

    static class ProducerAckCallback implements Callback {
        private final Long startTime;
        private final int key;
        private final String value;

        public ProducerAckCallback(Long startTime, int key, String value) {
            this.startTime = startTime;
            this.key = key;
            this.value = value;
        }

        public void onCompletion(RecordMetadata metadata, Exception e) {
            long spendTime = System.currentTimeMillis() - startTime;
            if (null != metadata) {
                System.out.println("消息(" + key + "," + value +
                        ")send to partition(" + metadata.partition()
                        + " and offest " + metadata.offset() +
                        " and spend  " + spendTime + " ms");
            }
        }
    }
}
