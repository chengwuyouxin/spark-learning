package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class JavaProducerDemo2 {
    public static void main(String[] args){
        //kafka-topics.sh --zookeeper localhost:2181
        // --delete --topic JavaProducerDemo2               删除分区

        //kafka-topics.sh --create --zookeeper localhost:2181
        // --replication-factor 1 --partitions 5 --topic JavaProducerDemo2  创建分区

        Properties props = new Properties();
        props.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer, String>(props);
        String topic="JavaProducerDemo2";
        int messageNo=1;

        //同步发送，send方法返回一个Future对象，调用get方法的等待kafka响应，正常返回一个RecordMetaData对象，
        //如果发生错误会抛出异常
        while(messageNo<=20){
            String message=String.format("JavaProducer produce %s message",
                    String.valueOf(messageNo));
            try{
                RecordMetadata metadata=producer.send(new ProducerRecord<Integer, String>(
                        topic,messageNo,message)).get();
                System.out.println(String.format("Message %d was sent to pattition: %d,offset:%d",
                        messageNo,metadata.partition(),metadata.offset()));
            }catch(Exception e){
                e.printStackTrace();
            }
            messageNo++;
        }
        producer.close();
    }



}
