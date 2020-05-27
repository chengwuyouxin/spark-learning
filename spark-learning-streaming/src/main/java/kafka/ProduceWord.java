package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

public class ProduceWord {
    public static void main(String[] args){
        Properties props=new Properties();
        props.put("bootstrap.servers","dn01:9092,dn02:9092,dn03:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Integer,String> producer=new KafkaProducer<Integer, String>(props);
        String topic="wordcount";

        String[] words={"spark","hadoop","hive","kafka",
                "redis","mongodb","mysql","sacla","linux","flume"};
        Random random=new Random(25);
        while(true){
            int i=random.nextInt(10);
            try{
                RecordMetadata metadata=producer.send(new ProducerRecord<Integer, String>(
                        topic,i,words[i])).get();
                System.out.println(String.format("send %s to topic:%s partition:%s offset:%d",
                        words[i],metadata.topic(),metadata.partition(),metadata.offset()));
//                Thread.sleep(1);
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
    }
}
