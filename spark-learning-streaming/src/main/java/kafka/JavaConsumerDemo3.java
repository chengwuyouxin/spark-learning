package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

//同步提交偏移量，处理完poll方法返回的偏移量之后commitSync()手工提交偏移量,若提交失败，不会重试，直接抛出异常
//若发生了再均衡，从最近一批消息到再均衡之间的所有消息都将被重复处理

public class JavaConsumerDemo3 {
    public static void main(String[] args){
        Properties props=new Properties();
        props.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","earliest");
        props.put("group.id","javaconsumerdemo3");

        //同步提交偏移量需要设置为false
        props.put("auto.commit.offset","false");

        KafkaConsumer<Integer,String> consumer=new KafkaConsumer<Integer, String>(props);
        consumer.subscribe(Arrays.asList("JavaProducerDemo2"));

        try{
            while(true){
                ConsumerRecords<Integer,String> records=consumer.poll(100);
                for(ConsumerRecord<Integer,String> record:records){
                    System.out.println(String.format("partition: %d,offerset: %d,key:%s,value:%s",
                            record.partition(),record.offset(),record.key(),record.value()));
                }
                try{
                    consumer.commitAsync();
                }catch(Exception ex){
                    System.out.println("commit failed!");
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            consumer.close();
        }
    }
}
