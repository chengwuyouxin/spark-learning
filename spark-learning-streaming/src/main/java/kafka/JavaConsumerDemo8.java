package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;

public class JavaConsumerDemo8 {
    public static void main(String[] args){
        Properties props=new Properties();
        props.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","earliest");
        props.put("group.id","javaconsumerdemo10");
        props.put("auto.commit.offset","false");

        final KafkaConsumer<Integer,String> consumer=new KafkaConsumer<Integer, String>(props);
        consumer.subscribe(Arrays.asList("JavaProducerDemo2"));

        final Thread mainThread=Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                System.out.println("Starting exit……");
                consumer.wakeup();
                try{
                    mainThread.join();
                }catch(InterruptedException ex){
                    ex.printStackTrace();
                }
            }
        });

        try{
            while(true){
                ConsumerRecords<Integer,String> records=consumer.poll(100);
                for(ConsumerRecord<Integer,String> record:records){
                    System.out.println(String.format("partition: %d,offerset: %d,key:%s,value:%s",
                            record.partition(),record.offset(),record.key(),record.value()));
                }
                consumer.commitAsync();
            }

        }catch(WakeupException ex){

        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            consumer.commitSync();
            consumer.close();
            System.out.println("consumer is closed!");
        }
    }
}
