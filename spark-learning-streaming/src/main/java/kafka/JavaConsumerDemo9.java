package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//多线程consumer的实现
//在Kafka_Consumer里面接收数据，在ConsumerThread中处理数据
public class JavaConsumerDemo9 {
    public static void main(String[] args){
        Kafka_Consumer kafka_consumer=new Kafka_Consumer();
        try{
            kafka_consumer.execute();
            Thread.sleep(2000);
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }finally {
            kafka_consumer.shutdown();
        }
    }

}

class Kafka_Consumer{
    private final KafkaConsumer<String,String> consumer;
    private ExecutorService executorService;

    public Kafka_Consumer(){
        Properties props=new Properties();
        props.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","earliest");
        props.put("group.id","javaconsumerdemo9");

        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.commit.interval.ms","1000");
        consumer=new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList("JavaProducerDemo2"));
    }

    public void execute(){
        executorService=Executors.newFixedThreadPool(6);
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(100);
            if(null != records){
                executorService.submit(new ConsumerThread(records));
            }
        }
    }
    public void shutdown(){
        try{
            if(consumer!=null){
                consumer.close();
            }
            if(executorService != null){
                executorService.shutdown();
            }
            if(!executorService.awaitTermination(1000, TimeUnit.MILLISECONDS)){
                executorService.shutdownNow();
            }
        }catch(Exception ex){
            executorService.shutdownNow();
        }
    }
}

class ConsumerThread implements Runnable{
    private ConsumerRecords<String,String> records;
    public ConsumerThread(ConsumerRecords<String,String> records){
        this.records=records;
    }

    public void run() {
        for(ConsumerRecord<String,String> record:records){
            System.out.println("当前线程:"+Thread.currentThread()+",偏移量:"+
            record.offset()+",主题:"+record.topic()+",分区:"+record.partition()
            +",获取的消息:"+record.value());
        }
    }
}
