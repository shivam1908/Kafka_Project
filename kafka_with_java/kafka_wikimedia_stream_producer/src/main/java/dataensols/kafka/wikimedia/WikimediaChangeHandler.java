package dataensols.kafka.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

public class WikimediaChangeHandler implements BackgroundEventHandler {
    KafkaProducer<String , String> kafkaProducer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer,String topic){
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }
    @Override
    public void onOpen() {

    }

    @Override
    public void onClosed() {
    kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws InterruptedException {
        log.info(messageEvent.getData());
        //asynchronous
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
        TimeUnit.SECONDS.sleep(1);
    }

    @Override
    public void onComment(String s)  {

    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in the Stream Reading from wikimedia");
    }
}
