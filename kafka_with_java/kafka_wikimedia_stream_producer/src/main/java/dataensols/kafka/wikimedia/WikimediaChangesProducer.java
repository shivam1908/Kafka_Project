package dataensols.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.beans.EventHandler;
import java.net.URI;
import java.util.Properties;

public class WikimediaChangesProducer {
    public static void main(String[] args) {

        // setting bootstrap server
        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        //Defing Eventhandler
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        BackgroundEventHandler backgroundEventHandler = new WikimediaChangeHandler(producer,topic);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder backgroundgEventSourceBuilder = new BackgroundEventSource.Builder(backgroundEventHandler,eventSourceBuilder);
        BackgroundEventSource backgroundEventSource = backgroundgEventSourceBuilder.build();
        // to  start producer in another thread
        backgroundEventSource.start();
    }
}
