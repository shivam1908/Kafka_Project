package dataensols.projects.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
    log.info("Hello World");

    //Create Producer properties
        Properties producerProperties = new Properties();
        //set bootstrap server details to connect to it using setProperty
        producerProperties.setProperty("bootstrap.servers","127.0.0.1:9092");
        producerProperties.setProperty("bootstrap.servers","127.0.0.1:9092");
        producerProperties.setProperty("bootstrap.servers","127.0.0.1:9092");
        producerProperties.setProperty("bootstrap.servers","127.0.0.1:9092");
        producerProperties.setProperty("bootstrap.servers","127.0.0.1:9092");
        producerProperties.setProperty("bootstrap.servers","127.0.0.1:9092");

    }
