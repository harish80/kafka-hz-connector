package com.example.demo;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
@EnableKafka

public class KafkaConfigs {
    private KafkaTemplate<String, String> kafkaTemplate;
    public String bootstrapServers = "localhost:9092";


    public static final int MESSAGE_COUNT_PER_TOPIC = 1_000_000;
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String AUTO_OFFSET_RESET = "earliest";

    public static final String SINK_NAME = "sink";

    @Bean
    public KafkaAdmin admin()
    {
        Map<String, Object> configs;
        configs = new HashMap<>();
        configs.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic3()
    {
        return TopicBuilder.name("t3")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topic4()
    {
        return TopicBuilder.name("t4")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topic5()
    {
        return TopicBuilder.name("t5")
                .partitions(1)
                .replicas(1)
                .build();
    }

    public static Properties props(String... kvs) {
        final Properties props = new Properties();
        for (int i = 0; i < kvs.length; ) {
            props.setProperty(kvs[i++], kvs[i++]);
        }
        return props;
    }


    public static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(props(
                                "bootstrap.servers", BOOTSTRAP_SERVERS,
                                "key.deserializer", StringDeserializer.class.getCanonicalName(),
                                "value.deserializer", StringDeserializer.class.getCanonicalName(),
                                "auto.offset.reset", AUTO_OFFSET_RESET)
                        , "t3", "t4"))
                .withoutTimestamps()
                .writeTo(Sinks.map(SINK_NAME));
        return p;
    }


    @Bean
    public ProducerFactory<String, String> userProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }



    @Bean
    public KafkaTemplate<String, String> userKafkaTemplate() {
        KafkaTemplate KafkaTemplate = new KafkaTemplate<>(userProducerFactory());
        KafkaTemplate.setConsumerFactory(consumerFactory());
        return KafkaTemplate;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }


    public static void printMapEntries(HazelcastInstance hzInstance) {
        Map<String, String> sinkEntries = hzInstance.getMap(KafkaConfigs.SINK_NAME);
        System.out.println("----------COUNT----------->"+ sinkEntries.entrySet().stream().count());

    }
}
