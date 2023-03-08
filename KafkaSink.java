package com.example.demo;


import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A sample which consumes an {@code IMap} and writes
 * the received items to a Kafka Topic.
 **/
@Component
public class KafkaSink {

    private static final String SINK_TOPIC_NAME = "t5";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(KafkaConfigs.SINK_NAME))
                .writeTo(KafkaSinks.kafka(KafkaConfigs.props(
                                "bootstrap.servers", KafkaConfigs.BOOTSTRAP_SERVERS,
                                "key.serializer", StringSerializer.class.getCanonicalName(),
                                "value.serializer", StringSerializer.class.getCanonicalName()),
                        SINK_TOPIC_NAME));
        return p;
    }

    public void run() throws Exception {
        try {
            Config config = new Config() ;
            config.setClusterName("dev");
            config.getJetConfig().setEnabled(true);
            HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
            JetService jet = hazelcastInstance.getJet();
            IMap<String, Integer> sourceMap = hazelcastInstance.getMap(KafkaConfigs.SINK_NAME);
            KafkaConfigs.printMapEntries(hazelcastInstance);

            Pipeline p = buildPipeline();

            long start = System.nanoTime();
            Job job = jet.newJob(p);

            System.out.println("-----------------------------Consuming Topics--------------------------------");


            int totalMessagesSeen = 0;
            while (true) {
                ConsumerRecord<String, String> records = kafkaTemplate.receive(SINK_TOPIC_NAME, 0, 0);
                System.out.format("Received %d entries in %d milliseconds.%n",
                        totalMessagesSeen, NANOSECONDS.toMillis(System.nanoTime() - start));

                totalMessagesSeen += 1;
                if (totalMessagesSeen == KafkaConfigs.MESSAGE_COUNT_PER_TOPIC) {
                    job.cancel();
                    break;
                }

            }
        } finally {

        }
    }


}
