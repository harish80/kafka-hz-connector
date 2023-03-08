package com.example.demo;




import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.map.IMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static com.example.demo.KafkaConfigs.SINK_NAME;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A sample which consumes two Kafka topics and writes
 * the received items to an {@code IMap}.
 **/
@Component
public class
KafkaSource {


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    public void run() throws Exception {
        try {
            fillTopics();
            Config config = new Config() ;
            config.setClusterName("dev");
            config.getJetConfig().setEnabled(true);
            HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
            JetService jet = hazelcastInstance.getJet();
            IMap<String, Integer> sinkMap = hazelcastInstance.getMap(SINK_NAME);
            KafkaConfigs.printMapEntries(hazelcastInstance);

            Pipeline p = KafkaConfigs.buildPipeline();

            long start = System.nanoTime();
            Job job = jet.newJob(p);

            while (true) {
                int mapSize = sinkMap.size();
                System.out.format("Received %d entries in %d milliseconds.%n",
                        mapSize, NANOSECONDS.toMillis(System.nanoTime() - start));
                if (mapSize == KafkaConfigs.MESSAGE_COUNT_PER_TOPIC * 2) {
                    job.cancel();
                    break;
                }
            }
            KafkaConfigs.printMapEntries(hazelcastInstance);

        } finally {
            //Jet.shutdownAll();
        }
    }


    // Creates 2 topics (t1, t2) with different partition counts (32, 64) and fills them with items
    private void fillTopics() {
        System.out.println("Filling Topics");

            for (int i = 1; i <= KafkaConfigs.MESSAGE_COUNT_PER_TOPIC; i++) {
                kafkaTemplate.send("t3", "t3-" + i, Integer.toString(i));
                kafkaTemplate.send("t4", "t4-" + i , Integer.toString(i));

            }
            System.out.println("Published " + KafkaConfigs.MESSAGE_COUNT_PER_TOPIC + " messages to topic t3");
            System.out.println("Published " + KafkaConfigs.MESSAGE_COUNT_PER_TOPIC + " messages to topic t4");
        }



}
