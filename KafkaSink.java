package com.example.demo;

/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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