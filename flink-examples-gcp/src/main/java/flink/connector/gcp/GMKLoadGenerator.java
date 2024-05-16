/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.connector.gcp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Pipeline code for generating load to GCS. */
public class GMKLoadGenerator {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String brokers = parameters.get("brokers", "localhost:9092");
        String gmkUsername = parameters.get("gmk-username");
        String kafkaTopic = parameters.get("kafka-topic", "my-topic");
        double rate = parameters.getDouble("rate", 1_000_000_000);
        Long maxRecords = parameters.getLong("max-records", 1_000_000_000L);

        env.getConfig().setGlobalJobParameters(parameters);

        DataGeneratorSource<String> generatorSource =
                new DataGeneratorSource<>(
                        n ->
                                "Release, Ornamental, Cosmetic, Cement, Mud, Cleave, Zephyr, Unusual, Receive, Atmosphere, Corrupt, Taboo, Cousin, Robotic, Tramp, Heavyset, Current, Whisper, Alert, Approval, Forsake, Wind, Consult, Women, Pitch, Easier, Shirk, Fighter, Disastrous, Basis, Vanish, Freezing, Soar, Old-fashioned, Blankly, Closed, Parade, Prophetic, Sponge, Moldy, Ransom, Identical, Tremor, Bearskin, Shout, Hook, Blackheart, Travel, Quit, Faulty, Cheerful, Dope, Cast, Admiral, Pen, Phenomenal, Acquire, Bird, Gentle, Selective, Bone, Scissors, Knowing, Begin, Guidebook, Stupid, Insurance, Conflict, Lace, Rule, Righteous, Warrior, Hundred, Coercion, Wiry, Aroma, Absorb, Homicide, Curve, Hammer, Fireman, Jump, Fake, Communication, Glance, Gushing, Ferment, Propellant, Enter, Kangaroo, Harplike, Scenic, Economics, Ignorant, Dirt, Elimination, Dozen, Dear, Mew, Vomit, Eruption, Reception, Arch, Security, Dynamite, Cynical, Murmur, Machine, Educated, Eternity, Manufacturer, Audience, Engine, Granite, Icicle, Loose, Guilty, Driving, Afraid, Crowd, Sugar, Harmonious, Airplane, Rerun, Fabulous, Balloon, Giddy, Smut, Bronze, Curfew, Harmonic, Death, Blimp, Calculate, Decade, Satisfy, Virus, Banana, Manage, Grand, Appetite, Celebrate, Beeswax, Recording, Dime, Speed, Round, Dignitary, Contemn, Serum, Pet, Absent, Confidence, Crispy, Hangman, Collide, Guilt, Cannibalism, Berserk, Agreement, Implant, Sable, Sass, Bludgeon, Construe, Indulge, Grin, Crazy, Boring, Spider, Animatronic, Stealthy, Discipline, Slimy, Quince, Blooper, Plucky, Civilization, Morning, Pandemic, Front, Wood, Industry, Makeshift, Zesty, World, Fugitive, Reflective, Horses, Baby, Tall, Adopter, Puffy, Squeeze, Baseball, Minipill, Dazzling, Aftertaste, Omniscient, Purring, Abducted, Retain, Essence, Link, Piano, Courage, Identify, Purpose, Whole, Appearance, Chain, Shrill, Alcohol, Sadistic, Erase, Entropy, Flight, Endorse, Crisp, Digest, Bed, Brass, Rat, Gainsay, Stir, Drop, Spiders, Blankness, Diplomat, Republic, Circuitry, Sphere, Honest, Augmentation, Cemetery, Recite, Bloke, Rustic, Hairless, Closing, Belligerent, Devastation, Religion, Inner, Pony, Left, Imitate, Falling, Domineering, Cigarette, Benevolent, Crafty, Affair, Freak, Alleged, Mice, Wish, Council, Animal, Contamination, Pick, Abnormality, Entity, Carnival, Youth, Press, Jade, Torpedo, Tend, Analyst, Uptown, Call, Far-flung, Useless, Sideways, Drone, Frequent, Hatch, Absurd, Participate, Magenta, Oafish, Werewolf, Roll, Texture, Glove, Dim, Ambiguous, Curvy, Frightening, Companion, Uproot, Ajar",
                        maxRecords,
                        RateLimiterStrategy.perSecond(rate),
                        Types.STRING);
        DataStreamSource<String> streamSource =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(brokers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(kafkaTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setProperty("security.protocol", "SASL_SSL")
                        .setProperty("sasl.mechanism", "PLAIN")
                        .setProperty(
                                "sasl.jaas.config",
                                String.format(
                                        "org.apache.kafka.common.security.plain.PlainLoginModule required"
                                                + " username=\'"
                                                + gmkUsername
                                                + "\'"
                                                + " password=\'"
                                                + System.getenv("GMK_PASSWORD") + "\';"))
                        .build();

        streamSource.sinkTo(sink);

        // Execute
        env.execute();
    }
}
