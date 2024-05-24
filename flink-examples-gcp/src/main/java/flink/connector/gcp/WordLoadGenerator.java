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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/** Creates load from a set list of words. */
public class WordLoadGenerator implements FlatMapFunction<Long, String> {
    private int load;
    private byte[] wordStr;

    public WordLoadGenerator(int l) {
        load = l;
        try {
            StorageOptions options = StorageOptions.newBuilder()
                    .setProjectId("managed-flink-shared-dev").build();

            Storage storage = options.getService();
            Blob blob = storage.get("gmf-samples", "words.txt");

            // Read the InputStream into a byte array
            wordStr = blob.getContent(null);

        } catch (Exception e) {
            System.out.println("Could not read words.txt file, using the head of the file instead");
            wordStr =
                    ("Release, Ornamental, Cosmetic, Cement, Mud, Cleave, Zephyr, "
                                    + "Unusual, Receive, Atmosphere, Corrupt, Taboo, Cousin, Robotic, "
                                    + "Tramp, Heavyset, Current, Whisper, Alert, Approval, Forsake, Wind, "
                                    + "Consult, Women, Pitch, Easier, Shirk, Fighter, Disastrous, Basis, "
                                    + "Vanish, Freezing, Soar, Old-fashioned, Blankly, Closed, Parade, "
                                    + "Prophetic, Sponge, Moldy,  Ransom, Identical, Tremor, Bearskin, Shout, Hook, Blackheart, Travel, Quit, Faulty, Cheerful, Dope, Cast, Admiral, Pen, Phenomenal, Acquire, Bird, Gentle, Selective, Bone, Scissors, Knowing, Begin, Guidebook, Stupid, Insurance, Conflict, Lace, Rule, Righteous, Warrior, Hundred, Coercion, Wiry, Aroma, Absorb, Homicide, Curve, Hammer, Fireman, Jump, Fake, Communication, Glance, Gushing, Ferment, Propellant, Enter, Kangaroo, Harplike, Scenic, Economics, Ignorant, Dirt, Elimination, Dozen, Dear, Mew, Vomit, Eruption, Reception, Arch, Security, Dynamite, Cynical, Murmur, Machine, Educated, Eternity, Manufacturer, Audience, Engine, Granite, Icicle, Loose, Guilty, Driving, Afraid, Crowd, Sugar, Harmonious, Airplane, Rerun, Fabulous, Balloon, Giddy, Smut, Bronze, Curfew, Harmonic, Death, Blimp, Calculate, Decade, Satisfy, Virus, Banana, Manage, Grand, Appetite, Celebrate, Beeswax, Recording, Dime, Speed, Round, Dignitary, Contemn, Serum, Pet, Absent, Confidence, Crispy, Hangman, Collide, Guilt, Cannibalism, Berserk, Agreement, Implant, Sable, Sass, Bludgeon, Construe, Indulge, Grin, Crazy, Boring, Spider, Animatronic, Stealthy, Discipline, Slimy, Quince, Blooper, Plucky, Civilization, Morning, Pandemic, Front, Wood, Industry, Makeshift, Zesty, World, Fugitive, Reflective, Horses, Baby, Tall, Adopter, Puffy, Squeeze, Baseball, Minipill, Dazzling, Aftertaste, Omniscient, Purring, Abducted, Retain, Essence, Link, Piano, Courage, Identify, Purpose, Whole, Appearance, Chain, Shrill, Alcohol, Sadistic, Erase, Entropy, Flight, Endorse, Crisp, Digest, Bed, Brass, Rat, Gainsay, Stir, Drop, Spiders, Blankness, Diplomat, Republic, Circuitry, Sphere, Honest, Augmentation, Cemetery, Recite, Bloke, Rustic, Hairless, Closing, Belligerent, Devastation, Religion, Inner, Pony, Left, Imitate, Falling, Domineering, Cigarette, Benevolent, Crafty, Affair, Freak, Alleged, Mice, Wish, Council, Animal, Contamination, Pick, Abnormality, Entity, Carnival, Youth, Press, Jade, Torpedo, Tend, Analyst, Uptown, Call, Far-flung,")
                            .getBytes();
            e.printStackTrace();
        }
    }

    public byte[] makeBytesOfSize(int sizeBytes) {
        // since the bytes will be converted with UTF-16 encoding
        sizeBytes = sizeBytes / 2;
        byte[] result = new byte[sizeBytes];
        int current = 0;
        int remaining = sizeBytes;
        while (remaining > 0) {
            int len = Math.min(wordStr.length, remaining);
            System.arraycopy(wordStr, 0, result, current, len);
            current += len;
            remaining -= len;
        }
        return result;
    }

    public String randomWordStringOfSize(int sizeBytes) {
        return new String(makeBytesOfSize(sizeBytes));
    }

    @Override
    public void flatMap(Long value, Collector<String> out) {
        out.collect(randomWordStringOfSize(load));
    }
}
