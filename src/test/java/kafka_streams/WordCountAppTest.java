package kafka_streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class WordCountAppTest {

    TopologyTestDriver testDriver;
    StringSerializer stringSerializer = new StringSerializer();
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    public void setUpTopologyTestDriver(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        WordCountApp wordCountApp = new WordCountApp();

        testDriver = new TopologyTestDriver(wordCountApp.createTopology(), config);
        inputTopic = testDriver.createInputTopic("word-count-input",stringSerializer,stringSerializer);
        outputTopic = testDriver.createOutputTopic("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    public void closeTestDriver(){
        testDriver.close();
    }

    public void pushNewInputRecord(String value){
        inputTopic.pipeInput(null, value);
    }

    public KeyValue<String, Long> readOutput(){
        return outputTopic.readKeyValue();
    }

    @Test
    public void makeSureCountsAreCorrect(){
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        KeyValue<String, Long> output;
        output = readOutput();
        assertTrue(Objects.equals(output.key, "testing")&&output.value==1L);
        output = readOutput();
        assertTrue(Objects.equals(output.key, "kafka")&&output.value==1L);
        output = readOutput();
        assertTrue(Objects.equals(output.key, "streams")&&output.value==1L);
        assertTrue(outputTopic.isEmpty());

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        output = readOutput();
        assertTrue(Objects.equals(output.key, "testing")&&output.value==2L);
        output = readOutput();
        assertTrue(Objects.equals(output.key, "kafka")&&output.value==2L);
        output = readOutput();
        assertTrue(Objects.equals(output.key, "again")&&output.value==1L);
    }

    @Test
    public void makeSureWordsBecomeLowerCase(){
        String uppercaseString = "KafKa kAfKa KAFKA";
        pushNewInputRecord(uppercaseString);
        KeyValue<String, Long> output;
        output = readOutput();
        assertTrue(Objects.equals(output.key, "kafka")&&output.value==1L);
        output = readOutput();
        assertTrue(Objects.equals(output.key, "kafka")&&output.value==2L);
        output = readOutput();
        assertTrue(Objects.equals(output.key, "kafka")&&output.value==3L);
    }
}
