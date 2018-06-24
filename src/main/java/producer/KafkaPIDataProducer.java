package producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import com.osisoft.pidevclub.piwebapi.models.PITimedValue;

/**
 * Kafka producer for the extracted PI Web API data.
 */
public class KafkaPIDataProducer {
	
	/* KAFKA Server Addresses: */
	// Address1:Port1, Address2:Port2,...
	public static final String BOOTSTRAP_SERVERS = "172.17.143.173:9092,172.17.143.174:9092,172.17.143.175:9092";
	public static final String ZOOKEEPER_SERVERS = "172.17.143.130:2181,172.17.143.134:2181,172.17.143.135:2181";

	private static KafkaProducer<String, PITimedValue> producer;
	private Properties kafkaProperties;

	/**
	 * TODO: Acceptable value for linger.ms
	 * 
	 * Constructor for the producer.
	 * Sets up the Kafka Producer with the list of brokers and serializer classes.
	 */
	public KafkaPIDataProducer() {
		kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"producer.PITimedValueSerializer");
		
		// linger.ms is a delay to allow the buffer to fill up before sending messages to Kafka
		// 0 means messages are sent as they're ready, but batching can still occur under heavy load.
		// Some batching is advised in orderto increase performance.
		kafkaProperties.put("linger.ms", 200);
		producer = new KafkaProducer<String, PITimedValue>(kafkaProperties);
	}
	
	/**
	 * Publishes a message to the Kafka brokers, using SERVERNAME:TAG as the key, and a PITimedValue as the message.
	 * Using the tag as a key allows us to keep the messages ordered, as every message with the same tag will be published to the same partition.
	 * The server name is also used to differentiate between possible tags with the same name in different servers.
	 * 
	 * @param topic - kafka topic
	 * @param key - the name of the PI Point, to be used as the message tag
	 * @param value - a PI Timed Value from the stream set.
	 */
	public void send(String topic, String key, PITimedValue value) {
		producer.send(new ProducerRecord<String, PITimedValue>(topic, key, value));
	}

	// Closes the Kafka producer.
	public void close() {
		producer.close();
	}

	// Flushes the Kafka producer. 
	// Blocks until all the messages are sent to the broker.
	public void flush() {
		producer.flush();
	}	
}
