package kafka_management;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.errors.TopicExistsException;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

/**
 * Kafka admin class to create and delete topics.
 */
public class KafkaUtils {
	public static final String BOOTSTRAP_SERVERS = "172.17.143.173:9092,172.17.143.174:9092,172.17.143.175:9092";
	public static final String ZOOKEEPER_SERVERS = "172.17.143.130:2181,172.17.143.134:2181,172.17.143.135:2181";
	private static final int DEFAULT_PARTITIONS = 100;
	private static final int DEFAULT_REPLICATION = 3;
	private static ZkClient zkclient;
	private static ZkUtils zkutils;
	
	public KafkaUtils() {
		zkclient = ZkUtils.createZkClient(ZOOKEEPER_SERVERS, 10000, 8000);
		zkutils = new ZkUtils(zkclient, new ZkConnection(ZOOKEEPER_SERVERS), false);
	}
	
	/**
	 * Deletes a topic from Kafka
	 * @param topic - topic to delete
	 */
	public void deleteTopic(String topic) {
		AdminUtils.deleteTopic(zkutils, topic);
		System.out.println("Deleted topic "+topic);
	}
	
	/** Checks if a Kafka topic already exists.
	 * @param topic
	 * @return true if this topic exists.
	 */
	boolean topicExists(String topic) {
		return AdminUtils.topicExists(zkutils, topic);
	}

	/**
	 * Creates a kafka topic with default parameters
	 * @param topic - kafka topic to store incoming messages into
	 */
	public void createTopic(String topic) {
		this.createTopic(topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION);
	}
	
	/**
	 * Creates a kafka topic.
	 * @param topic - KAFKA topic to store messages into 
	 * @param nPartitions - Number of partitions to assign to the topic
	 * @param replicationFactor - Replication factor of the topic
	 */
	void createTopic(String topic, int nPartitions, int replicationFactor) {
		//createTopic(zkutils, topic, #partitions, replicationFactor, Properties, RackAwareMode)
		try {
			AdminUtils.createTopic(zkutils, topic, nPartitions, replicationFactor, new Properties(), AdminUtils.createTopic$default$6());
			System.out.println("Created topic "+topic+".");
		} catch(TopicExistsException ex){
			//Topic already exists, just print a message and swallow the exception
			System.err.println("This topic already exists.");
		}
	}
	
}
