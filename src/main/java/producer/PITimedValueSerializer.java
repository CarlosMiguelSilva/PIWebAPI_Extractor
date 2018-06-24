package producer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.osisoft.pidevclub.piwebapi.models.PITimedValue;

public class PITimedValueSerializer implements Serializer<PITimedValue> {
	
	static final ObjectMapper mapper = new ObjectMapper();
	
	public void close() {}

	public void configure(Map<String, ?> configs, boolean isKey) {}

	public byte[] serialize(String topic, PITimedValue message) {
		try {
			return mapper.writeValueAsBytes(message);
		} catch (final JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

}
