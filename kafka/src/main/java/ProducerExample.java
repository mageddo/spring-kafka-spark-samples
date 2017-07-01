import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by elvis on 30/06/17.
 */
public class ProducerExample {

	public ProducerExample(String kafka, String topic) throws InterruptedException {
		final KafkaProducer<String, Object> producer = new KafkaProducer<>(producerConfigs(kafka));

		while (true) {

			final ProducerRecord<String, Object> record = new ProducerRecord<>(
				topic, String.valueOf(new Random().nextInt(3) + 1), Integer.toHexString(new Random().nextInt(100))
			);
			producer.send(record);
			producer.flush();
			System.out.println("posted");
			Thread.sleep(2000);
//			producer.close();

		}

	}

	public static Map<String, Object> producerConfigs(String kafkaServer) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put("request.required.acks", "1");
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return props;
	}

}
