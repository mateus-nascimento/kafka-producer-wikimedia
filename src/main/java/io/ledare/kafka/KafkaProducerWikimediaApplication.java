package io.ledare.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerWikimediaApplication {

	public static void main(String[] args) throws InterruptedException {

		String bootstrapServers = "127.0.0.1:9092";

		// propriedades necessárias para produzir mensagens
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// criação do producer utilizando as propriedades como parâmetro
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		// handler que será ouvinte dos novos eventos
		EventHandler eventHandler = new WikimediaChangeHandler("topico.comando.teste", producer);
		String url = "https://stream.wikimedia.org/v2/stream/recentchange";
		EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
		EventSource eventSource = builder.build();
		eventSource.start();

		// o event source cria uma nova thread para processamento, abaixo seguramos a thread principal por um período
		TimeUnit.MINUTES.sleep(10);
	}

}
