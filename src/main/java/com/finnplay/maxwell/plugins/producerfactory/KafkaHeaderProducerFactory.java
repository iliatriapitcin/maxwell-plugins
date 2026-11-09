package com.finnplay.maxwell.plugins.producerfactory;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.ProducerFactory;

/**
 * Custom {@link ProducerFactory} example that creates a new {@link KafkaHeaderProducer}. To register your custom producer, set the {@code custom_producer.factory} property to your custom producer
 * factory's fully qualified class name.
 */
public class KafkaHeaderProducerFactory implements ProducerFactory {
    @Override
    public synchronized AbstractProducer createProducer(MaxwellContext context) {

        return new KafkaHeaderProducer(context, context.getConfig().getKafkaProperties(), context.getConfig().kafkaTopic);
    }
}
