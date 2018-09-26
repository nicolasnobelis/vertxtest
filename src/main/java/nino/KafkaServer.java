package nino;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class KafkaServer {
    public static final Logger LOGGER = LoggerFactory.getLogger( KafkaServer.class );


    public void start() {
        Properties props = new Properties();
        props.put( "bootstrap.servers", "localhost:9092" );
        props.put( "group.id", "test-consumer" );
        props.put( "enable.auto.commit", "false" );
        props.put( "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
        props.put( "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
        try ( KafkaConsumer<String, String> consumer = new KafkaConsumer<>( props ) ) {
            consumer.subscribe( Collections.singletonList( "test" ), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {

                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    partitions.forEach( topicPartition -> {
                        LOGGER.info( "current position={}", consumer.position( topicPartition ) );
                    } );
                    LOGGER.info( "reset partitions offset" );
                    consumer.seekToBeginning( partitions );
                    partitions.forEach( topicPartition -> {
                        LOGGER.info( "current position={}", consumer.position( topicPartition ) );
                    } );
                }
            } );
            final int minBatchSize = 200;
            List<ConsumerRecord<String, String>> buffer = new ArrayList<>();



            LOGGER.info( "polling server" );
            ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis( 1000 ) );

            for ( ConsumerRecord<String, String> record : records ) {
                buffer.add( record );
                LOGGER.info( "Found record={}", record );
                LOGGER.info( "Found record={}", record.value() );
            }
            if ( buffer.size() >= minBatchSize ) {
                consumer.commitSync();
                buffer.clear();
            }
        }

    }
}
