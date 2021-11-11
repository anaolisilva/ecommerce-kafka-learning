package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class KafkaConsumerConfig {

    fun configConsumerProperties(groupId: String) : Properties {
    //Configura propriedades do consumidor.
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    //ConfiguraDeserializador da chave do consumer. Pode ser customizado se for o caso, ter um deserializer próprio a partir
    //do que recebe o consumidor.
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

        //Auto-commita apenas uma mensagem por vez com a config dessa propriedade.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")

        return properties
    }

}