package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaProducerConfig {

    val producer: KafkaProducer<String, String> = KafkaProducer(properties())

    fun properties(): Properties {
        //Configura propriedades de configuração do produtor
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        return properties
    }

    fun send(topic: String, key: String, message: String) {
        val record = ProducerRecord(topic, key, message)

        //Produtor envia mensagem (record). Send não é síncrono, ele devolve um future.
        // Se você der um get(), ele espera retorno.
        try {
            producer.send(record).get()
            println("Enviando: $record")
        } catch (e: Exception) {
            e.printStackTrace()
        }


    }
}