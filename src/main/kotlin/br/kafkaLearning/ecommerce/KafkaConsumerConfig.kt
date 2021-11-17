package br.kafkaLearning.ecommerce

import java.time.Duration
import java.util.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

class KafkaConsumerConfig(
    groupId: String,
    topic: String,
    val consume: ((consumerRecord: ConsumerRecord<String, String>) -> Unit)
) {
    private val consumer = KafkaConsumer<String, String>(configConsumerProperties(groupId))

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

    fun run() {
        //Laço infinito: escuta para sempre
        while(true){
            val records = consumer.poll(Duration.ofSeconds(5))
            if (!records.isEmpty) {
                println("Encontrei ${records.count()} registros. Processando...")
                //Consome todas as mensagens, retornando mensagem que se finge de funcionalidade + mensagem consumida
                records.forEach {
                    consume(it)
                }
            }
        }
    }


}