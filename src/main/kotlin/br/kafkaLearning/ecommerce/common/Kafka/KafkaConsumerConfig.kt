package br.kafkaLearning.ecommerce.common.Kafka

import java.time.Duration
import java.util.*
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

class KafkaConsumerConfig<T> (
    groupId: String,
    val consume: (consumerRecord: ConsumerRecord<String, T>) -> Unit,
    type: Class<T>,
    extraProperties: Map<String, String>
) {
    private val consumer = KafkaConsumer<String, T>(configConsumerProperties(groupId, type, extraProperties))

    //Não gosto muito da abordagem de colocar em tudo, mas é o que ele fez no vídeo.
    constructor(
        groupId: String,
        topic: String,
        consume: (consumerRecord: ConsumerRecord<String, T>) -> Unit,
        type: Class<T>,
        extraProperties: Map<String, String>
    ) : this(groupId,  consume, type, extraProperties) {
        consumer.subscribe(listOf(topic))
    }

    constructor(
        groupId: String,
        topic: Pattern,
        consume: (consumerRecord: ConsumerRecord<String, T>) -> Unit,
        type: Class<T>,
        extraProperties: Map<String, String>
    ) : this(groupId, consume, type, extraProperties) {
        consumer.subscribe(topic)
    }

    fun configConsumerProperties(groupId: String, type: Class<T>, extraProperties: Map<String, String>) : Properties {
        //Configura propriedades do consumidor.
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        //ConfiguraDeserializador da chave do consumer. Pode ser customizado se for o caso, ter um deserializer próprio a partir
        //do que recebe o consumidor.
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer::class.java.name)

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())

        //Define consumo de mensagens para desde o início.
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        //Auto-commita apenas uma mensagem por vez com a config dessa propriedade.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")

        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.name)

        //Override das properties definidas no caso de passar propriedades extra.
        properties.putAll(extraProperties)

        return properties
    }

    fun run() {

        //Laço infinito: escuta para sempre
        while(true){
            val records = consumer.poll(Duration.ofSeconds(5))
            if (!records.isEmpty) {
                println("Encontrei ${records.count()} registro(s). Processando...")
                //Consome todas as mensagens, retornando mensagem que se finge de funcionalidade + mensagem consumida
                records.forEach {
                    consume(it)
                }
            }
        }
    }
}
