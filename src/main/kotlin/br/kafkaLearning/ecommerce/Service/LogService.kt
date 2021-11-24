package br.kafkaLearning.ecommerce.Service

import br.kafkaLearning.ecommerce.common.Kafka.ConsumerFunction
import br.kafkaLearning.ecommerce.common.Kafka.KafkaConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

//Função chama main pra rodar separadamente.
fun main() {
    val logService = LogService()
    val kafkaService = KafkaConsumerConfig(logService.javaClass.name,
        Pattern.compile("ecommerce.*"),
        logService.callConsume(),
        String::class.java,
        mapOf(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name))

    //Escuta qualquer tópico que comece com ecommerce (regex). Pré-refactoing.
    //consumer.subscribe(Pattern.compile("ecommerce.*"))

    kafkaService.run()
}


class LogService : ConsumerFunction<String> {

    //Simula serviço de envio de LOG fraudes.
    override fun consume(record: ConsumerRecord<String, String>) {
        println("--------------------- LOG: ${record.topic()} ---------------------")
        println("key (userId): ${record.key()}")
        println("value: ${record.value()}")
        println("partition: ${record.partition()}")
        println("offset: ${record.offset()}")
        println("------------------------------------------------------------------")
        println()
    }

    fun callConsume(): (ConsumerRecord<String, String>) -> Unit {
        return {
            consume(it)
        }
    }
}