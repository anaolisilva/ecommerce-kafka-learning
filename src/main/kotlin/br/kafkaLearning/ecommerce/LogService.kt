package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.ConsumerRecord

//Função chama main pra rodar separadamente.
fun main() {

    val logService = LogService()

    val kafkaService = KafkaConsumerConfig(logService.javaClass.name,"ecommerce_new_order", logService.callConsume())

    val consumer = KafkaConsumer<String, String>(kafkaService.configConsumerProperties(logService.javaClass.name))

    //Escuta qualquer tópico que comece com ecommerce (regex).
    consumer.subscribe(Pattern.compile("ecommerce.*"))

    kafkaService.run()

}


class LogService : ConsumerFunction {

    //Simula serviço de envio de LOG fraudes.
    override fun consume(record: ConsumerRecord<String, String>) {
        println("--------------------- LOG: ${record.topic()} ---------------------")
        println("key: " + record.key())
        println("value: ${record.value()}")
        println("partition: ${record.partition()}")
        println("offset: ${record.offset()}")
    }

    fun callConsume(): (ConsumerRecord<String, String>) -> Unit {
        return {
            consume(it)
        }
    }
}