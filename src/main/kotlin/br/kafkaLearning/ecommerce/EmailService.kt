package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

//Função chama main pra rodar separadamente.
fun main() {

    val emailService = EmailService()

    val kafkaService = KafkaConsumerConfig(emailService.javaClass.name,"ecommerce_send_email", emailService.callConsume())

    val consumer = KafkaConsumer<String, String>(kafkaService.configConsumerProperties("EmailService"))

    consumer.subscribe(listOf("ecommerce_send_email"))

    kafkaService.run()

}

class EmailService : ConsumerFunction {

    //Simula serviço de envio de e-mail.
    override fun consume(record: ConsumerRecord<String, String>) {
        println("-------------------SENDING E-MAIL-------------------")
        println("topic: ${record.topic()}")
        println("key: ${record.key()}")
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