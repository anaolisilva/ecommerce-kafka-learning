package br.kafkaLearning.ecommerce.Service

import br.kafkaLearning.ecommerce.common.Kafka.ConsumerFunction
import br.kafkaLearning.ecommerce.common.Kafka.KafkaConsumerConfig
import br.kafkaLearning.ecommerce.Model.Email
import org.apache.kafka.clients.consumer.ConsumerRecord

//Função chama main pra rodar separadamente.
fun main() {
    val emailService = EmailService()
    val kafkaService = KafkaConsumerConfig<Email>(
        emailService::class.java.name,
        "ecommerce_send_email",
        emailService.callConsume(),
        Email::class.java,
        mapOf()
    )

    kafkaService.run()
}

class EmailService : ConsumerFunction<Email> {

    //Simula serviço de envio de e-mail.
    override fun consume(record: ConsumerRecord<String, Email>) {
        println("-------------------SENDING E-MAIL-------------------")
        println("topic: ${record.topic()}")
        println("key: ${record.key()}")
        println("value: ${record.value()}")
        println("partition: ${record.partition()}")
        println("offset: ${record.offset()}")
        println("----------------------------------------------------")
        println()
    }

    fun callConsume(): (ConsumerRecord<String, Email>) -> Unit {
        return {
            consume(it)
        }
    }
}