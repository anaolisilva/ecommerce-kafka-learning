package br.kafkaLearning.ecommerce.Service

import br.kafkaLearning.ecommerce.Model.Order
import br.kafkaLearning.ecommerce.common.Kafka.ConsumerFunction
import br.kafkaLearning.ecommerce.common.Kafka.KafkaConsumerConfig
import br.kafkaLearning.ecommerce.common.Kafka.KafkaProducerConfig
import kotlin.random.Random
import org.apache.kafka.clients.consumer.ConsumerRecord

fun main() {

    val registerUserService = RegisterUserService()
    val kafkaService = KafkaConsumerConfig<Order>(
        registerUserService::class.java.name,
        "ecommerce_new_order",
        registerUserService.callConsume(),
        Order::class.java,
        mapOf()
    )

    kafkaService.run()
}

class RegisterUserService : ConsumerFunction<Order> {

    //Simula serviço de envio de checar fraudes.
    override fun consume(record: ConsumerRecord<String, Order>) {
        println("-------------------Checking for new user-------------------")
        println("key: ${record.key()}")
        println("value: ${record.value()}")
        println("-----------------------------------------------------------")
        println()

    }

    fun callConsume(): (ConsumerRecord<String, Order>) -> Unit {
        return {
            consume(it)
        }
    }
}