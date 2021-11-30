package br.kafkaLearning.ecommerce.Service


import br.kafkaLearning.ecommerce.common.Kafka.ConsumerFunction
import br.kafkaLearning.ecommerce.common.Kafka.KafkaConsumerConfig
import br.kafkaLearning.ecommerce.Model.Order
import br.kafkaLearning.ecommerce.common.Kafka.KafkaProducerConfig
import kotlin.random.Random
import org.apache.kafka.clients.consumer.ConsumerRecord


//Função chama main pra rodar separadamente.
fun main() {

    val fraudDetectionService = FraudDetectionService()
    val kafkaService = KafkaConsumerConfig<Order>(
        fraudDetectionService::class.java.name,
        "ecommerce_new_order",
        fraudDetectionService.callConsume(),
        Order::class.java,
        mapOf()
    )

    //Código abaixo usado antes do refatoramento.
    //val consumer = KafkaConsumer<String, String>(kafkaService.configConsumerProperties(fraudDetectionService.javaClass.name))
    //Escuta o tópico definido. Pode escutar de vários tópicos, mas fica muito bagunçado.
    //Se trabalha com microsserviços, é muito provável que escute só um tópico.
    //consumer.subscribe(listOf("ecommerce_new_order"))

    kafkaService.run()

}

class FraudDetectionService : ConsumerFunction<Order> {

    //Simula serviço de envio de checar fraudes.
    override fun consume(record: ConsumerRecord<String, Order>) {
        println("-------------------Checking for fraud-------------------")
        println("topic: ${record.topic()}")
        println("key: ${record.key()}")
        println("value: ${record.value()}")
        println("partition: ${record.partition()}")
        println("offset: ${record.offset()}")
        println("--------------------------------------------------------")
        println()

        val order = record.value()
        val orderProducer = KafkaProducerConfig<Order>()

        if (isFraud()) {
            println("Order is a fraud. Cancelling operation.")
            orderProducer.send("ecommerce_order_rejected", order.userId.toString(), order)
        } else {
            println("Approved: $order")
            orderProducer.send("ecommerce_order_approved", order.userId.toString(), order)
        }

    }

    private fun isFraud(): Boolean {
        //Simula chance de fraude de 25%
        val fraudDetector = Random.nextInt(1, 100)
        return (fraudDetector % 2 == 1 && fraudDetector < 50)
    }

    fun callConsume(): (ConsumerRecord<String, Order>) -> Unit {
        return {
            consume(it)
        }
    }
}
