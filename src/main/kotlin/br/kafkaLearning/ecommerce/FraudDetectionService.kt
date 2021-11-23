package br.kafkaLearning.ecommerce


import org.apache.kafka.clients.consumer.ConsumerRecord


//Função chama main pra rodar separadamente.
fun main() {

    val fraudDetectionService = FraudDetectionService()
    val kafkaService = KafkaConsumerConfig<Order>(fraudDetectionService.javaClass.name,"ecommerce_new_order", fraudDetectionService.callConsume())

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
    }

    fun callConsume(): (ConsumerRecord<String, Order>) -> Unit {
        return {
            consume(it)
        }
    }
}
