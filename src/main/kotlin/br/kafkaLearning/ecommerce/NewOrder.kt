package br.kafkaLearning.ecommerce

import kotlin.random.Random

//Vai produzir mensagem do Kafka (Producer). No pique do Kafka mock.
fun main() {
    val producer = KafkaProducerConfig()

    var i = 0
    while (i < 5) {
        //Simulando id de usuário como int pra testar a divisão entre partições.
        val key: Int = Random.nextInt(0, 10)

        //Mensagem de exemplo
        val messageOrder = "id_user: $key, id_pedido, valor_da_compra"
        val email = "Thank you for your purchase. We're processing your order."

        //Pré-refatoramento: definia uma mensagem para cada um (abstraído abaixo)
        //val record = ProducerRecord("ecommerce_new_order", key.toString(), message)
        //val emailRecord = ProducerRecord("ecommerce_send_email", key.toString(), email)

        //Manda mensagem de novo pedido
        producer.send("ecommerce_new_order", key.toString(), messageOrder)
        //Manda mensagem de novo e-mail
        producer.send("ecommerce_send_email", key.toString(), email)
        i++
    }
}

class NewOrder {
}
