package br.kafkaLearning.ecommerce.Service

import br.kafkaLearning.ecommerce.common.Kafka.KafkaProducerConfig
import br.kafkaLearning.ecommerce.Model.Email
import br.kafkaLearning.ecommerce.Model.Order
import java.util.UUID
import kotlin.random.Random

//Vai produzir mensagem do Kafka (Producer). No pique do Kafka mock.
fun main() {
    val orderProducer = KafkaProducerConfig<Order>()
    val emailProducer = KafkaProducerConfig<Email>()

    var i = 0
    while (i < 5) {

        //Simulando id de usuário como int pra testar a divisão entre partições.
        val userId: Int = Random.nextInt(0, 10)

        val orderId: String = UUID.randomUUID().toString()
        val total: Int = Random.nextInt(1, 5000)


        //Mensagem de exemplo (pré-refactor)
        //val messageOrder = "id_user: $userId, id_pedido, valor_da_compra"
        val email: Email = Email("Purchase","Thank you for your purchase. We are processing your order.")

        //Cria um objeto Order, com os dados que eu preciso (evento)
        val order = Order(userId, orderId, total)


        //Pré-refatoramento: definia uma mensagem para cada um (abstraído abaixo)
        //val record = ProducerRecord("ecommerce_new_order", key.toString(), message)
        //val emailRecord = ProducerRecord("ecommerce_send_email", key.toString(), email)

        //Manda mensagem de novo pedido
        orderProducer.send("ecommerce_new_order", userId.toString(), order)
        //Manda mensagem de novo e-mail
        emailProducer.send("ecommerce_send_email", userId.toString(), email)
        i++
    }
}

class NewOrder {
}
