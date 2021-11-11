package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import kotlin.random.Random

//Vai produzir mensagem do Kafka (Producer). No pique do Kafka mock.
fun main() {
    val producer = KafkaProducer<String, String>(KafkaProducerConfig().properties())

    var i = 0
    while (i < 5) {
        //Simulando id de usuário como int pra testar a divisão entre partições.
        val key: Int = Random.nextInt(0, 10)

        //Mensagem de exemplo
        val message = "id_user: $key, id_pedido, valor_da_compra"
        val record = ProducerRecord("ecommerce_new_order", key.toString(), message)

        val email = "Thank you for your purchase. We're processing your order."
        val emailRecord = ProducerRecord("ecommerce_send_email", key.toString(), email)

        //Produtor envia mensagem (record). Send não é síncrono, ele devolve um future.
        // Se você der um get(), ele espera retorno.

        try {
            producer.send(record).get()
            producer.send(emailRecord).get()
            println(record)
            println(emailRecord)
        } catch (e: Exception) {
            e.printStackTrace()
        }
        i++
    }

}

class NewOrder {

}