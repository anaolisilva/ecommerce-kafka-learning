package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

//Vai produzir mensagem do Kafka (Producer). No pique no Kafka mock.
fun main() {
    val producer = KafkaProducer<String, String>(KafkaProducerConfig().properties())

    //Mensagem de exemplo
    val message = "id_pedido, id_usuario, valor_da_compra"
    val record = ProducerRecord("ecommerce_new_order", message, message)

    val email = "Thank you for your purchase. We're processing your order."
    val emailRecord = ProducerRecord("ecommerce_send_email", email, email)

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
}

class NewOrder {

}