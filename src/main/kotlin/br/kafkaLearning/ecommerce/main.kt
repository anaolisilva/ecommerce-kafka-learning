package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

//Vai produzir mensagem do Kafka (Producer). No pique no Kafka mock.
fun main() {
    val producer = KafkaProducer<String, String>(NewOrder().properties())

    //Mensagem de exemplo
    val message = "id_pedido, id_usuario, valor_da_compra"

    val record = ProducerRecord("ecommerce_new_order", message, message)

    //Produtor envia mensagem (record). Send não é síncrono, ele devolve um future.
    // Se você der um get(), ele espera retorno.
    try {
        producer.send(record).get()
    } catch (e: Exception) {
        e.printStackTrace();
    }
}