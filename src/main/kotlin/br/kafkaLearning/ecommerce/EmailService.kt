package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

class EmailService {

    //Função chama main pra rodar separadamente.
    fun main() {
        val consumer = KafkaConsumer<String, String>(KafkaConsumerConfig().configConsumerProperties("EmailService"))

        consumer.subscribe(listOf("ecommerce_send_email"))

        //Laço infinito: escuta para sempre
        while(true){
            val records = consumer.poll(Duration.ofSeconds(5))
            if (!records.isEmpty) {
                //Consome todas as mensagens, retornando mensagem que se finge de funcionalidade + mensagem consumida
                records.forEach {
                    println("Sending e-mail, message: $it")
                }
            }
        }

    }
}