package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.regex.Pattern

//Função chama main pra rodar separadamente.
fun main() {

    val consumer = KafkaConsumer<String, String>(KafkaConsumerConfig().configConsumerProperties("EmailService"))

    //Escuta qualquer tópico que comece com ecommerce (regex).
    consumer.subscribe(Pattern.compile("ecommerce.*"))

    //Laço infinito: escuta para sempre
    while(true){
        val records = consumer.poll(Duration.ofSeconds(5))
        if (!records.isEmpty) {
            //Consome todas as mensagens, logando pontos específicos das mensagens.
            records.forEach {
                println("--------------------- LOG: ${it.topic()} ---------------------")
                println("key: " + it.key())
                println("value: ${it.value()}")
                println("partition: ${it.partition()}")
                println("offset: ${it.offset()}")
            }
        }
    }

}


class LogService {
}