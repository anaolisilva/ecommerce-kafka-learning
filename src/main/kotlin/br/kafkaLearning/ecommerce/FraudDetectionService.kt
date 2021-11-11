package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration



//Função chama main pra rodar separadamente.
fun main() {
    val consumer = KafkaConsumer<String, String>(KafkaConsumerConfig().configConsumerProperties("FraudDetectionService"))

    //Escuta o tópico definido. Pode escutar de vários tópicos, mas fica muito bagunçado.
    //Se trabalha com microsserviços, é muito provável que escute só um tópico.
    consumer.subscribe(listOf("ecommerce_new_order"))


    //Laço infinito: escuta para sempre
    while(true){
        //Por que criar essaFraudDetectionService variável dentor do laço? Qual a função dela?
        val records = consumer.poll(Duration.ofSeconds(5))
        if (!records.isEmpty) {
            //Consome todas as mensagens, retornando mensagem que se finge de funcionalidade + mensagem consumida
            records.forEach {
                println("Checking for fraud, record: $it")
            }
        }
    }

}


class FraudDetectionService {
}