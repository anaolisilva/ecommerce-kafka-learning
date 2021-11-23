package br.kafkaLearning.ecommerce

import org.apache.kafka.clients.consumer.ConsumerRecord

interface ConsumerFunction<T> {
    fun consume(record: ConsumerRecord<String, T>)
}
