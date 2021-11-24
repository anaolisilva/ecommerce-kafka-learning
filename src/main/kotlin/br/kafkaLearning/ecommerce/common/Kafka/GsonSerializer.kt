package br.kafkaLearning.ecommerce.common.Kafka

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Serializer

class GsonSerializer<T> : Serializer<T>{
    //Para funcionar como Serializador para o Kafka, temos que implementar a classe Serializer, do próprio Kafka.

    val gson: Gson = GsonBuilder().create()

    override fun serialize(s: String?, t: T): ByteArray {
        return gson.toJson(t).toByteArray()
    }
}
