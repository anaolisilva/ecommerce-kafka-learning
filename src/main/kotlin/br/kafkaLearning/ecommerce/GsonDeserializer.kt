package br.kafkaLearning.ecommerce

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.lang.RuntimeException
import org.apache.kafka.common.serialization.Deserializer

class GsonDeserializer<T> : Deserializer<T> {

    val gson : Gson = GsonBuilder().create()
    lateinit var type : Class<T>

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        //Talvez a safe call não seja a melhor maneira de chamar essa config.
        val typeName: String = configs?.get(Companion.TYPE_CONFIG).toString()

        try {
            type = Class.forName(typeName) as Class<T>
        } catch (e: ClassNotFoundException) {
            throw RuntimeException("Type for deserialization does not exist.", e)
        }
    }

    override fun deserialize(p0: String, b: ByteArray): T {
        return gson.fromJson(String(b), type)
    }

    companion object {
        const val TYPE_CONFIG : String = "br.kafkaLearning.ecommerce"
    }
}
