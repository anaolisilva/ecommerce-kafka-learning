package br.kafkaLearning.ecommerce.Service

import br.kafkaLearning.ecommerce.Model.Order
import br.kafkaLearning.ecommerce.Model.User
import br.kafkaLearning.ecommerce.common.Kafka.ConsumerFunction
import br.kafkaLearning.ecommerce.common.Kafka.KafkaConsumerConfig
import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import org.apache.kafka.clients.consumer.ConsumerRecord

fun main() {

    val registerUserService = RegisterUserService()
    val kafkaService = KafkaConsumerConfig<Order>(
        registerUserService::class.java.name,
        "ecommerce_new_order",
        registerUserService.callConsume(),
        Order::class.java,
        mapOf()
    )

    kafkaService.run()
}

class RegisterUserService : ConsumerFunction<Order>  {

    val connection: Connection = DriverManager.getConnection("jdbc:sqlite:users_database.db")

//    Cria conexão com SQLite e cria tabela de usuários.
    constructor(
    urlDataBase: String = "jdbc:sqlite:users_database.db",
    connection: Connection = DriverManager.getConnection(urlDataBase)
    ) {
        connection.createStatement().execute("create table Users (" +
                "id varchar(50) primary key," +
                "email varchar(200))")
    }

    lateinit var users: User

    //Simula serviço de envio de checar fraudes.
    override fun consume(record: ConsumerRecord<String, Order>) {
        println("-------------------Checking for new user-------------------")
        println("key: ${record.key()}")
        println("value: ${record.value()}")
        println("-----------------------------------------------------------")
        println()
        val order = record.value()

        if (isNewUser(order.getEmail())) {
            insertNew(order.getEmail())
        }

    }

    fun callConsume(): (ConsumerRecord<String, Order>) -> Unit {
        return {
            consume(it)
        }
    }

    fun isNewUser(email: String) : Boolean {
        val existsUser = connection.prepareStatement("select id from Users where email = ? limit 1")
        existsUser.setString(1, email)
        val result = existsUser.executeQuery()
        return !result.next()
    }

    fun insertNew(email: String) {
        var statement = connection.prepareStatement("insert into Users (uuid, email) " +
                "values (?,?)");
        statement.setString(1, UUID.randomUUID().toString());
        statement.setString(2, email);
        statement.execute();
        println("User " + email + " inserted.");
    }
}