package br.kafkaLearning.ecommerce.`http-service`

import br.kafkaLearning.ecommerce.Model.Email
import br.kafkaLearning.ecommerce.Model.Order
import br.kafkaLearning.ecommerce.common.Kafka.KafkaProducerConfig
import jakarta.servlet.ServletConfig
import jakarta.servlet.http.HttpServlet
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import java.util.UUID
import kotlin.random.Random

class NewOrderServlet : HttpServlet() {
    val orderProducer = KafkaProducerConfig<Order>()
    val emailProducer = KafkaProducerConfig<Email>()

    override fun init(config: ServletConfig?) {
        super.init(config)
    }

    override fun doGet(req: HttpServletRequest?, resp: HttpServletResponse?) {
        val userId: Int = Random.nextInt(0, 1000)
        val orderId: String = UUID.randomUUID().toString()

        //Simulando parâmetros passados pela requisição, total da compra e e-mail do usuário.
        val total: Int = req!!.getParameter("amount").toInt()
        val userEmail: String = req.getParameter("email")

        val emailBody = Email("Purchase","Thank you for your purchase. We are processing your order.")

        val order = Order(userId, orderId, total, userEmail)
        orderProducer.send("ecommerce_new_order", userId.toString(), order)
        emailProducer.send("ecommerce_send_email", userId.toString(), emailBody)

        resp!!.status = HttpServletResponse.SC_OK
        resp.writer.println("New order sent")
    }
}