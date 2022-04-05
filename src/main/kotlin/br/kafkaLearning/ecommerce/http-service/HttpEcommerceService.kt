package br.kafkaLearning.ecommerce.`http-service`

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder

fun main() {
    //Configura servidor http
    val server = Server(8080)

    val context = ServletContextHandler()
    context.setDefaultContextPath("/")
    context.addServlet(ServletHolder(NewOrderServlet()), "/newOrder")

    server.handler = context
    server.start()
    server.join()

}

class HttpEcommerceService {

}