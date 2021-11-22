package br.kafkaLearning.ecommerce

import java.math.BigInteger

class Order (
    val userId: Int,
    val orderId: String,
    val total: Int
    //Valor do pedido em centavos
)
