package br.kafkaLearning.ecommerce.Model

import java.math.BigInteger

data class Order (
    val userId: Int,
    val orderId: String,
    val total: Int
    //Valor do pedido em centavos
)
