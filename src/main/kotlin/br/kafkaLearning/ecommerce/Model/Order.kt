package br.kafkaLearning.ecommerce.Model

data class Order (
    val userId: Int,
    val orderId: String,
    val total: Int,
    //Valor do pedido em centavos
    val email: String
)
