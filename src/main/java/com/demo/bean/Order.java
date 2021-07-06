package com.demo.bean;

public class Order {
    private String userId;
    private String product;
    private int amount;

    public Order() {
    }

    public Order(String userId, String product, int amount) {
        this.userId = userId;
        this.product = product;
        this.amount = amount;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "userId='" + userId + '\'' +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
    }
}
