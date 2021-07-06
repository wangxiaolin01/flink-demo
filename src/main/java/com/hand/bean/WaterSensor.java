package com.hand.bean;

public class WaterSensor {
    private int id;
    private double temperature;
    private long timestamp;

    public WaterSensor() {
    }

    public WaterSensor(int id, double temperature, long timestamp) {
        this.id = id;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id=" + id +
                ", temperature=" + temperature +
                ", timestamp=" + timestamp +
                '}';
    }
}
