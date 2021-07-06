package com.demo.bean;

public class SensorReading {
    private  String id;
    private  Double temperature;
    private  Long  timestamp;

    public SensorReading() {
    }

    public SensorReading(String id, Double temperature, Long timestamp) {
        this.id = id;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", temperature=" + temperature +
                ", timestamp=" + timestamp +
                '}';
    }
}
