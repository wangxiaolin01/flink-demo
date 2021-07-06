package demo2.watermark.bean;

public class WaterSensor {
    private String id;
    private long timestamp;
    private double temperature;

    public WaterSensor(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
