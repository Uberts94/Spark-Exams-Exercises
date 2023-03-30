package it.polito.bigdata.spark.example;

public class Readings {
	private float value;
	private int readings;
	
	public Readings(int readings, float value) {
		this.readings = readings;
		this.value = value;
	}
	
	public float getValue() {
		return value;
	}
	
	public void setValue(float value) {
		this.value = value;
	}
	
	public int getReadings() {
		return readings;
	}
	
	public void setReadings(int readings) {
		this.readings = readings;
	}
}
