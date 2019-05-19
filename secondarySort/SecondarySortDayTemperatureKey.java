import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class SecondarySortDayTemperatureKey implements Writable, WritableComparable<SecondarySortDayTemperatureKey> {

	private String day;
	private double temperature;

	public SecondarySortDayTemperatureKey() {

	}

	public SecondarySortDayTemperatureKey(String day, double temperature) {
		this.day = day;
		this.temperature = temperature;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		day = WritableUtils.readString(in);
		temperature = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, day);
		out.writeDouble(temperature);
	}

	@Override
	public int compareTo(SecondarySortDayTemperatureKey o) {
		int result = day.compareTo(o.day);
		if (0 == result) {
			return Double.compare(temperature, o.temperature);
		}
		return result;
	}

	@Override
	public String toString() {
		return (new StringBuilder()).append(day).append(" --> ").append(temperature).toString();
	}

	public String getDay() {
		return day;
	}

	public double getTemparature() {
		return temperature;
	}
}