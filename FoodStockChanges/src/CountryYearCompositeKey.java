import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custom WritableComparable key used in MapReduce.
 * It combines country and year as a composite key for MapReduce tasks.
 */
public class CountryYearCompositeKey implements WritableComparable<CountryYearCompositeKey>{

	// Store country and year as parts of the key
	private String country;
	private int year;
	
	// Default constructor required for Hadoop's serialization and deserialization
	public CountryYearCompositeKey() {
	}
	
	// Constructor with parameter
	public CountryYearCompositeKey(String country, int year) {
		this.country = country;
		this.year = year;
	}
	
	// Method to write data to DataOutput for serialization
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, country);
		out.writeInt(year);
	}
	
	// Method to read data from DataInput for deserialization
	@Override
	public void readFields(DataInput in) throws IOException {
		country = WritableUtils.readString(in);
		year = in.readInt();
	}
	
	// Method to compare two CountryYearCompositeKey objects
	@Override
	public int compareTo(CountryYearCompositeKey other) {
		// First compare by country
		int countryComparison = country.compareTo(other.country);
		if (countryComparison != 0) {
			return countryComparison;
		}
		
		// If countries are same, then compare by year
		return Integer.compare(year, other.year);
	}
	
	// Getters and setters for country and year
	public String getCountry() {
		return this.country;
	}
	
	public int getYear() {
		return this.year;
	}
	
	public void setCountry(String country) {
		this.country = country;
	}
	
	public void setYear(int year) {
		this.year = year;
	}
	
	// Overridden toString method for displaying results in readable way
	@Override
	public String toString() {
		return country + "\t" + year;
	}
}
