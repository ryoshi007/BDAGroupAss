import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custom Writable class used in MapReduce.
 * It stores stock values for different commodities, including wheat, maize, barley
 * and sunflower oil.
 */
public class StockWritable implements Writable {
	
	// Store stock values for each commodity as parts of value
	private int wheatStock;
	private int maizeStock;
	private int barleyStock;
	private int sunflowerOilStock;
	
	// Default constructor required for Hadoop's serialization and deserialization
	public StockWritable() {
	}
	
	// Constructor with parameters to set the stock values
	public StockWritable(int wheatStock, int maizeStock, int barleyStock, int sunflowerOilStock) {
		this.wheatStock = wheatStock;
		this.maizeStock = maizeStock;
		this.barleyStock = barleyStock;
		this.sunflowerOilStock = sunflowerOilStock;
	}
	
	// Method to write data to DataOutput for serialization
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(wheatStock);
		out.writeInt(maizeStock);
		out.writeInt(barleyStock);
		out.writeInt(sunflowerOilStock);
	}
	
	// Method to read data from DataInput for deserialization
	@Override
	public void readFields(DataInput in) throws IOException {
		wheatStock = in.readInt();
		maizeStock = in.readInt();
		barleyStock = in.readInt();
		sunflowerOilStock = in.readInt();
	}	
	
	// Getters and setters for each commodity's stock value
	public int getWheatStock() {
		return wheatStock;
	}

	public int getMaizeStock() {
		return maizeStock;
	}
	
	public int getBarleyStock() {
		return barleyStock;
	}
	
	public int getSunflowerOilStock() {
		return sunflowerOilStock;
	}
	
	public void setWheatStock(int wheatStock) {
		this.wheatStock = wheatStock;
	}
	
	public void setMaizeStock(int maizeStock) {
		this.maizeStock = maizeStock;
	}
	
	public void setBarleyStock(int barleyStock) {
		this.barleyStock = barleyStock;
	}
	
	public void setSunflowerOilStock(int sunflowerOilStock) {
		this.sunflowerOilStock = sunflowerOilStock;
	}
	
	// Overridden toString method for displaying results in readable way
	@Override
	public String toString() {
		return wheatStock + "\t" + maizeStock + "\t" + barleyStock + "\t" + sunflowerOilStock;
	}
}
