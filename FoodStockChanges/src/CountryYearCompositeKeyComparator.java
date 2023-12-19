import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * A custom comparator that implements RawComparator.
 * It used for comparing CountryYearCompositeKey objects for sorting the partitions.
 */
public class CountryYearCompositeKeyComparator implements RawComparator<CountryYearCompositeKey>{

	// Compare two CountryYearCompositeKey objects
	@Override
	public int compare(CountryYearCompositeKey key1, CountryYearCompositeKey key2) {
		
		// Compare by country
		int result = key1.getCountry().compareTo(key2.getCountry());
		if (result != 0) {
			return result;
		}
		
		// If countries are same, then compare by year
		return Integer.compare(key1.getYear(), key2.getYear());
	}
	
	// Default comparisons at byte array provided by Hadoop Apache
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
	}
}
