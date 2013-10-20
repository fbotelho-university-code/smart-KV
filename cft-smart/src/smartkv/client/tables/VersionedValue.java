/**
 * 
 */
package smartkv.client.tables;

/**
 * @author fabiim
 * Also known as a tuple!
 */
public class VersionedValue<V> {
	
	public  final int ts; 
	public final V value;
	
	public VersionedValue(int ts, V value) {
		super();
		this.ts = ts;
		this.value = value;
	}
	
	/**
	 * @return
	 */
	public int version() {
		return ts;
	} 
	
	public V value(){
		return value; 
	}

	@Override
	public String toString() {
		return "TimestampedValue [ts=" + ts + ", value=" + value + "]";
	}
	
	
}
