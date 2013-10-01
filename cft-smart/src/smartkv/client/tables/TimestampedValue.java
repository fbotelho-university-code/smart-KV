/**
 * 
 */
package smartkv.client.tables;

/**
 * @author fabiim
 * Also known as a tuple!
 */
public class TimestampedValue<V> {
	
	public  final int ts; 
	public final V value;
	
	public TimestampedValue(int ts, V value) {
		super();
		this.ts = ts;
		this.value = value;
	} 
}
