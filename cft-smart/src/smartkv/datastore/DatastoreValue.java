/**
 * 
 */
package smartkv.datastore;

import com.google.common.primitives.Ints;


/**
 * @author fabiim
 *
 */
public class DatastoreValue {
	public  final byte[]  data;
	public  final int ts;
	
	public byte[] getRawData() {
		return data;
	}
	
	public int getTs() {
		return ts;
	}
	
	public DatastoreValue(byte[] data, short ts) {
		super();
		this.data = data;
		this.ts = ts;
	}
	
	public DatastoreValue(byte[] reply){
		//FIXME - use ByteBuffer to avoid creating a new array? 
		ts = Ints.fromByteArray(reply);
		data = new byte[reply.length-2]; 
		System.arraycopy(reply, 2, data, 0, data.length-2);
	}
}
