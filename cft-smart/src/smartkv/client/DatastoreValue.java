/**
 * 
 */
package smartkv.client;

import java.util.Arrays;

import com.google.common.primitives.Ints;


/**
 * @author fabiim
 *
 */
public class DatastoreValue {
	private final  byte[]  data;
	public final static boolean timeStampValues = true;  
	//XXX - so all datastore values must be greater than 1 byte! :)
	public static DatastoreValue createValue(byte[] data){
		if (timeStampValues) return createTimestampedValue(data); 
		if (data != null){
			return new DatastoreValue(data);
		}
		return null; 
	}
	
	public static DatastoreValue createTimestampedValue(byte[] data){
		if (data != null){
			if (data.length > 4){
				//It is a timestamped value 
				return new VersionedDatastoreValue(data); 
			}
		}
		return null; 
	}
	
	public byte[] getRawData() {
		return data;
	}
	
	public DatastoreValue(byte[] reply){
		this.data = reply; 
	}
	public DatastoreValue(byte[] reply, int start, int finish){
		int len = finish-start +1;
		//FIXME - this is kind of really ugly. Null response and "truth" values (1 element byte array) are passing to here... 
		if (len >0 ) {
			this.data = new byte[len ];
			System.arraycopy(reply, start, data, 0, len );
		}
		else data = reply; 
	}
}

