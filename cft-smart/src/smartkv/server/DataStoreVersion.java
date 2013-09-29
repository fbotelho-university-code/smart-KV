/**
 * 
 */
package smartkv.server;

import com.google.common.primitives.Ints;

/**
 * @author fabiim
 *
 */
public  enum DataStoreVersion {
	KEY_VALUE, 
	COLUMN_KEY_VALUE;
	
	public final byte[] byteArrayOrdinal; 
	
	private DataStoreVersion(){
		byteArrayOrdinal = Ints.toByteArray(ordinal()); 
	}

}