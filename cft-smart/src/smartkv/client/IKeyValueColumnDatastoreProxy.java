/**
 * 
 */
package smartkv.client;

import java.util.Collection;
import java.util.TreeMap;

/**
 * @author fabiim
 * 
 */

public interface IKeyValueColumnDatastoreProxy extends IKeyValueDataStoreProxy{
	
		
	//FIXME : doc. What happens when setColumn when key is inexistent? 
	public boolean setColumn(String tableName, byte[] key, String columnName , byte[] value); 
	public byte[] getColumn(String tableName, byte[] key, String columnName);
	
	/**
	 * @param tableName
	 * @param key
	 * @param columnName
	 * @return
	 */
	byte[] getColumnByReference(String tableName, byte[] key, String columnName);
	
	
}
