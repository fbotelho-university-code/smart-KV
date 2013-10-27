/**
 * 
 */
package smartkv.client;

import java.util.Set;

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
	/**
	 * @param serializeKey
	 * @param columns
	 * @return
	 */
	public DatastoreValue getColumns(String tableName, byte[] serializeKey,  Set<String> columns);
	/**
	 * @param columnName
	 * @param serializeKey
	 * @param currentValue
	 * @param columnName2
	 * @param serializeColumn
	 * @return 
	 */
	public boolean replace(String columnName, byte[] serializeKey,
			int currentValue, String columnName2, byte[] serializeColumn);
	/**
	 * @param deviceKey
	 * @param version
	 * @param entityindex
	 * @param l
	 * @param serialize
	 * @return
	 */
	public Object updateDevice(Long deviceKey, int version, int entityindex,
			long l, byte[] serialize);
	
	
}
