/**
 * 
 */
package smartkv.client;

import java.util.Set;
import java.util.TreeMap;

import com.google.common.primitives.Bytes;

import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;
import smartkv.server.DataStoreVersion;
import smartkv.server.RequestType;

/**
 * @author fabiim
 *
 */


public class ColumnProxy extends KeyValueProxy implements IKeyValueColumnDatastoreProxy{
	/**
	 * @param cid
	 */
	public ColumnProxy(int cid) {
		super(cid);
		// TODO Auto-generated constructor stub
	}

	Serializer<TreeMap<String,byte[]>> serializer = UnsafeJavaSerializer.getInstance();
	
	
	/* 
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#setColumn(java.lang.String, byte[], java.lang.String, byte[])
	 */
	@Override
	public boolean setColumn(String tableName, byte[] key, String columnName,
			byte[] value) {
		RequestType type = RequestType.SET_COLUMN; 
		byte[] request = concatArrays(type.byteArrayOrdinal, getBytes(tableName), getBytes(key.length), key,  getBytes(columnName) , value);
		byte[] result = invokeRequestWithRawReturn(type, request);
		return result != null; 
	}

	/* 
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#getColumn(java.lang.String, byte[], java.lang.String)
	 */
	@Override
	public byte[] getColumn(String tableName, byte[] key, String columnName) {
		RequestType type = RequestType.GET_COLUMN; 
		byte[] request = concatArrays(type.byteArrayOrdinal, getBytes(tableName), getBytes(key.length), key, getBytes(columnName));
		byte[] result = invokeRequestWithRawReturn(type, request);
		return result; 
	}
	
	public DatastoreValue getColumns(String tableName, byte[] key, Set<String> columnName){
		RequestType type = RequestType.GET_COLUMNS;
		byte[] request = concatArrays(type.byteArrayOrdinal, getBytes(tableName), getBytes(key.length), key, getBytes(columnName));
		return invokeRequest(type, request); 
	}
	
	

	@Override
	public byte[] getColumnByReference(String tableName, byte[] key, String columnName) {
		RequestType type = RequestType.GET_COLUMN_BY_REFERENCE; 
		byte[] request = concatArrays(type.byteArrayOrdinal, getBytes(tableName), getBytes(key.length), key, getBytes(columnName));
		byte[] result = invokeRequestWithRawReturn(type, request);
		return result; 
	}

	
	@Override
	protected DataStoreVersion version(){
		return DataStoreVersion.COLUMN_KEY_VALUE; 
	}

	
} 
