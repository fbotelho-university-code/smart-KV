/**
 * 
 */
package smartkv.client;

import java.util.Collection;
import java.util.TreeMap;

import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;

import smartkv.server.DataStoreVersion;
import smartkv.server.RequestType;

/**
 * @author fabiim
 *
 */


public class ColumnProxy extends KeyValueProxy implements KeyValueColumnDatastoreProxy{
	private Serializer<Collection<TreeMap<String,byte[]>>> valuesSerializer = UnsafeJavaSerializer.getInstance();
	
	/**
	 * @param cid
	 */
	public ColumnProxy(int cid) {
		super(cid);
		// TODO Auto-generated constructor stub
	}

	Serializer<TreeMap<String,byte[]>> serializer = UnsafeJavaSerializer.getInstance();
	
	/**
	 * 
	 * @see smartkv.client.KeyValueColumnDatastoreProxy#put(java.lang.String, byte[], java.util.Map)
	 * TODO: The Map must be an instance of Serializable. 
	 */
	@Override
	public TreeMap<String, byte[]> put(String tableName, byte[] key,
			 TreeMap<String, byte[]> value) {
		DatastoreValue  result = super.put(tableName, key, serializer.serialize(value));
		return (TreeMap<String, byte[]>) ((result != null)  ? serializer.deserialize(result.getRawData()): null); 
	}
	
	/* 
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#get(java.lang.String, java.util.Map)
	 * TODO: when fixed, change method name to get
	 */
	@SuppressWarnings("unchecked")
	@Override
	public TreeMap<String, byte[]> getValue(String tableName, byte[] key) {
		DatastoreValue  result = super.get(tableName, key); 
		if (result != null){
			TreeMap<String,byte[]>  map = (TreeMap<String, byte[]>)  serializer.deserialize(result.getRawData());
			return map; 
		}
		return null; 
	}

	/* (non-Javadoc)
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#getValueByReference(java.lang.String, byte[])
	 */
	@Override
	public TreeMap<String, byte[]> getValueByReference(String tableName,
			byte[] key) {
		DatastoreValue  result = super.getByReference(tableName, key);
		if (result != null){
			TreeMap<String,byte[]>  map = (TreeMap<String, byte[]>)  serializer.deserialize(result.getRawData());
			return map; 
		}
		return null;
	}

	/* 
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#insert(java.lang.String, byte[], java.util.Map)
	 */
	@Override
	public boolean insert(String tableName, byte[] key,
			TreeMap<String, byte[]> value) {
		return super.insert(tableName, key, serializer.serialize(value)); 
	}

	/* 
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#remove(java.lang.String, java.util.Map)
	 * TODO: when fixed, rename method to remove
	 */
	@Override
	public TreeMap<String, byte[]> removeValue(String tableName,byte[] key) {
		DatastoreValue  result = super.remove(tableName, key); 
		return (TreeMap<String, byte[]>) ((result != null)  ? serializer.deserialize(result.getRawData()): null); 

	}

	/* 
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#replace(java.lang.String, byte[], java.util.Map, java.util.Map)
	 */
	@Override
	public boolean replace(String tableName, byte[] key,
			TreeMap<String, byte[]> oldValue, TreeMap<String, byte[]> newValue) {
		return super.replace(tableName, key, serializer.serialize(oldValue),  serializer.serialize(newValue));
	}

	/* 
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#remove(java.lang.String, byte[], java.util.Map)
	 */
	@Override
	public boolean remove(String tableName, byte[] key,
			TreeMap<String, byte[]> expectedValue) {
		return super.remove(tableName, key, serializer.serialize(expectedValue));
	}

	/* 
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#putIfAbsent(java.lang.String, byte[], java.util.Map)
	 */
	@Override
	public TreeMap<String, byte[]> putIfAbsent(String tableName, byte[] key,
			TreeMap<String, byte[]> value) {
		DatastoreValue result =  super.putIfAbsent(tableName, key, serializer.serialize(value));
		return result != null? serializer.deserialize(result.getRawData()) : null; 
	}
	
	
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

	
	
	@Override
	public byte[] getColumnByReference(String tableName, byte[] key, String columnName) {
		RequestType type = RequestType.GET_COLUMN_BY_REFERENCE; 
		byte[] request = concatArrays(type.byteArrayOrdinal, getBytes(tableName), getBytes(key.length), key, getBytes(columnName));
		byte[] result = invokeRequestWithRawReturn(type, request);
		return result; 
	}
	/* (non-Javadoc)
	 * @see bonafide.getRawData()store.KeyValueColumnDatastoreProxy#valueS(java.lang.String)
	 */
	//FIXME
	@Override
	public Collection<TreeMap<String, byte[]>> valueS(String tableName) {
		RequestType type  = RequestType.VALUES; 
		byte[] request = concatArrays(type.byteArrayOrdinal, getBytes(tableName));
		byte[] result = invokeRequestWithRawReturn(type,request); 
		return result != null ? valuesSerializer.deserialize(result) : null; 
	}
	
	//FIXME: Comment and explain. 
	//TODO:  Solve this problem. Get a wrap around key in the interfaces , something... 
	@Override
	@Deprecated
	public DatastoreValue get(String tableName, byte[] key) {
		throw new  UnsupportedOperationException("The use of this interface in this class is not permitted. Use KeyValueProxy instead!");
	}


	@Override
	@Deprecated
	public DatastoreValue put(String tableName, byte[] key, byte[] value) {
		throw new  UnsupportedOperationException("The use of this interface in this class is not permitted. Use KeyValueProxy instead!");
	}


	@Override
	@Deprecated
	public boolean insert(String tableName, byte[] key, byte[] value) {
		throw new  UnsupportedOperationException("The use of this interface in this class is not permitted. Use KeyValueProxy instead!");
		// TODO Auto-generated method stub
	}


	@Override
	@Deprecated
	public DatastoreValue remove(String tableName, byte[] key) {
		throw new  UnsupportedOperationException("The use of this interface in this class is not permitted. Use KeyValueProxy instead!");
	}


	@Override
	@Deprecated
	public boolean replace(String tableName, byte[] key, byte[] oldValue,
			byte[] newValue) {
		throw new  UnsupportedOperationException("The use of this interface in this class is not permitted. Use KeyValueProxy instead!");
	}


	@Override
	@Deprecated
	public boolean remove(String tableName, byte[] key, byte[] expectedValue) {
		throw new  UnsupportedOperationException("The use of this interface in this class is not permitted. Use KeyValueProxy instead!");
	}


	@Override
	public DatastoreValue putIfAbsent(String tableName, byte[] key, byte[] value) {
		throw new  UnsupportedOperationException("The use of this interface in this class is not permitted. Use KeyValueProxy instead!");
	}

	@Override
	@Deprecated 
	public Collection<DatastoreValue> values(String tableName){
		throw new UnsupportedOperationException("The use of this interface in this class is not permitted. Use valuesS"); 
	}
	@Override
	protected DataStoreVersion version(){
		return DataStoreVersion.COLUMN_KEY_VALUE; 
	}
	
} 
