/**
 * 
 */
package smartkv.client;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Set;

import net.floodlightcontroller.devicemanager.internal.IndexedEntity;
import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;
import smartkv.server.DataStoreVersion;
import smartkv.server.RequestType;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

/**
 * @author fabiim
 *
 */

public class KeyValueProxy extends AbstractDatastoreProxy implements IKeyValueDataStoreProxy{
	//FIXME set version of the data store. 
	
	private Serializer<Collection<byte[]>> serializer = UnsafeJavaSerializer.getInstance();  
	
	public KeyValueProxy(int cid){
		super(cid); 
	}
	
	/* 
	 * @see bonafide.datastore.DatastoreProxy#get(java.lang.String, byte[])
	 */
	@Override
	public DatastoreValue get(String tableName, byte[] key) {
		RequestType type = RequestType.GET_VALUE_IN_TABLE;

		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key);
		return invokeRequest(type, request);
	}
	
	@Override
	public DatastoreValue getByReference(String tableName, byte[] key) {
		RequestType type = RequestType.GET_VALUE_IN_TABLE_BY_REFERENCE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key);
		return invokeRequest(type, request);
	}
	
	@Override
	public DatastoreValue getColumnsByReference(String tableName, byte[] key, Set<String> columns) {
		RequestType type = RequestType.GET_COLUMNS_REFERENCE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key, 
				getBytes(columns));
		return invokeRequest(type, request);
	}
	
	/**
	 * @param columnName
	 * @return
	 */
	protected byte[] getBytes(Set<String> columnName) {
		byte[] finalByte = new byte[0];
		
		for (String s : columnName ){
			finalByte = Bytes.concat(finalByte, getBytes(s));
		}
		return finalByte; 
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#put(java.lang.String, byte[], byte[])
	 */
	@Override
	public DatastoreValue put(String tableName, byte[] key, byte[] value) {
		RequestType type = RequestType.PUT_VALUE_IN_TABLE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key, 
				value);
		return invokeRequest(type, request); 
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#insert(java.lang.String, byte[], byte[])
	 */
	@Override
	public boolean insert(String tableName, byte[] key, byte[] value) {
		RequestType type = RequestType.INSERT_VALUE_IN_TABLE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key, 
				value);
		byte[] result = invokeRequestWithRawReturn(type, request);
		return result != null; 
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#remove(java.lang.String, byte[])
	 */
	@Override
	public DatastoreValue remove(String tableName, byte[] key) {
		RequestType type = RequestType.REMOVE_VALUE_FROM_TABLE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key);
		DatastoreValue result = invokeRequest(type, request);
		return result; 
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#replace(java.lang.String, byte[], byte[], byte[])
	 */
	@Override
	public boolean replace(String tableName, byte[] key, byte[] oldValue,
			byte[] newValue) {
		RequestType type = RequestType.ATOMIC_REPLACE_VALUE_IN_TABLE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key,
				getBytes(oldValue.length),
				oldValue, 
				newValue);
		byte[] result = invokeRequestWithRawReturn(type, request);
		return result != null; 
	}

	
	/* (non-Javadoc)
	 * @see smartkv.client.KeyValueDatastoreProxy#replace(java.lang.String, byte[], int, byte[])
	 */
	@Override
	public boolean replace(String tableName, byte[] key,
			int knownVersion, byte[] serialize2) {
		RequestType type =RequestType.REPLACE_WITH_TIMESTAMP; 
		byte[] request = concatArrays(type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key,
				Ints.toByteArray(knownVersion), 
				serialize2); 
		byte[] result = invokeRequestWithRawReturn(type,request);
		return result != null; 
	}
	
	@Override
	public boolean remove(String tableName, byte[] key, byte[] expectedValue) {
		RequestType type = RequestType.ATOMIC_REMOVE_IF_VALUE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key,
				expectedValue
				); 
		byte[] result = invokeRequestWithRawReturn(type, request);
		return result != null;
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#putIfAbsent(java.lang.String, byte[], byte[])
	 */
	@Override
	public DatastoreValue putIfAbsent(String tableName, byte[] key, byte[] value) {
		RequestType type = RequestType.ATOMIC_PUT_IF_ABSENT;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key,
				value
				); 
		DatastoreValue result = invokeRequest(type, request);
		return result; 
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.AbstractDatastoreProxy#version()
	 */
	@Override
	protected DataStoreVersion version() {
		
		return DataStoreVersion.KEY_VALUE;
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.KeyValueDatastoreProxy#values()
	 */
	@Override
	//FIXME
	public Collection<DatastoreValue> values(String tableName) {
		RequestType type = RequestType.VALUES;
		byte[] request = concatArrays(type.byteArrayOrdinal, getBytes(tableName));
		byte[] result = invokeRequestWithRawReturn(type, request);
		Collection<byte[]> vals =  result != null ?  serializer.deserialize(result) : null;
		if (vals == null) return null; 
		Collection<DatastoreValue> finalValues = Lists.newArrayList(); 
		for (byte[] v : vals){
			finalValues.add(DatastoreValue.createValue(v)); 
		}
		return finalValues;
	}

	/* (non-Javadoc)
	 * @see smartkv.client.IKeyValueDataStoreProxy#getTwoDevices(byte[], byte[])
	 */
	@Override
	public byte[] getTwoDevices(byte[] cenas) {
		RequestType type = RequestType.DM_TWO_DEVICES;
		byte[] req = concatArrays(
				type.byteArrayOrdinal,
				cenas
				); 
		byte[] res = invokeRequestWithRawReturn(type, req);
		return res; 
	}

	public final byte[] ONE = Ints.toByteArray(1);
	public final byte[] TWO = Ints.toByteArray(2);
	/* (non-Javadoc)
	 * @see smartkv.client.IKeyValueDataStoreProxy#createDevice(byte[])
	 */
	
	@Override
	public byte[] createDevice(byte[] serialize) {
		RequestType type = RequestType.DM_CREATE_DEVICE; 
		byte[] req = concatArrays(type.byteArrayOrdinal, 
				serialize); 
		return invokeRequestWithRawReturn(type, req); 
	}
	
	/* (non-Javadoc)
	 * @see smartkv.client.IKeyValueDataStoreProxy#createDevice(java.lang.Long, int, int, long)
	 */
	@Override
	public boolean updateDevice(Long deviceKey, int version, int entityindex,
			long l) {
		RequestType type = RequestType.DM_UPDATE_DEVICE; 
		byte[] req = concatArrays(type.byteArrayOrdinal, type.byteArrayOrdinal, 
				getBytes(deviceKey), 
				getBytes(version), 
				getBytes(entityindex), 
				getBytes(l));
		byte[] b =invokeRequestWithRawReturn(type,req);
		return b != null ; 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.IKeyValueDataStoreProxy#getTwoDevices(net.floodlightcontroller.devicemanager.internal.IndexedEntity, net.floodlightcontroller.devicemanager.internal.IndexedEntity)
	 */
	@Override
	public byte[] getTwoDevices(IndexedEntity ieSource,
			IndexedEntity ieDestination) {
		RequestType type = RequestType.DM_TWO_DEVICES;
		byte[] req = ieDestination != null ? concatArrays(
				type.byteArrayOrdinal,
				getBytes(ieSource.getEntity().getMacAddress()), 
				getBytes(ieSource.getEntity().getVlanOrZero()),
				getBytes(ieDestination.getEntity().getMacAddress()), 
				getBytes(ieDestination.getEntity().getVlanOrZero())
				) :
				concatArrays(
						type.byteArrayOrdinal,
						getBytes(ieSource.getEntity().getMacAddress()), 
						getBytes(ieSource.getEntity().getVlan())
						);  
		byte[] res = invokeRequestWithRawReturn(type, req);
		return res;
	}
}
