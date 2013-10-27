/**
 * 
 */
package smartkv.client.tables;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import net.floodlightcontroller.devicemanager.internal.Device;
import net.floodlightcontroller.devicemanager.internal.Entity;
import net.floodlightcontroller.devicemanager.internal.IndexedEntity;
import smartkv.client.DatastoreValue;
import smartkv.client.IKeyValueDataStoreProxy;
import smartkv.client.KeyValueProxy;
import smartkv.client.VersionedDatastoreValue;
import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
//FIXME are you certain that updates are updating the cache and the corresponding timestamp ? 
/**
 * @author fabiim
 *
 */
public class KeyValueTable_<K, V> extends AbstractTable<K,V> implements IKeyValueTable<K,V>{
	
	//Shadow "re-declaration" of datastore. Because we need a more specific type. 
	protected IKeyValueDataStoreProxy datastore;
	protected Serializer<V> valueSerializer;
	protected Serializer<Object> referenceSerializer;
	private ColumnObject<Object> referenceColumnSerializer; 
	
	
	public static <K,V> KeyValueTable_<K,V> getTable(IKeyValueDataStoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		return new KeyValueTable_<K,V>(proxy,tableName,  keySerializer, valueSerializer); 
	}
	
	public static <K,V> KeyValueTable_<K,V> getTable(IKeyValueDataStoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer, String tableReference, Serializer<Object> referenceSerializer) {
		return new KeyValueTable_<K,V>(proxy,tableName,  keySerializer, valueSerializer,tableReference , referenceSerializer, null );
	}
	

	
	/**
	 * If datastoreId is being used... you will be in a world of pain! 
	 * @param stringId
	 * @param string
	 * @param s
	 * @return
	 */
	public static <K,V> KeyValueTable_<K, V> getTableAndCreateProxy(int datastoreId, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer){
		return new KeyValueTable_<K,V>(new KeyValueProxy(datastoreId), tableName, keySerializer, valueSerializer);
	}
	
	public static <K,V> ITable<K, V> getTableWithJavaSerialization(int datastoreId, String tableName){
		return new KeyValueTable_<K,V>(new KeyValueProxy(datastoreId), tableName, UnsafeJavaSerializer.<K>getInstance(),  UnsafeJavaSerializer.<V>getInstance());
	}
	
	public KeyValueTable_(TableBuilder<K,V> b){
		this((IKeyValueDataStoreProxy) b.getOrCreateProxy(), b.getTableName(), b.getKeySerializer(), b.getValueSerializer(), b.getCrossReferenceTable(), b.getCrossReferenceValueSerializer(), b.getCrossReferenceColumnSerializer());
	}
	/**
	 * @param proxy
	 * @param tableName
	 * @param keySerializer
	 * @param valueSerializer
	 */
	protected KeyValueTable_(IKeyValueDataStoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer){
		this(proxy, tableName, keySerializer, valueSerializer, null, null, null); 
	}
	
	/**
	 * @param proxy
	 * @param tableName
	 * @param keySerializer
	 * @param valueSerializer
	 * @param columnObject 
	 */
	protected KeyValueTable_(IKeyValueDataStoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer,String tableReference, Serializer<Object> referenceSerializer, ColumnObject<Object> columnObject) {
		super(proxy, tableName, keySerializer, tableReference);
		this.valueSerializer = valueSerializer; 
		datastore = proxy;
		this.referenceSerializer = referenceSerializer;
		this.referenceColumnSerializer = columnObject; 
	}

	
	/* 
	 * @see bonafide.getRawData()store.tables.Table#remove(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean remove(K key, V value) {
		return datastore.remove(tableName, serializeKey(key), serializeValue(value));
	}
	
	/* (non-Javadoc)
	 * @see bonafide.getRawData()store.tables.Table#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V putIfAbsent(K key, V value){
		VersionedValue<V> v = putIfAbsentWithTimestamp(key, value);
		return v != null ? v.value : null; 
	}
	
	public VersionedValue<V> putIfAbsentWithTimestamp(K key, V value) {
		DatastoreValue result =  datastore.putIfAbsent(tableName, serializeKey(key), serializeValue(value));
		return result != null  ?  new VersionedValue<V>(DatastoreValue.timeStampValues ? ((VersionedDatastoreValue) result).ts : 0, deserializeValue(result)) : null;
	}


	/* (non-Javadoc)
	 * @see bonafide.getRawData()store.tables.Table#remove(java.lang.Object)
	 */
	@Override
	public V remove(K key) {
		DatastoreValue result = datastore.remove(tableName, serializeKey(key));
		return result != null && result.getRawData() != null? deserializeValue(result) : null; 
	}

	/* (non-Javadoc)
	 * @see bonafide.getRawData()store.tables.Table#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V put(K key, V value) {
		VersionedValue<V> v = putAndGetPreviousWithTimestamp(key,value);
		return v != null ? v.value : null;
	}
	
	public VersionedValue<V> putAndGetPreviousWithTimestamp(K key, V value) {
		DatastoreValue result = datastore.put(tableName, serializeKey(key), serializeValue(value));
		return result != null  ?  new VersionedValue<V>(DatastoreValue.timeStampValues ? ((VersionedDatastoreValue) result).ts : 0, deserializeValue(result)) : null;
	}
	
	/* (non-Javadoc)
	 * @see bonafide.getRawData()store.tables.KeyValueTable#insert(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean insert(K key, V value) {
		return datastore.insert(tableName, serializeKey(key), serializeValue(value));
	}


	/* (non-Javadoc)
	 * @see bonafide.getRawData()store.tables.Table#get(java.lang.Object)
	 */
	@Override
	public V get(K key) {
		VersionedValue<V> v = getWithTimeStamp(key);
		return v != null ?  v.value : null;
	}
	
	@Override
	public VersionedValue<V> getWithTimeStamp(K key) {
		if (key == null) return null;
		DatastoreValue result =  datastore.get(tableName, serializeKey(key));
		return result != null ? new VersionedValue<V>( DatastoreValue.timeStampValues ? ((VersionedDatastoreValue) result).ts : 0 ,deserializeValue(result)): null;
	}
	
	@Override
	public Collection<V> values(){
		Collection<DatastoreValue> byte_values = datastore.values(tableName); 
		Collection<V>  values = Lists.newArrayList(); 
		for (DatastoreValue ba : byte_values){
			values.add(deserializeValue(ba));
		}
		return values; 
	}
	
	/* (non-Javadoc)
	 * @see bonafide.getRawData()store.tables.KeyValueTable#getValueByReference(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <V1> V1 getValueByReference(K key) {
		VersionedValue<V1> val = this.getValueByReferenceWithTimestamp(key);
		return val != null ? val.value : null; 
	}
	
	
	@SuppressWarnings("unchecked")
	public <V1> VersionedValue<V1> getValueByReferenceWithTimestamp(K key) {
		DatastoreValue val = datastore.getByReference(tableName, keySerializer.serialize(key));
		return val != null  ?  new VersionedValue<V1>(
							DatastoreValue.timeStampValues ? ((VersionedDatastoreValue) val).ts : 0, (V1) deserializeReferenceValue(val)) 
							: null;
	}
	
	

	/**
	 * @param val
	 * @return
	 */
	private <V1> V1 deserializeReferenceValue(DatastoreValue val) {
		if (this.referenceColumnSerializer != null){
			return (V1) referenceColumnSerializer.fromColumns(serializeMap.deserialize(val.getRawData()));
		}
		else{
			return (V1) referenceSerializer.deserialize(val.getRawData());
		}
	}
	
	/* (non-Javadoc) 
	 * @see bonafide.getRawData()store.tables.KeyValueTable#replace(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean replace(K key, V currentValue, V newValue) {
		return datastore.replace(tableName, keySerializer.serialize(key), serializeValue(currentValue), serializeValue(newValue));
	}
	
	@Override
	public boolean replace(K key, int knownVersion , V newValue) {
		return datastore.replace(tableName, keySerializer.serialize(key), knownVersion, serializeValue(newValue));
	}

	
	/**
	 * @param result
	 * @return
	 */
	protected V deserializeValue(DatastoreValue result) { 
		//FIXME remove null checks from other methods. 
		return result != null ? valueSerializer.deserialize(result.getRawData()) : null;  
	}


	/**
	 * @param value
	 * @return
	 */
	protected byte[] serializeValue(V value) {
		return valueSerializer.serialize(value);
	}
	private Serializer<Map<String,byte[]>>  serializeMap = UnsafeJavaSerializer.<Map<String,byte[]>>getInstance();  
	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#getColumnsByReference(java.lang.Object, java.util.Set)
	 */

	@Override
	public VersionedValue<Object> getColumnsByReference(K key, Set<String> columns) {
		VersionedDatastoreValue v = (VersionedDatastoreValue) datastore.getColumnsByReference(tableName, serializeKey(key), columns);
		return v != null ? new VersionedValue<Object>(v.ts, this.referenceColumnSerializer.fromColumns(serializeMap.deserialize(v.getRawData()))) : null;
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#roundRobin(java.lang.String)
	 */
	@Override
	public Integer roundRobin(String id) {
		return datastore.roundRobin(id); 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#getTwoDevices(net.floodlightcontroller.devicemanager.internal.IndexedEntity, net.floodlightcontroller.devicemanager.internal.IndexedEntity)
	 */
/*	@Override
	public byte[] getTwoDevices(Entity ieSource,
			Entity ieDestination) {
		try {
		ByteArrayOutputStream ob = new ByteArrayOutputStream(); 
			ObjectOutputStream oo = new ObjectOutputStream(ob);
			oo.writeObject(ieSource);
			oo.writeObject(ieDestination);
			return datastore.getTwoDevices(ob.toByteArray());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}*/
	public byte[] getTwoDevices(IndexedEntity ieSource,
			IndexedEntity ieDestination) {
		System.out.println("KeyValue Proxy");
		return datastore.getTwoDevices(ieSource, ieDestination); 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#createDevice(net.floodlightcontroller.devicemanager.internal.Entity)
	 */
	@Override
	public Device createDevice(Entity entity) {
		byte[] b = datastore.createDevice(UnsafeJavaSerializer.getInstance().serialize(entity));
		return b != null ? (Device) UnsafeJavaSerializer.getInstance().deserialize(b) : null;
	}
	
	//FIXME : DataStoreValue and VersionedValue must be the same. No need to create two different objects. 
	//Nullyfy the byte representation of DatastoreValue and add the value after deserialization.
	
	/**
	 * @param deviceKey
	 * @param version
	 * @param entityindex
	 * @param l
	 * @return
	 */
	@Override
	public boolean updateDevice(Long deviceKey, int version, int entityindex,
			long l){
		return datastore.updateDevice(deviceKey, version, entityindex, l); 
	}
}
	

