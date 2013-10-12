/**
 * 
 */
package smartkv.client.tables;

import java.util.Collection;

import smartkv.client.DatastoreValue;
import smartkv.client.IKeyValueDataStoreProxy;
import smartkv.client.KeyValueProxy;
import smartkv.client.TimestampedDatastoreValue;
import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;

import com.google.common.collect.Lists;

/**
 * @author fabiim
 *
 */
public class KeyValueTable_<K, V> extends AbstractTable<K,V> implements IKeyValueTable<K,V>{
	
	//Shadow "re-declaration" of datastore. Because we need a more specific type. 
	protected IKeyValueDataStoreProxy datastore;
	protected Serializer<V> valueSerializer;
	protected Serializer<Object> referenceSerializer; 
	
	public static <K,V> KeyValueTable_<K,V> getTable(IKeyValueDataStoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		return new KeyValueTable_<K,V>(proxy,tableName,  keySerializer, valueSerializer); 
	}
	
	public static <K,V> KeyValueTable_<K,V> getTable(IKeyValueDataStoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer, String tableReference, Serializer<Object> referenceSerializer) {
		return new KeyValueTable_<K,V>(proxy,tableName,  keySerializer, valueSerializer,tableReference , referenceSerializer );
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
	
	/**
	 * @param proxy
	 * @param tableName
	 * @param keySerializer
	 * @param valueSerializer
	 */
	protected KeyValueTable_(IKeyValueDataStoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer){
		this(proxy, tableName, keySerializer, valueSerializer, null, null); 
	
	}
	
	/**
	 * @param proxy
	 * @param tableName
	 * @param keySerializer
	 * @param valueSerializer
	 */
	protected KeyValueTable_(IKeyValueDataStoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer,String tableReference, Serializer<Object> referenceSerializer) {
		super(proxy, tableName, keySerializer, tableReference);
		this.valueSerializer = valueSerializer; 
		datastore = proxy;
		this.referenceSerializer = referenceSerializer;
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
		return result != null  ?  new VersionedValue<V>(DatastoreValue.timeStampValues ? ((TimestampedDatastoreValue) result).ts : 0, valueSerializer.deserialize(result.getRawData())) : null;
	}

	/* (non-Javadoc)
	 * @see bonafide.getRawData()store.tables.Table#remove(java.lang.Object)
	 */
	@Override
	public V remove(K key) {
		DatastoreValue result = datastore.remove(tableName, serializeKey(key));
		return result != null && result.getRawData() != null? valueSerializer.deserialize(result.getRawData()) : null; 
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
		return result != null  ?  new VersionedValue<V>(DatastoreValue.timeStampValues ? ((TimestampedDatastoreValue) result).ts : 0, valueSerializer.deserialize(result.getRawData())) : null;
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
		return result != null ? new VersionedValue<V>( DatastoreValue.timeStampValues ? ((TimestampedDatastoreValue) result).ts : 0 ,valueSerializer.deserialize(result.getRawData())): null;
	}
	
	@Override
	public Collection<V> values(){
		Collection<DatastoreValue> byte_values = datastore.values(tableName); 
		Collection<V>  values = Lists.newArrayList(); 
		for (DatastoreValue ba : byte_values){
			values.add(valueSerializer.deserialize(ba.getRawData()));
		}
		return values; 
	}
	
	/**
	 * @param value
	 * @return
	 */
	protected byte[] serializeValue(V value) {
		return valueSerializer.serialize(value);
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
							DatastoreValue.timeStampValues ? ((TimestampedDatastoreValue) val).ts : 0, (V1) valueSerializer.deserialize(val.getRawData())) 
							: null;

	}
	
	

	/* (non-Javadoc) 
	 * @see bonafide.getRawData()store.tables.KeyValueTable#replace(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean replace(K key, V currentValue, V newValue) {
		return datastore.replace(tableName, keySerializer.serialize(key), valueSerializer.serialize(currentValue), valueSerializer.serialize(newValue));
	}
	
	@Override
	public boolean replace(K key, int knownVersion , V newValue) {
		return datastore.replace(tableName, keySerializer.serialize(key), knownVersion, valueSerializer.serialize(newValue));
	}

}
	

