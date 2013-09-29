/**
 * 
 */
package smartkv.client.tables;

import java.util.Collection;

import smartkv.client.DatastoreValue;
import smartkv.client.KeyValueDatastoreProxy;
import smartkv.client.util.Serializer;

import com.google.common.collect.Lists;

/**
 * @author fabiim
 *
 */
public class KeyValueTable_<K, V> extends AbstractTable<K,V> implements KeyValueTable<K,V>{
	
	//Shadow "re-declaration" of datastore. Because we need a more specific type. 
	protected KeyValueDatastoreProxy datastore;
	protected Serializer<V> valueSerializer;
	protected Serializer<Object> referenceSerializer; 
	
	public static <K,V> KeyValueTable_<K,V> getTable(KeyValueDatastoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		return new KeyValueTable_<K,V>(proxy,tableName,  keySerializer, valueSerializer); 
	}
	
	public static <K,V> KeyValueTable_<K,V> getTable(KeyValueDatastoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer, String tableReference, Serializer<Object> referenceSerializer) {
		return new KeyValueTable_<K,V>(proxy,tableName,  keySerializer, valueSerializer,tableReference , referenceSerializer );
	}
	
	/**
	 * @param proxy
	 * @param tableName
	 * @param keySerializer
	 * @param valueSerializer
	 */
	protected KeyValueTable_(KeyValueDatastoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer){
		this(proxy, tableName, keySerializer, valueSerializer, null, null); 
	
	}
	
	/**
	 * @param proxy
	 * @param tableName
	 * @param keySerializer
	 * @param valueSerializer
	 */
	protected KeyValueTable_(KeyValueDatastoreProxy proxy, String tableName,
			Serializer<K> keySerializer, Serializer<V> valueSerializer,String tableReference, Serializer<Object> referenceSerializer) {
		super(proxy, tableName, keySerializer, tableReference);
		this.valueSerializer = valueSerializer; 
		datastore = proxy;
		this.referenceSerializer = referenceSerializer;
		
	}

	
	/* 
	 * @see bonafide.datastore.tables.Table#remove(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean remove(K key, V value) {
		return datastore.remove(tableName, serializeKey(key), serializeValue(value));
	}
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V putIfAbsent(K key, V value) {
		DatastoreValue result =  datastore.putIfAbsent(tableName, serializeKey(key), serializeValue(value));
		return result != null && result.data != null ? valueSerializer.deserialize(result.data): null; 
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#remove(java.lang.Object)
	 */
	@Override
	public V remove(K key) {
		DatastoreValue result = datastore.remove(tableName, serializeKey(key));
		return result != null && result.data != null? valueSerializer.deserialize(result.data) : null; 
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V put(K key, V value) {
		DatastoreValue result = datastore.put(tableName, serializeKey(key), serializeValue(value));
		return result != null && result.data != null ?  valueSerializer.deserialize(result.data) : null; 
	}
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#insert(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean insert(K key, V value) {
		return datastore.insert(tableName, serializeKey(key), serializeValue(value));
	}


	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#get(java.lang.Object)
	 */
	@Override
	public V get(K key) {
		//XXX
		if (key == null) return null; 
		DatastoreValue result = datastore.get(tableName, serializeKey(key));
		return result != null ? valueSerializer.deserialize(result.data): null;
	}

	@Override
	public Collection<V> values(){
		Collection<DatastoreValue> byte_values = datastore.values(tableName); 
		Collection<V>  values = Lists.newArrayList(); 
		for (DatastoreValue ba : byte_values){
			values.add(valueSerializer.deserialize(ba.data));
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
	 * @see bonafide.datastore.tables.KeyValueTable#getValueByReference(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <V1> V1 getValueByReference(K key) {
		DatastoreValue val = datastore.getByReference(tableName, keySerializer.serialize(key));
		return (V1) (val !=  null && val.data != null? referenceSerializer.deserialize(val.data) : null); 
	}


	/* (non-Javadoc) 
	 * @see bonafide.datastore.tables.KeyValueTable#replace(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean replace(K key, V currentValue, V newValue) {
		return datastore.replace(tableName, keySerializer.serialize(key), valueSerializer.serialize(currentValue), valueSerializer.serialize(newValue));
	}

	
}
