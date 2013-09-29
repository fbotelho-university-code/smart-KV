/**
 * 
 */
package smartkv.client.tables;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import smartkv.client.ColumnProxy;
import smartkv.client.KeyValueColumnDatastoreProxy;
import smartkv.client.util.JavaSerializer;
import smartkv.client.util.Serializer;

import com.google.common.collect.Lists;

/**
 * @author fabiim
 *
 */
public class ColumnTable_<K, V> extends AbstractTable<K, V> implements
		ColumnTable<K, V> {

	private KeyValueColumnDatastoreProxy datastore;
	private ColumnObject<V> valueSerializer;
	
	/**
	 * @param proxy
	 * @param tableName
	 * @param keySerializer
	 * @param valueSerializer
	 */

	public static <K,V> ColumnTable_<K,V>  getTable(ColumnProxy proxy, String tableName, Serializer<K> keySerializer, ColumnObject<V> valueSerializer){
		return new ColumnTable_<K, V>(proxy, tableName, keySerializer, valueSerializer); 
	}
	
    //FIXME: do builder for tables... Then when id and name is the same , it just returns the same object.... 
	
	public static <K extends Serializable,V> ColumnTable_<K,V> getTableDefault(String tableName , Class<V> clazz) {
		return new ColumnTable_<K,V>(new ColumnProxy((int) Thread.currentThread().getId()),tableName,  JavaSerializer.<K>getJavaSerializer(), AnnotatedColumnObject.newAnnotatedColumnObject(clazz)); 
	}
	
	//FIXME: constructor...
	//FIXME: this is a good point favoring uniting the Table - KeyValue - Column interface hierarchie in a single line... At least it is a good point showing that this code sucks...
	private ColumnTable_(ColumnProxy proxy, String tableName, Serializer<K> keySerializer, ColumnObject<V> valueSerializer) {
		super(proxy, tableName, keySerializer); 
		datastore = proxy; 
		this.valueSerializer = valueSerializer; 
	}
		
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnTable#getColumn(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumn(K key, String columnName) {
		return (C) deserializeColumn(columnName, datastore.getColumn(tableName, serializeKey(key), columnName)) ;
	}
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnTable#getColumnByReference(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumnByReference(K key, String columnName) {
		return (C) deserializeColumn(columnName, datastore.getColumnByReference(tableName, serializeKey(key), columnName)) ;
	}
	
	public boolean setColumn(K key, String columnName, Object value){
		return datastore.setColumn(tableName, serializeKey(key), columnName, serializeColumn(columnName, value));
	}
	
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#remove(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean remove(K key, V value) {
		return datastore.remove(tableName, serializeKey(key), serializeValue(value)); 
	}


	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V putIfAbsent(K key, V value) {
		return deserializeValue(datastore.putIfAbsent(tableName, serializeKey(key), serializeValue(value)));
	}

	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#remove(java.lang.Object)
	 */
	@Override
	public V remove(K key) {
		return deserializeValue(datastore.removeValue(tableName, serializeKey(key))); 
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V put(K key, V value) {
		return deserializeValue(datastore.put(tableName, serializeKey(key), serializeValue(value))); 
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#get(java.lang.Object)
	 */
	@Override
	public V get(K key) {
		
		return deserializeValue(datastore.getValue(tableName, serializeKey(key))); 
	}


	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#getValueByReference(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <V1> V1 getValueByReference(K key) {
		return (V1) deserializeValue(datastore.getValueByReference(tableName, serializeKey(key))); 
	}
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException("Not yet implemented!");
	}
	
	/**
	 * @param columnName
	 * @param column
	 * @return
	 */
	@SuppressWarnings("unchecked")
	//FIXME:see what can be done with static typing and cast... 
	protected <C> C deserializeColumn(String columnName, byte[] value) {
		return (C) valueSerializer.deserializeColumn(columnName, value);
	}
	
	protected byte[] serializeColumn(String columnName, Object value){
		return valueSerializer.serializeColumn(columnName, value);
	}
		
	/**
	 * @param value
	 * @return
	 */
	private Map<String, byte[]> serializeValue(V value) {
		 Map<String, byte[]> val = valueSerializer.toColumns(value);
		 return val; 
	}

	/**
	 * @param putIfAbsent
	 * @return
	 */
	private V deserializeValue(Map<String, byte[]> values) {
		return values != null ? valueSerializer.fromColumns(values) : null;   
	}
	
	

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#insert(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean insert(K key, V value) {
		return datastore.insert(tableName, serializeKey(key), serializeValue(value));
	}

	@Override
	public Collection<V> values(){
		Collection<Map<String,byte[]>> byte_values = datastore.valueS(tableName); 
		Collection<V>  values = Lists.newArrayList(); 
		for (Map<String,byte[]> m : byte_values){
			values.add(deserializeValue(m));
		}
		return values; 
	}
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#replace(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean replace(K key, V currentValue, V newValue) {
		return datastore.replace(tableName, serializeKey(key), serializeValue(currentValue), serializeValue(newValue));
	}
	
}
