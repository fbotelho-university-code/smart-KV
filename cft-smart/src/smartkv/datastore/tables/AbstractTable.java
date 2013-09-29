/**al
 * 
 */
package smartkv.datastore.tables;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import smartkv.datastore.TableDataStoreProxy;
import smartkv.datastore.util.JavaSerializer;
import smartkv.datastore.util.Serializer;

/**
 * @author fabiim
 *
 */
public abstract  class AbstractTable<K, V> implements Table<K, V> {
	
	TableDataStoreProxy datastore;
	String tableName; 
	Serializer<K> keySerializer;
	
	
	protected AbstractTable(TableDataStoreProxy proxy, String tableName, Serializer<K> keySerializer, String reference){
		this.tableName = tableName;
		this.datastore = proxy;
		this.keySerializer = keySerializer;
		if (!datastore.containsTable(tableName)){
			if (reference == null){
				datastore.createTable(tableName);
			}
			else{
				datastore.createPointerTable(tableName, reference);
			}
		}
	}
	
	protected AbstractTable(TableDataStoreProxy proxy, String tableName, Serializer<K> keySerializer){
		this(proxy, tableName, keySerializer, null);
	}
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#clear()
	 */
	@Override
	public void clear() {
		datastore.clear(tableName); 

	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#containsKey(java.lang.Object)
	 */
	@Override
	public boolean containsKey(K key) {
		return datastore.containsKey(tableName, serializeKey(key)); 
	}


	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#entrySet()
	 */
	@Override
	public Set<Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException("Not yet implemented!");
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return datastore.isEmpty(tableName);
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#keySet()
	 */
	@Override
	public Set<K> keySet() {
		throw new UnsupportedOperationException("Not yet implemented!"); 
	}


	

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException("Not yet implemented!"); 

	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#size()
	 */
	@Override
	public int size() {
		//FIXME
		return (int) datastore.size(tableName); 
	}

	

	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.Table#getAndIncrement(java.lang.Object)
	 */
	@Override
	public int getAndIncrement(String key) {
		//FIXME - documentation, data store etc.. If key of table is string, then you may have a problem.... Do not use the same  
		return datastore.getAndIncrement(tableName, key);
	}
		
	/**
	 * @param key
	 * @return
	 */
	protected byte[] serializeKey(K key) {
		return keySerializer.serialize(key);
	}
}
