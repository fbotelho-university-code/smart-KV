/**
 * 
 */
package smartkv.client.tables;

import java.util.Set;

/**
 * @author fabiim
 *
 */
public interface ICachedKeyValueTable<K, V> extends IKeyValueTable<K,V> {
	
	public V getCached(K key); 
	public V get(K key, long ts);
	public <V1> V1 getValueByReference(K key, long ts);
	/**
	 * @param key
	 * @param columns
	 * @param delta
	 * @return
	 */
	VersionedValue<Object> getColumnsByReference(K key, Set<String> columns,
			long delta);
	
}
