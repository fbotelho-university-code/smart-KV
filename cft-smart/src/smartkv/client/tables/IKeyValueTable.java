/**
 * 
 */
package smartkv.client.tables;

/**
 * @author fabiim
 *
 */
public interface IKeyValueTable<K, V> extends ITable<K,V>{

	
	/**
	 * @param key
	 * @param value
	 * @return
	 */
	boolean remove(K key, V value);

	/**
	 * @param key
	 * @param value
	 * @return
	 */
	V putIfAbsent(K key, V value);

	/**
	 * @param key
	 * @return
	 */
	V remove(K key);

	/**
	 * @param key
	 * @param value
	 * @return
	 */
	V put(K key, V value);

	/**
	 * @param key
	 * @return
	 */
	V get(K key);

	boolean insert(K key, V value);
	
	public boolean replace(K key, V currentValue, V newValue);
	
	/**
	 * @param key
	 * @return
	 */
	public <V1> V1 getValueByReference(K key);


	/**
	 * @param key
	 * @param knownVersion
	 * @param newValue
	 * @return
	 */
	boolean replace(K key, int knownVersion, V newValue);
	
	public <V1> VersionedValue<V1> getValueByReferenceWithTimestamp(K key); 

	/**
	 * @param key
	 * @return
	 */
	public VersionedValue<V> getWithTimeStamp(K key);
	
	public VersionedValue<V> putAndGetPreviousWithTimestamp(K key, V value);
	public VersionedValue<V> putIfAbsentWithTimestamp(K key, V value); 
}
