/**
 * 
 */
package smartkv.client.tables;

import java.util.Set;

import net.floodlightcontroller.devicemanager.internal.Device;
import net.floodlightcontroller.devicemanager.internal.Entity;
import net.floodlightcontroller.devicemanager.internal.IndexedEntity;

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

	public VersionedValue<Object> getColumnsByReference(K key, Set<String> columns); 
	/**
	 * Get the name of the table 
	 * @return the name of the table 
	 */
	public String getName();
	
	public Integer roundRobin(String id);

	/**
	 * @param ieSource
	 * @param ieDestination
	 * @return
	 */
	byte[] getTwoDevices(IndexedEntity ieSource, IndexedEntity ieDestination); 
	
	public Device createDevice(Entity entity);

	/**
	 * @param deviceKey
	 * @param version
	 * @param entityindex
	 * @param l
	 * @return
	 */
	boolean updateDevice(Long deviceKey, int version, int entityindex, long l);
}
