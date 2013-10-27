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
	/**
	 * @param key
	 * @param delta
	 * @return
	 */
	VersionedValue<Object> getVersionedValueByReference(K key, long delta);
	/**
	 * @param entity
	 * @return
	 */
	

}
