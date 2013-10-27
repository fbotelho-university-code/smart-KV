/**
 * 
 */
package smartkv.client.tables;

import net.floodlightcontroller.devicemanager.internal.Device;
import net.floodlightcontroller.devicemanager.internal.Entity;
import net.floodlightcontroller.devicemanager.internal.IndexedEntity;

/**
 * @author fabiim
 *
 */
public interface IColumnTable<K, V> extends IKeyValueTable<K,V>{
	public <C>  C getColumn(K key, String columnName);
	public <C>  C getColumnByReference(K key, String columnName);
	public boolean setColumn(K key, String columnName, Object type);
	
	/**
	 * @return
	 */
	public ColumnObject<V> getColumnsSerializer();
	/**
	 * @param key
	 * @param currentValue
	 * @param columnName
	 * @param value
	 * @return
	 */
	boolean replaceColumn(K key, int currentValue, String columnName,
			Object value);
	
	
	
}
