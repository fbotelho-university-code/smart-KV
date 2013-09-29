/**
 * 
 */
package smartkv.datastore.tables;

/**
 * @author fabiim
 *
 */
public interface ColumnTable<K, V> extends KeyValueTable<K,V>{
	public <C>  C getColumn(K key, String columnName);
	public <C>  C getColumnByReference(K key, String columnName);
	public boolean setColumn(K key, String columnName, Object type); 
}
