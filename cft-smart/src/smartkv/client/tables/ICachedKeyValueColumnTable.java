/**
 * 
 */
package smartkv.client.tables;

/**
 * @author fabiim
 *
 */
public interface ICachedKeyValueColumnTable<K, V> extends
		ICachedKeyValueTable<K, V>, IColumnTable<K,V>{
	
	public <C> C getColumn(K key, String columnName, long ts); 

}
