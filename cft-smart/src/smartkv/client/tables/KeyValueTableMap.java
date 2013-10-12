/**
 * 
 */
package smartkv.client.tables;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A Map interface to a KeyValueTable
 * @author fabiim
 *
 */
public class KeyValueTableMap<K,V> implements Map<K, V>{

	IKeyValueTable<K,V> table;
	/* (non-Javadoc)
	 * @see java.util.Map#clear()
	 */
	@Override
	public void clear() {
		table.clear();
		
	}

	/* (non-Javadoc)
	 * @see java.util.Map#containsKey(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean containsKey(Object key) {
		return table.containsKey((K) key);
	}

	/* (non-Javadoc)
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException("Not supported");
	}

	/* (non-Javadoc)
	 * @see java.util.Map#entrySet()
	 */
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return table.entrySet(); 
	}

	/* (non-Javadoc)
	 * @see java.util.Map#get(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		return table.get((K) key);
	}

	/* (non-Javadoc)
	 * @see java.util.Map#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return table.isEmpty(); 
	}

	/* (non-Javadoc)
	 * @see java.util.Map#keySet()
	 */
	@Override
	public Set<K> keySet() {
		return table.keySet(); 
	}

	/* (non-Javadoc)
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V put(K key, V value) {
		return table.put(key, value);
	}

	/* (non-Javadoc)
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException("Not yet implemented!");
	}

	/* (non-Javadoc)
	 * @see java.util.Map#remove(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		return table.remove((K) key);
	}

	/* (non-Javadoc)
	 * @see java.util.Map#size()
	 */
	@Override
	public int size(){
		return table.size();
	}

	/* (non-Javadoc)
	 * @see java.util.Map#values()
	 */
	@Override
	public Collection<V> values() {
		return table.values();
	}

	

}
