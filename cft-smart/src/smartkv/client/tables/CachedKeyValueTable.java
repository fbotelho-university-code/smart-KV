package smartkv.client.tables;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Maps;


// Only works if the datastore is with timestamped values....

//FIXME the modifications to the cache are concurrent. So we need concurrent hash map with weak references.  
//FIXME Or we can use concurrent with least recently used. 

public class CachedKeyValueTable<K,V> implements ICachedKeyValueTable<K,V>{
	IKeyValueTable<K,V> table;
	
	public static <K,V> CachedKeyValueTable<K,V> startCache(IKeyValueTable<K,V> table){
		return new CachedKeyValueTable<K,V>(table);
	}
	
	protected CachedKeyValueTable(IKeyValueTable<K,V> table){
		this.table = table; 
	}
	
	//TODO - extends the final map to take care of values for you. 
	static class ClockTimeStampValue<V>{
		//TODO check weak references... 
		final VersionedValue<V> value;
		final long timestamp;
		ClockTimeStampValue(VersionedValue<V> v){
			value = v; 
			timestamp = System.currentTimeMillis(); 
		}
	}
	
	//FIXME , probably bad ideia. Sould use eviction hash map, and limit size. The GC will eat himself up everytime it runs because it will clean all the references....  
	Map<K,ClockTimeStampValue<V>> cache =  Maps.newConcurrentMap(); 
	Map<K,ClockTimeStampValue<Object>> referencesCache =  Maps.newConcurrentMap();
	
	@Override
	public V get(K key, long timestamp) {
		ClockTimeStampValue<V> cached_value = cache.get(key);
		if (cached_value != null){
			if (System.currentTimeMillis() - cached_value.timestamp < timestamp){
				return cached_value.value.value;
			}
		}
		return getValueAndUpdateCache(key);  
	}
	
	/**
	 * @param key, 
	 * @return
	 */
	private V getValueAndUpdateCache(K key) {
		ClockTimeStampValue<V> cached_value;
		//Nothing in cache, let us go get them from the data store... 
		VersionedValue<V>  value = table.getWithTimeStamp(key);
		if (value != null){
			cached_value = new ClockTimeStampValue<V>(value);
			cache.put(key,cached_value);
			return value.value; 
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public <V1> V1 getValueByReference(K key, long delta) {
		ClockTimeStampValue<Object> cached_value = referencesCache.get(key); 
		if (cached_value != null){
			if (System.currentTimeMillis() - cached_value.timestamp < delta){
				 return (V1) cached_value.value.value; 
			}
		}
		return getReferenceAndUpdateCache(key); 
	}

	/**
	 * @param key
	 * @return
	 */
	private <V1> V1 getReferenceAndUpdateCache(K key) {
		VersionedValue<Object> v = table.getValueByReferenceWithTimestamp(key); 
		if (v != null){
			referencesCache.put(key, new ClockTimeStampValue<Object>(v));
			return (V1) v.value; 
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#getColumnsByReference(java.lang.Object, java.util.Set)
	 */
	@Override
	public VersionedValue<Object> getColumnsByReference(K key,
			Set<String> columns, long delta) {
		ClockTimeStampValue<Object> cached_value = referencesCache.get(key);
		
		if (cached_value != null){
			
			System.out.println((System.currentTimeMillis() - cached_value.timestamp) +" < " + delta + "? " + ((System.currentTimeMillis() - cached_value.timestamp) < delta) + " - " + columns);
			if ((System.currentTimeMillis() - cached_value.timestamp) < delta){
				 return cached_value.value;  
			}
		}
		System.out.println("not found on cache: " + key);
		return getColumnsByReferenceAndUpdateCache(key,columns); 
	}
	/**
	 * @param key
	 * @param columns
	 * @return
	 */
	private VersionedValue<Object> getColumnsByReferenceAndUpdateCache(K key,
			Set<String> columns) {
		VersionedValue<Object> v = table.getColumnsByReference(key, columns);
		if (v!= null){
			System.out.println("Updated cache : " + key + " -> " + v);
			referencesCache.put(key, new ClockTimeStampValue<Object>(v));
		}
		return v; 
	}
	@Override
	public VersionedValue<Object> getColumnsByReference(K key,
			Set<String> columns) {
		return getColumnsByReferenceAndUpdateCache(key, columns); 
	}
	
	@Override
	public V getCached(K key){
		ClockTimeStampValue<V> cached_value = cache.get(key);
		if (cached_value != null){
			return cached_value.value.value;
		}
		else 
			return null; 
	}
	
	@Override
	public V get(K key) {
		return getValueAndUpdateCache(key); 
	}
	
	@Override
	public <V1> V1 getValueByReference(K key) {
		return getReferenceAndUpdateCache(key);
	}
	
	@Override
	public V put(K key, V value) {
		VersionedValue<V> oldValue = table.putAndGetPreviousWithTimestamp(key, value);
		return updatedValueFromPrevious(key, value, oldValue);  
	}
	
	@Override
	public boolean insert(K key, V value) {
		//Right now we can't have a clue what is the current ts is because we used insert...
		//FIXME: we should be able to keep the value... 
		cache.remove(key); 
		return table.insert(key, value);
	}
	
	/**
	 * @param key
	 * @param value
	 * @param oldValue
	 * @return
	 */
	private V updatedValueFromPrevious(K key, V value,
			VersionedValue<V> oldValue) {
		cache.put(key, new ClockTimeStampValue<V> (new VersionedValue<V>( oldValue != null ? oldValue.version() +1 : 0 , value)));
		return oldValue != null  ? oldValue.value : null;
	}
	
	@Override
	public V remove(K key) {
		cache.remove(key);
		return table.remove(key);
	}
	
	@Override
	public boolean replace(K key, V existentValue, V newValue) {
		cache.remove(key);
		return table.replace(key, existentValue, newValue); 
	}
	
	@Override 
	public boolean replace(K key, int knownVersion, V newValue){
		boolean hasReplaced = table.replace(key, knownVersion, newValue);
		if (hasReplaced){
			cache.put(key, new ClockTimeStampValue<V> (new VersionedValue<V>(knownVersion +1 , newValue))); 
		}
		else{
			cache.remove(key); 
		}
		return hasReplaced; 
	}
	
	@Override
	public boolean remove(K key, V value) {
		cache.remove(key); 
		// TODO Auto-generated method stub
		return table.remove(key, value);
	}
	
	@Override
	public V putIfAbsent(K key, V value) {
		//XXX - think about the typical use case of this method. Maybe it can be optimized...   
		VersionedValue<V> existentValue = table.putIfAbsentWithTimestamp(key, value);
		//FIXME worst performance ever. Recreating objects and stuff... 
		cache.put(key,
				new ClockTimeStampValue<V> (new VersionedValue<V>(existentValue == null ? 0 : existentValue.ts, existentValue == null ? value : existentValue.value))
				);
		return null; 
	}
	
	@Override
	public VersionedValue<V> putAndGetPreviousWithTimestamp(K key, V value){
		 VersionedValue<V> oldValue = table.putAndGetPreviousWithTimestamp(key, value);
		 updatedValueFromPrevious(key, value, oldValue);
		 return oldValue; 
	}

	public void clear() {
		table.clear();
	}

	public boolean containsKey(K key) {
		return table.containsKey(key);
	}

	public Set<Entry<K, V>> entrySet() {
		return table.entrySet();
	}

	public boolean isEmpty() {
		return table.isEmpty();
	}

	public Set<K> keySet() {
		return table.keySet();
	}

	public void putAll(Map<? extends K, ? extends V> m) {
		table.putAll(m);
	}

	public int size() {
		return table.size();
	}

	public Collection<V> values() {
		return table.values();
	}

	public int getAndIncrement(String key) {
		return table.getAndIncrement(key);
	}

	//FIXME Why I am not adding this to the cache?
	public <V1> VersionedValue<V1> getValueByReferenceWithTimestamp(K key) {
		return table.getValueByReferenceWithTimestamp(key);
	}
	//FIXME Why I am not adding this to the cache?
	public VersionedValue<V> getWithTimeStamp(K key) {
		return table.getWithTimeStamp(key);
	}
	//FIXME Why I am not adding this to the cache?
	public VersionedValue<V> putIfAbsentWithTimestamp(K key, V value) {
		return table.putIfAbsentWithTimestamp(key, value);
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#getName()
	 */
	@Override
	public String getName() {
		return table.getName();
	}

	

	
}