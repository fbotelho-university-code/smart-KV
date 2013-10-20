/**
 * 
 */
package smartkv.client.tables;

import java.util.Map;
import java.util.Set;

import smartkv.client.DatastoreValue;
import smartkv.client.IKeyValueColumnDatastoreProxy;
import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;

/**
 * @author fabiim
 *
 */
public class ColumnTable_<K, V> extends KeyValueTable_<K, V> implements
		IColumnTable<K, V> {

	private IKeyValueColumnDatastoreProxy datastore;
	private ColumnObject<V> valueSerializer;
	
	public ColumnTable_(TableBuilder<K,V> builder){
		super(builder);
		datastore = (IKeyValueColumnDatastoreProxy) builder.getProxy(); 
		valueSerializer = builder.getColumnSerializer(); 
	}
	
	/**
	 * @param proxy
	 * @param tableName
	 * @param keySerializer
	 * @param valueSerializer
	 */

	/*public static <K,V> ColumnTable_<K,V>  getTable(ColumnProxy proxy, String tableName, Serializer<K> keySerializer, ColumnObject<V> valueSerializer){
		return new ColumnTable_<K, V>(proxy, tableName, keySerializer, valueSerializer); 
	}
	*/
    //FIXME: do builder for tables... Then when id and name is the same , it just returns the same object.... 
	
	/*public static <K extends Serializable,V> ColumnTable_<K,V> getTableDefault(String tableName , Class<V> clazz) {
		return new ColumnTable_<K,V>(new ColumnProxy((int) Thread.currentThread().getId()),tableName,  JavaSerializer.<K>getJavaSerializer(), AnnotatedColumnObject.newAnnotatedColumnObject(clazz)); 
	}
	*/
	//FIXME: constructor...
	//FIXME: this is a good point favoring uniting the Table - KeyValue - Column interface hierarchie in a single line... At least it is a good point showing that this code sucks...
	/*private ColumnTable_(ColumnProxy proxy, String tableName, Serializer<K> keySerializer, ColumnObject<V> valueSerializer) {
		super(proxy, tableName, keySerializer); 
		datastore = proxy; 
		this.valueSerializer = valueSerializer; 
	}*/
		
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnTable#getColumn(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumn(K key, String columnName) {
		return (C) deserializeColumn(columnName, datastore.getColumn(tableName, serializeKey(key), columnName)) ;
	}
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnTable#getColumnByReference(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumnByReference(K key, String columnName) {
		return (C) deserializeColumn(columnName, datastore.getColumnByReference(tableName, serializeKey(key), columnName)) ;
	}
	
	@Override 
	public boolean setColumn(K key, String columnName, Object value){
		return datastore.setColumn(tableName, serializeKey(key), columnName, serializeColumn(columnName, value));
	}
	
	public V getColumns(K key, Set<String> columns){
		DatastoreValue v = datastore.getColumns(tableName, serializeKey(key), columns);
		return deserializeValue(v); 
	}
	
	// *****  Serialization ************** 


	private Serializer<Map<String,byte[]>>  serialize = UnsafeJavaSerializer.<Map<String,byte[]>>getInstance();  
	
	@Override
	protected V deserializeValue(DatastoreValue v) {
		if (v != null){
			return valueSerializer.fromColumns(serialize.deserialize(v.getRawData())); 
		}
		return null;
	}
	
	@Override
	protected byte[] serializeValue(V value) {
		try{
			return serialize.serialize(valueSerializer.toColumns(value));
		}catch (NullPointerException e){
			e.printStackTrace();

		}
		System.exit(0);
		return null;
	}

	/**
	 * @param columnName
	 * @param column
	 * @return
	 */
	@SuppressWarnings("unchecked")
	//FIXME:see what can be done with static typing and cast... 
	protected <C> C deserializeColumn(String columnName, byte[] value) {
		return (C) valueSerializer.deserializeColumn(columnName, value);
	}
	
	protected byte[] serializeColumn(String columnName, Object value){
		return valueSerializer.serializeColumn(columnName, value);
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IColumnTable#getColumnsSerializer()
	 */
	@Override
	public ColumnObject<V> getColumnsSerializer() {
		//FIXME isIt safe to share ?
		return valueSerializer; 
	}
		
	
	

}
