/**
 * 
 */
package smartkv.client.tables;


import smartkv.client.ColumnProxy;
import smartkv.client.IDataStoreProxy;
import smartkv.client.KeyValueProxy;
import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;

/**
 * @author fabiim
 *
 */
public class TableBuilder<K,V> {
	
	private Integer cid = null; 
	private IDataStoreProxy proxy = null; 
	
	private String tableName = null; 
	private String crossReferenceTable = null;
	
	
	private Serializer<K> keySerializer = UnsafeJavaSerializer.getInstance();
	private Serializer<V> valueSerializer = UnsafeJavaSerializer.getInstance(); 
	private Serializer<Object> crossReferenceValueSerializer = UnsafeJavaSerializer.getInstance();
	private ColumnObject<Object> crossReferenceColumnSerializer = null;
	private ColumnObject<V> columnSerializer = null;

	public Integer getCid() {
		return cid;
	}

	
	public ColumnObject<Object> getCrossReferenceColumnSerializer() {
		return crossReferenceColumnSerializer;
	}


	public TableBuilder<K,V> setCrossReferenceColumnSerializer(
			ColumnObject<Object> crossReferenceColumnSerializer) {
		this.crossReferenceColumnSerializer = crossReferenceColumnSerializer;
		return this; 
	}


	public TableBuilder<K,V> setCid(Integer cid) {
		this.cid = cid;
		return this; 
	}

	public IDataStoreProxy getProxy(){
		return this.proxy; 
	}
	public IDataStoreProxy getOrCreateProxy() {
		return proxy != null ? proxy : createProxy(cid) ;
	}

	/**
	 * @param cid2
	 * @return
	 */
	private IDataStoreProxy createProxy(Integer cid2) {
		//what kind of proxy i need? 
		if (this.columnSerializer == null){
			return new KeyValueProxy(cid2); 
		}
		return new ColumnProxy(cid2); 
	}

	public TableBuilder<K,V> setProxy(IDataStoreProxy proxy) {
		this.proxy = proxy;return this;
	}

	public String getTableName() {
		return tableName;
	}

	public TableBuilder<K,V> setTableName(String tableName) {
		this.tableName = tableName;return this;
	}

	public String getCrossReferenceTable() {
		return crossReferenceTable;
	}

	public TableBuilder<K,V> setCrossReferenceTable(String crossReferenceTable) {
		this.crossReferenceTable = crossReferenceTable;return this;
	}

	public Serializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TableBuilder<K,V> setKeySerializer(Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;return this;
	}

	public Serializer<V> getValueSerializer() {
		return valueSerializer;
	}

	public TableBuilder<K,V> setValueSerializer(Serializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;return this;
	}

	public Serializer<Object> getCrossReferenceValueSerializer() {
		return crossReferenceValueSerializer;
	}

	public TableBuilder<K,V> setCrossReferenceValueSerializer(
			Serializer<Object> crossReferenceValueSerializer) {
		this.crossReferenceValueSerializer = crossReferenceValueSerializer;return this;
	}

	public ColumnObject<V> getColumnSerializer() {
		return columnSerializer; 
	}

	public TableBuilder<K,V> setColumnSerializer(ColumnObject<V> columnSerializer) {
		this.columnSerializer = columnSerializer;return this;
	} 
	
	public TableBuilder<K,V> setColumnSerializer(Class<V> clazz) {
		this.columnSerializer = AnnotatedColumnObject.newAnnotatedColumnObject(clazz); 
		return this;
	}


	/**
	 * @param class1
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public TableBuilder<K, V> setCrossReferenceColumnSerializer(
			Class<Object> class1) {
		this.crossReferenceColumnSerializer = (ColumnObject<V>) AnnotatedColumnObject.newAnnotatedColumnObject(class1);
		return this; 
	} 
	
	
	
}
