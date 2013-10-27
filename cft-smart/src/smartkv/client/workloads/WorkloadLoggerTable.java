/**
 * 
 */
package smartkv.client.workloads;

//XXX pass Objects to RequestLogData and avoid checking everything if it is null or not... 
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.floodlightcontroller.devicemanager.internal.Device;
import net.floodlightcontroller.devicemanager.internal.Entity;
import net.floodlightcontroller.devicemanager.internal.IndexedEntity;
import smartkv.client.KeyValueProxy;
import smartkv.client.tables.IKeyValueTable;
import smartkv.client.tables.KeyValueTable_;
import smartkv.client.tables.TableBuilder;
import smartkv.client.tables.VersionedValue;
import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;
import smartkv.server.RequestType;

//TODO - It would be nice to have an implementation of this that did not require using the smart middleware, only the server implementation locally. It would be faster to perform Workload analysis. 

/**
* A Data store proxy client that records <b>RPC</b> requests characteristics for  <b>one</b> request. 
* Yeah, I known, one instance per/ request sounds lame. I didn't find a way to make this more efficient. However it does the job, and usually while recording 
* requests characteristics we do not  care about efficiency. At least in the use for which this class was designed for we don't.  
* <p>
* 
* Each instance of this class must keep a reference to a {@link RequestLogEntry }. For each request invocation   
* this RequestLogEntry will be set up with: 
* <ul>
* <li> {@link RequestLogEntry#getTimeStarted() timeStarted}     		the time (ns) at which the request was sent to the data store. </li>
* <li> {@link RequestLogEntry#getTimeEnded() timeEnded }         		the time (ns) at which a reply was received from the data store. </li>
* <li> {@link RequestLogEntry#getSizeOfRequest() sizeOfRequest}  		the size (bytes) of the byte array payload sent to the data store. </li>
* <li> {@link RequestLogEntry#getSizeOfResponse() sizeOfResponse}  	the size (bytes) of the reply payload sent from the data store </li>
* <li> {@link RequestLogEntry#getType() type} 			                the operation performed  on the data store</li>
* <li> </li>
* </ul>
* 
* The values of <code>timeStarted</code> and TimeEnded are taken before and after the invocation of {@link AbstractDatastoreProxy#invokeRequest(RequestType, byte[])}. There units are taken in nanoseconds.
* <p>    
* As for <code>sizeOfRequest</code> and <code>sizeOfResponse</code> they measure in bytes the request and reply <b>RPC</b> payload sent/received from the data store. 
* Please notice that this does not take into account: the total packet size sent, or the overhead of the data store middleware protocol in use (e.g., consensus data).  
* <p>
* Finally, <code>type</code> sets the <b>RPC</b> operation information ({@link RequestType}).   
* <p>
* 
*/

/*class WorkloadLoggerDataStoreProxy {

	private RequestLogEntry logEntry;
		
	*//**
	 * Create 
	 *//*
	public WorkloadLoggerDataStoreProxy(RequestLogEntry logEntry){
		super(0); //FIXME 
		this.logEntry = logEntry; 
	}
		
	*//**
	 * Records requests characteristics in a {@link RequestLogEntry}.
	 * <p>
	 * Characteristics recorded are: timeStarted/timeEnded (ns); sizeOfRequest/sizeofResponse (bytes) and type (RequestType).   
	 *  
	 * @see bonafide.datastore.AbstractDatastoreProxy#invokeRequest(mapserver.RequestType, byte[])
	 * @see WorkloadLoggerDataStoreProxy
	 *//*
	@Override
	protected byte[] invokeRequest(RequestType type, byte[] request) {
		logEntry.setTimeStarted(System.nanoTime());
		logEntry.setSizeOfRequest(request.length);
		logEntry.setType(type);
		byte[] result =  super.invokeRequest(type, request);
		logEntry.setTimeEnded(System.nanoTime());
		return result; 
	}
	
	*//**
	 * Returns the RequestLogEntry used to capture the request characteristics.
	 * @return the RequestLogEntry used.   
	 *//*
	public RequestLogEntry getLogEntry(){
		return logEntry; 
	}
	
}
*/


/**
 * @author fabiim
 * 
 */
/*
 * In a nutshell. Each request to the datastore will be logged. 
 * The RequestLogEntry entry is always the same reference. The underlying WorkloadLoggerDataStoreProxy will change it everytime
 * a request is made to the datastore. (this is actually not thread safe, so we synchronize every request to this facade Table). 
 * This is not meant to be used when performance is the issue. 
 */

public class WorkloadLoggerTable<K,V>  implements IKeyValueTable<K,V>{

	public static <K,V> WorkloadLoggerTable<K,V> workloadLoggerDefault(int cid , String tablename, RequestLogger logger){
		return new WorkloadLoggerTable<K,V>(cid,tablename,logger,  UnsafeJavaSerializer.<K>getInstance(), UnsafeJavaSerializer.<V>getInstance() );
	}
	
	public static <K,V> WorkloadLoggerTable<K,V> workloadLoggerDefaultCrossReference(int cid , String tablename, RequestLogger logger, String crossTable){
		return new WorkloadLoggerTable<K,V>(cid,tablename, logger,   UnsafeJavaSerializer.<K>getInstance(), UnsafeJavaSerializer.<V>getInstance() , crossTable, UnsafeJavaSerializer.getInstance() );
	}
	
	public static <K,V> WorkloadLoggerTable<K,V> withSingletonLogger(TableBuilder<K,V> builder){
		return new WorkloadLoggerTable<K,V>(builder, RequestLogger.getRequestLogger());
	}
	
	protected IKeyValueTable<K,V> table;
	protected RequestLogEntry entry;
	
	protected String tableName;
	protected RequestLogger logger; 
	
	/**
	 * Construct a TableLogger with default (unsafe) serializers for keys and values.
	 */
	public WorkloadLoggerTable(int cid,String tableName, RequestLogger logger){
		this(cid, tableName, logger, UnsafeJavaSerializer.<K>getInstance(), UnsafeJavaSerializer.<V>getInstance()); 
	}
	
	
	protected WorkloadLoggerTable(){
	}
	
	protected WorkloadLoggerTable(TableBuilder<K,V> builder, RequestLogger logger){
		this.entry = new RequestLogEntry();  
		this.logger = logger;
		this.tableName =builder.getTableName();
		if (builder.getProxy() ==null){
		builder.setProxy(new KeyValueProxy(builder.getCid()){
			@Override
			protected byte[] invokeRequestWithRawReturn(RequestType type, byte[] request) {
				entry.setTimeStarted(System.currentTimeMillis());
				entry.setSizeOfRequest(request.length);
				entry.setType(type);
				byte[] result =  super.invokeRequestWithRawReturn(type, request);
				entry.setTimeEnded(System.currentTimeMillis());
				entry.setSizeOfResponse( result != null ? result.length : 0);
				return result; 
			}
		});
		}
		this.table = new KeyValueTable_<K,V>(builder); 
	}
	
	
	public WorkloadLoggerTable(int cid, String tableName, RequestLogger logger, Serializer<K> keys, Serializer<V> values){
		this(cid, tableName, logger, keys, values, null, null); 
	}
	
	
	public WorkloadLoggerTable(int cid, String tableName, RequestLogger logger, Serializer<K> keys, Serializer<V> values, String tablereference, Serializer<Object> referenceSerializer){
		this.entry = new RequestLogEntry(); 
		this.logger = logger;
		this.tableName = tableName; 
		this.table = KeyValueTable_.getTable( new KeyValueProxy(cid){
			@Override
			protected byte[] invokeRequestWithRawReturn(RequestType type, byte[] request) {
				entry.setTimeStarted(System.currentTimeMillis());
				entry.setSizeOfRequest(request.length);
				entry.setType(type);
				byte[] result =  super.invokeRequestWithRawReturn(type, request);
				entry.setTimeEnded(System.currentTimeMillis());
				entry.setSizeOfResponse( result != null ? result.length : 0);
				return result; 
			}; 
		}
		, tableName, keys,values, tablereference, referenceSerializer); 
	}
	

	
	@Override
	public synchronized boolean remove(K key, V value) {
		boolean val = table.remove(key, value);
		logEntry(new RequestLogWithDataInformation.Builder().setTable(tableName).
				setKey(key != null? key.toString() : "null" ).setValue(value != null ? value.toString() : "null").build(entry));
		return val; 
	}
	
	/**
	 * @param build
	 */
	protected void logEntry(RequestLogWithDataInformation req) {
		req.setStrackTrace();
		logger.addRequest(req);
	}

	@Override
	public synchronized V putIfAbsent(K key, V value) {
		V val = table.putIfAbsent(key, value);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setValue(value != null ? value.toString() : "null").
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry));
		return val; 
	}
	@Override
	public synchronized void clear() {
		table.clear();
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				build(entry)); 
	}
	@Override
	public synchronized boolean containsKey(K key) {
		Boolean val = table.containsKey(key);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry));
		return val; 

	}
	@Override
	public synchronized V remove(K key) {
		V val = table.remove(key);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry));
		return val; 
	}

	@Override
	public synchronized Set<Entry<K, V>> entrySet() {
		Set<Entry<K,V>> entries = table.entrySet();
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setReturnedValue(entries.toString()).
				build(entry));
		return entries; 
	}
	
	@Override
	public synchronized V put(K key, V value) {

		V val =  table.put(key, value);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setValue(value != null ? value.toString() : "null").
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry)); 
		return val; 
	}
	
	@Override
	public synchronized boolean isEmpty() {
		Boolean val = table.isEmpty();
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				build(entry));
		return val; 
	}
	@Override
	public synchronized Set<K> keySet() {
		Set<K> keys =  table.keySet();
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setReturnedValue(keys.toString()).
				build(entry));
		return keys; 
	}
	@Override
	public synchronized V get(K key) {
		V val = table.get(key);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry));
		return val; 
	}
	
	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> m) {
		table.putAll(m);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(m != null? m.toString() : "null" ).
				build(entry));
	}
	
	@Override
	public synchronized boolean insert(K key, V value) {
		Boolean b = table.insert(key, value);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setValue(value != null ? value.toString() : "null").
				setReturnedValue(b != null? b.toString(): "null").
				build(entry));
		return b; 
	}
	
	@Override
	public synchronized int size() {
		Integer val = table.size();
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry));
		return val; 
	}
	
	@Override
	public synchronized Collection<V> values() {
		Collection<V> values  =  table.values();
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setReturnedValue(values != null ? values.toString(): "null").
				build(entry));
		return values; 
	}
	
	@Override
	public synchronized int getAndIncrement(String key) {
		Integer val = table.getAndIncrement(key);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setReturnedValue(val != null ? val.toString() : "null" ).build(entry));
		return val; 
	}
	
	/* (non-Javadoc)
	 * @see bonafid
	 * 
	 * e.datastore.tables.KeyValueTable#getValueByReference(java.lang.Object)
	 */
	@Override
	public <V1> V1 getValueByReference(K key) {
		
		V1 val = (V1) table.getValueByReference(key);
		System.out.println(val);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry));
		return val; 
	}
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.KeyValueTable#replace(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean replace(K key, V currentValue, V newValue) {
		Boolean val = table.replace(key, currentValue, newValue); 
		logEntry(new RequestLogWithDataInformation.Builder(). 
				setTable(tableName).
				setKey(key != null ? key.toString() : "null").
				setExistentValue(currentValue != null ? currentValue.toString() : "null").
				setValue(newValue!= null ?newValue.toString() : "null"). 
				setReturnedValue(val.toString()).build(entry)); 
		return val; 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.KeyValueTable#getWithTimeStamp(java.lang.Object)
	 */
	@Override
	public VersionedValue<V> getWithTimeStamp(K key) {
		VersionedValue<V> value =  table.getWithTimeStamp(key); 
		logEntry(new RequestLogWithDataInformation.Builder(). 
				setTable(tableName).
				setKey(key != null ? key.toString() : "null").
				setReturnedValue(value != null ? value.toString() : null).build(entry));
		return value; 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.KeyValueTable#replace(java.lang.Object, int, java.lang.Object)
	 */
	@Override
	public boolean replace(K key, int knownVersion, V newValue) {
		Boolean val = table.replace(key, knownVersion, newValue); 
		logEntry(new RequestLogWithDataInformation.Builder(). 
				setTable(tableName).
				setKey(key != null ? key.toString() : "null").
				setExistentValue("" + knownVersion).
				setValue(newValue!= null ?newValue.toString() : "null"). 
				setReturnedValue(val.toString()).build(entry)); 
		return val; 
	
	}


	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#getValueByReferenceWithTimestamp(java.lang.Object)
	 */
	@Override
	public <V1> VersionedValue<V1> getValueByReferenceWithTimestamp(K key) {
		VersionedValue<V1> val = table.getValueByReferenceWithTimestamp(key);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry));
		return val; 
	}


	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#putAndGetPreviousWithTimestamp(java.lang.Object, java.lang.Object)
	 */
	@Override
	public VersionedValue<V> putAndGetPreviousWithTimestamp(K key, V value) {
		VersionedValue<V> val =  table.putAndGetPreviousWithTimestamp(key, value);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setValue(value != null ? value.toString() : "null").
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry)); 
		return val; 
	}


	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#putIfAbsentWithTimestamp(java.lang.Object, java.lang.Object)
	 */
	@Override
	public VersionedValue<V> putIfAbsentWithTimestamp(K key, V value) {
		VersionedValue<V> val = table.putIfAbsentWithTimestamp(key, value);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setValue(value != null ? value.toString() : "null").
				setReturnedValue(val != null ? val.toString() : "null" ).
				build(entry));
		return val; 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#getName()
	 */
	@Override
	public String getName() {
		return tableName; 
	}
	
	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#getColumnsByReference(java.lang.Object, java.util.Set)
	 */
	@Override
	public VersionedValue<Object> getColumnsByReference(K key,
			Set<String> columns) {
		VersionedValue<Object> val = table.getColumnsByReference(key, columns);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null? key.toString() : "null" ).
				setReturnedValue(val != null ? val.toString() : "null" ).
				setColumns(columns).
				build(entry));
		return val;
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#roundRobin(java.lang.String)
	 */
	@Override
	public Integer roundRobin(String id) {
		Integer v = table.roundRobin(id);
		logEntry(new RequestLogWithDataInformation.Builder().setKey(id).setReturnedValue(v != null ? v.toString() : null).build(entry));
		return v; 
	}
	
	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#getTwoDevices(net.floodlightcontroller.devicemanager.internal.IndexedEntity, net.floodlightcontroller.devicemanager.internal.IndexedEntity)
	 */
	@Override
	public byte[] getTwoDevices(IndexedEntity ieSource,
			IndexedEntity ieDestination){
		byte[] v = table.getTwoDevices(ieSource,ieDestination);
		logEntry(new RequestLogWithDataInformation.Builder().
				setKey(ieSource + " - " + ieDestination).
				setReturnedValue(v != null ? v.toString() : null).build(entry));
		return v; 
	}
	
	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#createDevice(net.floodlightcontroller.devicemanager.internal.Entity)
	 */
	@Override
	public Device createDevice(Entity entity) {
		Device d = table.createDevice(entity); 
		logEntry(new RequestLogWithDataInformation.Builder() 
				.setKey(entity.toString())
				.setReturnedValue(d)
				.build(entry)
				);
		return d; 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IKeyValueTable#updateDevice(java.lang.Long, int, int, long)
	 */
	@Override
	public boolean updateDevice(Long deviceKey, int version, int entityindex,
			long l) {
		Boolean b = table.updateDevice(deviceKey, version, entityindex, l); 
		logEntry(new RequestLogWithDataInformation.Builder() 
		.setKey("key= " + deviceKey +", version= " + version + ", entityIndex = " + entityindex + " , newTimeStamp" + l)
		.setReturnedValue(b)
		.build(entry)
		);
		return b; 
	}
	
}


