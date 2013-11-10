package smartkv.client;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;
import smartkv.server.DataStoreVersion;
import smartkv.server.RequestType;
import bftsmart.tom.ServiceProxy;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;


/*
 * TODO - Clean up the messages. Some require sending sizes of values others don't.
 * 
 * FIXME: Document thread safety. 
 * FIXME: initialize the ServiceProxy.
 * FIXME: documentation of this class.  
 */



public abstract class AbstractDatastoreProxy  implements IDataStoreProxy{
	private static Set<Integer> cids = Sets.newHashSet();
	private ServiceProxy  server; 
	
	private static int cid =1;
	
	//FIXME- There should no byte manipulation in the Datastore classes. Byte manipulation (creating DataStoreValue) should exist in Tables. Where they know what they want to do in order to give values to the users. 
	// Right now you are creating Maps and Sets and shit here, that will simply be put to waste later...
	//Just return byte[] in here.. Let the Tables do their job...
	
	private static Map<Long, ServiceProxy> proxiesByClients = new HashMap<Long,ServiceProxy>(); 
	private static int counter = 0; 
	private static synchronized ServiceProxy createThreadServiceProxy(int id){
		long id2 = Thread.currentThread().getId();
		if (proxiesByClients.containsKey(id2)){
			return proxiesByClients.get(id2); 
		}
		else{
			ServiceProxy toReturn  = new ServiceProxy(counter++);
			proxiesByClients.put(id2, toReturn);
			return toReturn; 
		}
	}
	
	/**
	 * Initialize this proxy with a given client id. 
	 * @param cid The client id used in {@link ServiceProxy} . Remember that is must be globally unique. 
	 */
	protected AbstractDatastoreProxy(int id){
		if (id >=0 ){
		//FIXME
		///TODO have a testing setting that verifies if the same cid is being used, and throws an exception to find out where. 
		
		//server = createThreadServiceProxy(id);
		server = new ServiceProxy(counter++); 
		}
	}
	
	/* 
	 * @see bonafide.datastore.DatastoreProxy#clear()
	 */
	@Override
	public void clear() {
		RequestType type = RequestType.CLEAR_DATASTORE; 
		byte[] request = concatArrays(type.byteArrayOrdinal);
		invokeRequest(type, request);
		return; 
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#createTable(java.lang.String)
	 */
	@Override
	public boolean createTable(String tableName) {
		RequestType type = RequestType.CREATE_TABLE; 
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName));
		byte[] result = invokeRequestWithRawReturn(type, request);
		//Result is null only if table was not created. 
		return result != null;  
	}
	
	
	/* 
	 * @see bonafide.datastore.DatastoreProxy#createTable(java.lang.String, int)
	 */
	@Override
	public boolean createTable(String tableName, long maxSize) {
		RequestType type = RequestType.CREATE_TABLE_MAX_SIZE; 
		
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(maxSize));
		byte[] result = invokeRequestWithRawReturn(type, request);
		//Result is null only if table was not created. 
		return result != null;  
		
	}

	@Override
	public boolean createPointerTable(String tableName, String reference) {
		RequestType type = RequestType.CREATE_POINTER_TABLE;
		
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName),
				getBytes(reference)
				);
		byte[] result = invokeRequestWithRawReturn(type, request);
		//Result is null only if table was not created. 
		return result != null;  
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#removeTable(java.lang.String)
	 */
	@Override
	public boolean removeTable(String tableName) {
		RequestType type = RequestType.REMOVE_TABLE; 
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName));
		byte[] result = invokeRequestWithRawReturn(type, request);
		//Result is null only if table was not removed. 
		return result != null;  
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#containsTable(java.lang.String)
	 */
	@Override
	public boolean containsTable(String tableName) {
		RequestType type = RequestType.CONTAINS_TABLE; 
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName));
		byte[] result = invokeRequestWithRawReturn(type, request);
		//Result is null only if table was not removed. 
		return result != null;  
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#clear(java.lang.String)
	 */
	@Override
	public void clear(String tableName) {
		RequestType type = RequestType.CLEAR_TABLE; 
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName));
	    invokeRequest(type, request);
	}
	

	/* 
	 * @see bonafide.datastore.DatastoreProxy#isEmpty(java.lang.String)
	 */
	@Override
	public boolean isEmpty(String tableName) {
		RequestType type = RequestType.IS_TABLE_EMPTY; 
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName));
		byte[] result = invokeRequestWithRawReturn(type, request);
		return result != null; 
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#containsKey(java.lang.String, byte[])
	 */
	@Override
	public boolean containsKey(String tableName, byte[] key) {
		RequestType type = RequestType.CONTAINS_KEY_IN_TABLE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName), 
				getBytes(key.length), 
				key);
		byte[] result = invokeRequestWithRawReturn(type, request);
		return result != null; 
	}
	
	@Override
	public Map<byte[], DatastoreValue> getTable(String tableName){
		RequestType type = RequestType.GET_TABLE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName) 
				);
		byte[] result = invokeRequestWithRawReturn(type, request); 
		if (result != null){
			Map<byte[], byte[]> map =  UnsafeJavaSerializer.<Map<byte[],byte[]>>getInstance().deserialize(result);
			Map<byte[], DatastoreValue> resultMap = Maps.newHashMap(); 
			for (Entry<byte[], byte[]> en : map.entrySet()){
				resultMap.put(en.getKey(), DatastoreValue.createValue(en.getValue()));
			}
		}
		return null;
	}

	/* 
	 * @see bonafide.datastore.DatastoreProxy#size(java.lang.String)
	 */
	@Override
	public int size(String tableName) {
		RequestType type = RequestType.SIZE_OF_TABLE;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName) 
				); 
		byte[] result = invokeRequestWithRawReturn(type, request);
		return Ints.fromByteArray(result); 
	}

	
	/* 
	 * @see bonafide.datastore.DatastoreProxy#getAndIncrement(java.lang.String, byte[])
	 */
	@Override
	public int getAndIncrement(String tableName, String key) {
		RequestType type = RequestType.GET_AND_INCREMENT;
		byte[] request = concatArrays(
				type.byteArrayOrdinal, 
				getBytes(tableName),
				//FIXME 
				getBytes(key)
				); 
		byte[] result = invokeRequestWithRawReturn(type, request);
		return Ints.fromByteArray(result);
	}
	
	/**
	 * Sends the <b>RPC</b> request to the data store and block waiting for the reply.
	 * <p>
	 * <code>request</code> is sent to the data store can be sent in one of two ways: 
	 * <ul>
	 * <li>Ordered Request  - if <code>type.isWrite()</code>  is <code>true</code>; </li>
	 * <li>Unordered Request - otherwise</li>
	 * </ul>
	 *  <p>
	 *  This method is designed for extension. Classes that wish to extend behavior (such as logging) can subclass this implementation and override this method.
	 *  However it still must be called if the message is to be sent to the data store. <br/>
	 *  
	 *  TODO - set a reference to ordered and unordered request forma specification and explanation. 
	 *  
	 * @param type the type of the request (<b>RPC</b>) message sent. 
	 * @param request the request to be sent to the data store. 
	 * @return the reply from the data store. 
	 */
	
	//TODO - catch exceptions in invokeOrdered/Unordered and throw them as RuntimeExceptions. 
	protected byte[] invokeRequestWithRawReturn(RequestType type, byte[] request){
		byte[] result  = type.isRead() ?server.invokeOrdered(request) : server.invokeUnordered(request);
		return result; 
	}
	
	//TODO - catch exceptions in invokeOrdered/Unordered and throw them as RuntimeExceptions. 
	protected DatastoreValue invokeRequest(RequestType type, byte[] request){
		byte[] result  = invokeRequestWithRawReturn(type,request); 
		return DatastoreValue.createValue(result);  
	}
	

	/**
	 * Concatenates all byte arrays passed as argument.
	 * The method signature forces the client to pass at least one array.
	 * <p>
	 * The final result is a newly formed array: <code>result</code> that is composed if the specified arguments arrays in order of argument passing (left to right).  
	 * @param a0 the first byte array to concatenate 
	 * @param an the remaining byte arrays.   
	 * @returns a newly created array in the form <code>a0:a1:...:an</code>.
	 */
	protected byte[] concatArrays(byte[] a0, byte[]... an){
		//FIXME - urgent : use Guava.concat 
		int len = a0.length; //total length of result array 
		for (byte[] ax : an){
			len += ax.length;
		}
		byte[] version = version().byteArrayOrdinal;
		//Allocate result array
		byte[] result = new byte[len + version.length]; 
		//Copy all argument arrays in order to the result array
		System.arraycopy(version, 0, result, 0, version.length);
		System.arraycopy(a0, 0, result, version.length, a0.length);
		int destPos = a0.length + version.length; 
		for (byte[] ax: an){
			int dstLength = ax.length; //destiny length  
			System.arraycopy(ax, 0, result, destPos, dstLength);
			destPos += dstLength; 
		}
		return result; 		
	}
	
	protected abstract DataStoreVersion version();
	
	protected static  byte[] getBytes(String s){
		//FIXME
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream stream = new DataOutputStream(out);
		try {
			stream.writeUTF(s);
			return out.toByteArray(); 
		} catch (IOException e) {
			throw new RuntimeException("Could not serialize string"); 
			
		}
	}
	
	protected static  byte[] getBytes(int i){
		return ByteBuffer.allocate(4).putInt(i).array();
	}
	
	protected static  byte[] getBytes(short i){
		return ByteBuffer.allocate(2).putShort(i).array();
	}
	
	protected static byte[] getBytes(long l){
		return ByteBuffer.allocate(8).putLong(l).array();
	}
	
	@Override
	public Integer roundRobin(String id){
		byte[] msg = concatArrays(RequestType.LB_ROUND_ROBIN.byteArrayOrdinal, getBytes(id));
		byte [] result = invokeRequestWithRawReturn(RequestType.LB_ROUND_ROBIN, msg); 
		return result != null ?  Serializer.INT.deserialize(result ) : null; 
	}
	
}

	
