/**
 * 
 */
package smartkv.micro.dm;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;

import net.floodlightcontroller.devicemanager.IDeviceService.DeviceField;
import net.floodlightcontroller.devicemanager.IEntityClass;
import net.floodlightcontroller.devicemanager.IEntityClassifierService;
import net.floodlightcontroller.devicemanager.internal.AttachmentPoint;
import net.floodlightcontroller.devicemanager.internal.Device;
import net.floodlightcontroller.devicemanager.internal.DeviceMultiIndex;
import net.floodlightcontroller.devicemanager.internal.DeviceUniqueIndex;
import net.floodlightcontroller.devicemanager.internal.Entity;
import net.floodlightcontroller.devicemanager.internal.IndexedEntity;
import smartkv.client.ColumnProxy;
import smartkv.client.KeyValueProxy;
import smartkv.client.tables.CachedKeyValueTable;
import smartkv.client.tables.ColumnObject;
import smartkv.client.tables.ColumnTable_;
import smartkv.client.tables.IKeyValueTable;
import smartkv.client.tables.KeyValueTable_;
import smartkv.client.tables.TableBuilder;
import smartkv.client.tables.VersionedValue;
import smartkv.client.util.Serializer;
import smartkv.client.util.UnsafeJavaSerializer;
import smartkv.server.MapSmart;
import smartkv.server.RequestType;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

/**
 * @author fabiim
 *
 */
public class Operations {
	
	final DeviceUniqueIndex primaryIndex;  //Wrapper, for the table
	final DeviceMultiIndex index; 
	final protected IEntityClassifierService entityClassifier;
	//final IEntityClass entityClass; 
	final IKeyValueTable<Long, Device> deviceMap; 
	

	
	@SuppressWarnings("unchecked")
	public Operations(final MapSmart m){
		
		entityClassifier = new net.floodlightcontroller.devicemanager.internal.DefaultEntityClassifier();
		ColumnObject  column = smartkv.client.tables.AnnotatedColumnObject.<Device>newAnnotatedColumnObject(Device.class);
		deviceMap = new ColumnTable_(new TableBuilder<Long,Device>().setTableName("DMAP").setKeySerializer(Serializer.LONG).setColumnSerializer(column)
				.setProxy(new ColumnProxy(-1){
        			@Override
        			protected byte[] invokeRequestWithRawReturn(RequestType type,
        					byte[] request) {
        				return m.execute(request);
        			}}));		


		primaryIndex = new DeviceUniqueIndex(entityClassifier.getKeyFields(),CachedKeyValueTable.startCache(new KeyValueTable_(new TableBuilder<IndexedEntity,Long>().setTableName("DEVICE_UNIQUE_INDEX")
	     		   .setKeySerializer(IndexedEntity.SERIALIZER)
	     		   .setValueSerializer(Serializer.LONG).setCrossReferenceColumnSerializer(column)
	     		   .setCrossReferenceTable("DMAP").setProxy(new KeyValueProxy(-1){
	     			   
	     	@Override
			protected byte[] invokeRequestWithRawReturn(RequestType type,
					byte[] request) {
				return m.execute(request);
			}
		})
	     		   )));
		index =  new DeviceMultiIndex(EnumSet.of(DeviceField.IPV4),  new KeyValueTable_(new TableBuilder<IndexedEntity, HashSet<Long>>()
        		.setTableName("MULTI_INDEX")
        		.setKeySerializer(IndexedEntity.SERIALIZER)

        		.setProxy(new KeyValueProxy(-1){
        			@Override
        			protected byte[] invokeRequestWithRawReturn(RequestType type,
        					byte[] request) {
        				return m.execute(request);
        			}}
        		)) );
		
	}

	protected byte[] updateDeviceTimestamp(long device, int ts, int entityindex , long lastSeen){
		VersionedValue<Device> v = deviceMap.getWithTimeStamp(device); 
		if ( v == null || v.version() != ts ) return null;
        // Entity already exists
        // update timestamp on the found entity
		Entity[] entities = v.value().getEntities(); 
        entities[entityindex].setLastSeenTimestamp(new Date(lastSeen));
        if (deviceMap.replace(device, ts, v.value())){
        	return new byte[1];
        }
        else return null; 
	}
	
	public byte[] createDevice(DataInputStream dis) throws IOException, ClassNotFoundException{
		return createDevice((Entity) UnsafeJavaSerializer.getInstance().deserialize(ByteStreams.toByteArray(dis)));
	}
	
	protected byte[] createDevice(Entity entity){
        long deviceKey = (long) deviceMap.getAndIncrement("DM_ID");
		IEntityClass entityClass = entityClassifier.classifyEntity(entity);
		
        Device device =  new Device(deviceKey, entity, entityClass);
        
        // Add the new device to the primary map with a simple put
        deviceMap.insert(deviceKey, device);
        boolean updatePrimary = primaryIndex.updateIndex(device, deviceKey) ; //if false then it should go to deleteQueue  

        index.updateIndex(entity, deviceKey);
        return UnsafeJavaSerializer.getInstance().serialize(device); 
	}
	
	
	/**
	 * @param dis
	 * @return
	 */
	public byte[] updateDevice(DataInputStream dis) throws Exception{
		byte[] ints = new byte[4]; 
		byte[] longs = new byte[8]; 
		dis.readFully(longs);
		long deviceKey = Longs.fromByteArray(longs); 
		dis.readFully(ints); 
		int version = Ints.fromByteArray(ints); 
		dis.readFully(ints);
		int entityIndex = Ints.fromByteArray(ints);
		dis.readFully(longs); 
		long lastSeen = Longs.fromByteArray(longs); 
		return updateDeviceTimestamp(deviceKey,version, entityIndex , lastSeen); 
	}

	/**
	 * @param dis
	 * @return
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	/*public  byte[] twoDevice(DataInputStream dis) throws IOException, ClassNotFoundException {
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		ObjectOutputStream oo= new ObjectOutputStream(bo);

		Entity source, destination = null; 
		ObjectInputStream oi = new ObjectInputStream(dis);
		source = (Entity) oi.readObject();

		if (oi.available() >0 ){
			destination = (Entity) oi.readObject(); 
		}
		
		VersionedValue<Object> sd = this.primaryIndex.findDeviceByEntity(source);
		if (sd != null){
			oo.writeObject(sd);
		}
		List<AttachmentPoint> list = null ;
		if (destination != null){
			VersionedValue<Object> d = this.primaryIndex.findDeviceByEntity(destination);
			if (d!= null ){
				List<AttachmentPoint> aps = ((Device) d.value()).getAps(); 
				if (aps != null){
					oo.writeObject(aps);
				}
			}
		}
		if (list != null || sd != null){
			return bo.toByteArray(); 
		}
		return null; 
	}*/
	
	public  byte[] twoDevice(DataInputStream dis) throws IOException, ClassNotFoundException {
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		ObjectOutputStream oo= new ObjectOutputStream(bo);
		
		Entity source = null; 
		Entity destination = null;
		
		byte[] macSource = new byte[8];
		dis.readFully(macSource);
		long macSourceValue = Longs.fromByteArray(macSource); 
		byte[] vlanSource = new byte[2];
		dis.readFully(vlanSource);
		short vl = Shorts.fromByteArray(vlanSource); 
		source = new Entity(macSourceValue, vl,null,null,null,null); 
		if (dis.available() >0 ){
			byte[] macDest = new byte[8];
			dis.readFully(macDest);
			long macDestValue = Longs.fromByteArray(macSource); 
			byte[] vlanDest = new byte[2]; 
			dis.readFully(vlanDest);
			vl = Shorts.fromByteArray(vlanSource);
			destination = new Entity(macDestValue, vl,null,null,null,null); 
		}
		VersionedValue<Object> sd = this.primaryIndex.findDeviceByEntity(source);
		if (sd != null){
			oo.writeObject(sd);
		}
		List<AttachmentPoint> list = null ;
		if (destination != null){
			VersionedValue<Object> d = this.primaryIndex.findDeviceByEntity(destination);
			if (d!= null ){
				List<AttachmentPoint> aps = ((Device) d.value()).getAps(); 
				if (aps != null){
					oo.writeObject(aps);
				}
			}
		}
		if (list != null || sd != null){
			return bo.toByteArray(); 
		}
		return null; 
	}
}
