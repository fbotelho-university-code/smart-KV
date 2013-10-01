package smartkv.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import smartkv.server.experience.unmarshallRequests.KeyValueColumnStoreRpc;
import smartkv.server.experience.unmarshallRequests.KeyValueStoreRPC;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;


public class MapSmart extends DefaultSingleRecoverable{
	
	//TODO extract these methods to a standalone package. 
	public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        o.flush(); 
        o.close(); 
        return b.toByteArray();
    }
	
    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
    
    private Datastore keyValue = new KeyValueStoreRPC(true); 
	private ColumnDatastore columns = new KeyValueColumnStoreRpc(true); 
	public static void main(String[] args){
		new MapSmart(0);
		new MapSmart(1);
		new MapSmart(2);
		new MapSmart(3);
		 
		/*File f = new File("./config/currentView");
		if (f.exists()){
			f.delete();
		}*/
	}
	
	 ServiceReplica replica = null;
     @SuppressWarnings("unused")
     private ReplicaContext replicaContext;
     
     private Logger log = LoggerFactory.getLogger(MapSmart.class);
     
     public MapSmart(int id) {
             replica = new ServiceReplica(id, this, this);
     }
     
	@Override
	public void setReplicaContext(ReplicaContext replicaContext) {
		this.replicaContext = replicaContext;
	}
	
	@Override
	public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
	 return execute(command); 
	//TODO cleanup.		 
	}
	
	@Override
	public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
		
		 return execute(command);
	}

	/**
	 * @param command
	 * @return
	 */
	private byte[] execute(byte[] command) {
		 //FIXME: Proper serialization/deserialization of messages.
		ByteArrayInputStream in = new ByteArrayInputStream(command);
		 try {
			 DataInputStream dis = new DataInputStream(in);
			 //XXX bad in so many different ways... :/
			 //byte[] version_b = new byte[4];
			 //dis.read(version_b);
			 DataStoreVersion version = DataStoreVersion.values()[dis.readInt()];
			 Datastore ds = version == DataStoreVersion.COLUMN_KEY_VALUE ? this.columns : this.keyValue; 
			 RequestType reqType = RequestType.values()[dis.readInt()];
			 //XXX- remember to convince yourself again that this shouldn't go in the enum itfself.
			 
			 switch(reqType){
			 case GET_AND_INCREMENT:
				 return ds.get_and_increment(dis); 
			 case CREATE_TABLE:
				 return ds.create_table(dis);
			 case CREATE_TABLE_MAX_SIZE: 
				 return ds.create_table_max_size(dis); 
			 case REMOVE_TABLE: 
				 return ds.remove_table(dis); 
			 case CONTAINS_TABLE: 
				 return ds.contains_table(dis); 
			 case CLEAR_DATASTORE: 
				 ds.clear(); 
				 return null; 
			 case CLEAR_TABLE: 
				 return ds.clear_table(dis); 
			 case CONTAINS_KEY_IN_TABLE: 
				 return ds.contains_key_in_table(dis);
			 case GET_TABLE: 
				 return ds.get_table(dis);
			 case GET_VALUE_IN_TABLE: 
				 return ds.get_value_in_table(dis); 
			 case IS_DATASTORE_EMPTY: 
				 return ds.is_datastore_empty();
			 case IS_TABLE_EMPTY: 
				 return ds.is_table_empty(dis);
			 case PUT_VALUE_IN_TABLE:
				 return ds.put_value_in_table( dis);
			 case INSERT_VALUE_IN_TABLE:
				 return ds.insert_value_in_table(dis);
			 case PUT_VALUES_IN_TABLE: 
				 return ds.put_Values_in_table(dis);
			 case REMOVE_VALUE_FROM_TABLE: 
				 return ds.remove_value_from_table(dis);
			 case SIZE_OF_TABLE: 
				 return ds.size_of_table(dis);
			 case ATOMIC_REPLACE_VALUE_IN_TABLE:
				return ds.atomic_replace_value_in_table(dis);
			 case ATOMIC_REMOVE_IF_VALUE: 
				return ds.atomic_remove_if_value(dis);
		    case ATOMIC_PUT_IF_ABSENT: 
				return ds.atomic_put_if_absent(dis);
			case GET_COLUMN:
				if (version != DataStoreVersion.COLUMN_KEY_VALUE) throw new UnsupportedOperationException("This is operation is not supported for the specified version");
				return this.columns.get_column(dis);
			case SET_COLUMN:
				if (version != DataStoreVersion.COLUMN_KEY_VALUE) throw new UnsupportedOperationException("This is operation is not supported for the specified version");
				return this.columns.put_column(dis);
			 case VALUES: 
				 return ds.values(dis);
			case CREATE_POINTER_TABLE:
				return ds.create_pointer_table(dis); 
			case GET_COLUMN_BY_REFERENCE:
				if (version != DataStoreVersion.COLUMN_KEY_VALUE) throw new UnsupportedOperationException("This is operation is not supported for the specified version");
				return this.columns.get_column_by_reference(dis);
			case GET_VALUE_IN_TABLE_BY_REFERENCE:
				return ds.get_referenced_value(dis);
			default:
				break;
			 }
		 } catch (IOException e) {
			 System.err.println("Exception reading data in the replica: " + e.getMessage());
			 e.printStackTrace();
			 return null;
		 } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void installSnapshot(byte[] state) {
		
		 ByteArrayInputStream bis = new ByteArrayInputStream(state);
         try {
                 ObjectInputStream in = new ObjectInputStream(bis);
                 this.columns = (ColumnDatastore) in.readObject();
                 this.keyValue = (Datastore) in.readObject();
                 in.close();
                 bis.close();
         } catch (ClassNotFoundException e) {
                 System.err.print("Coudn't find Map: " + e.getMessage());
                 e.printStackTrace();
         } catch (IOException e) {
                 System.err.print("Exception installing the application state: " + e.getMessage());
                 e.printStackTrace();
         }
		
	}

	@Override
	public byte[] getSnapshot() {
		try {
             ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos);
             out.writeObject(this.columns);
             out.writeObject(this.keyValue);
             out.flush();
             out.close();
             bos.close();
             return bos.toByteArray();
     } catch (IOException e) {
             System.out.println("Exception when trying to take a + " +
                             "snapshot of the application state" + e.getMessage());
             e.printStackTrace();
             return null; //FIXME
     }	// TODO Auto-generated method stub
     
	
	}


}

