package smartkv.client.workloads;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.google.common.collect.Lists;



public class RequestLogger {
	public static final String SERIALIZED = ".objects";
	public static final String TXT = ".string";
	static private RequestLogger instance = null;
	
	public synchronized static void startRequestLogger(String path){
		if (instance == null){
			instance = new RequestLogger(path);
			
			// Activate server to receive events from mininet 			
			new Thread( new WebService(instance)).start();
			
			System.out.println("Starting request logger"); 

				System.out.println("Adding shutdown hook");
				//Add shutdown hook to save file 
			Runtime.getRuntime().addShutdownHook( 
		    		new Thread(
		    			new Runnable() {
		    				public void run() {
		    					try {
									instance.end("ctrlc");
								} catch (Throwable e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								} 
		    					System.out.println("RUn shutdown hook"); 
		    				}	
		    			}
		    		)
		    	);
		}
	}
	
	
	public synchronized static RequestLogger getRequestLogger(){
		return instance; 
	}
	
	
	private Queue<RequestLogEntry> operations;
	private Queue<ActivityEvent> events; 
	private String fileOutput; 
	
	
	private RequestLogger(String fileOutput){
		this.fileOutput = fileOutput; 
		operations = new LinkedList<RequestLogEntry>();
		events = new LinkedList<ActivityEvent>(); 
	}
	
	private int last = 0; 
	public synchronized void addRequest(RequestLogEntry r){
		if (r.getType() == null){
			try{ throw new Exception(); }catch(Exception e){e.printStackTrace(System.out);} 
			System.exit(-1); 
		}
		operations.add(r);
		last = r.serial; 
	}
	
	public synchronized ActivityEvent  addActivity(ActivityEvent e){
		e.setStart(last + 1);
		events.add(e);
		return e; 
	}
	
	public synchronized void endActivity(ActivityEvent e){
		e.setEnd(last);
		e.setTimeEnd(System.currentTimeMillis()); 
	}
	
	public synchronized List<ActivityEvent> getEvents() {
		return Lists.newArrayList( events);
	}

	
	public synchronized List<RequestLogEntry> getOperations() {
		return Lists.newArrayList(operations);
	}
	
	public synchronized void end(String s){
		writeRequestToFileSerialized(s);
		//writeRequestToFileInText() // NEVER TESTED!
	}
	
	private synchronized void  writeRequestToFileSerialized(String ss){
		try {
			
			String s = this.fileOutput + SERIALIZED + ss; 
			System.out.println("File outputed: " + s);
			File f = new File(s);
			if (f.exists()){
				f.delete(); 
			}
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(s,true));
			
			out.writeObject(operations);
			out.writeObject(events);
			System.out.println("Saved :" + operations.size() + " requests And : "+ events.size() +"events."); 
			out.flush(); 
			out.close(); 
		} catch (FileNotFoundException e) {
			System.err.println("Could not save the requests to file");
			e.printStackTrace(System.err); 
		} catch (IOException e) {
			System.err.println("Could not save the requests to file");
			e.printStackTrace(System.err); 
		} 
	}
	
	
	/*private void writeRequestToFileInText() {
		//TODO not writing events 
		StringBuilder s = new StringBuilder();
		Joiner joiner = Joiner.on(';').useForNull("null");

		for (RequestLogEntry r : operations){
			joiner.join(r.getType(), 
					r.getTimeStarted(), r.getTimeEnded(),
					r.getSizeOfRequest(), r.getSizeOfResponse()); 	
		}
		try {
			DataOutputStream out = new DataOutputStream(new FileOutputStream(fileOuput + TXT,true));
			out.writeUTF(s.toString());
			out.close(); 
		} catch (FileNotFoundException e) {
			System.err.println("Could not save the requests to file");
			e.printStackTrace(System.err); 
		} catch (IOException e) {
			System.err.println("Could not save the requests to file");
			e.printStackTrace(System.err); 
		}  
	}*/
	
	
}



