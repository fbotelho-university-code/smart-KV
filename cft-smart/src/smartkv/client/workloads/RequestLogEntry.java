package smartkv.client.workloads;

import java.io.Serializable;
import java.util.Comparator;

import smartkv.server.RequestType;



public  class RequestLogEntry implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static int serialGenerator = 0;
	public static  Comparator<RequestLogEntry> timeStartedComparisonASC = new Comparator<RequestLogEntry>(){
		public int compare (RequestLogEntry r1, RequestLogEntry r2){
			if (r1.getTimeStarted() < r2.getTimeStarted()) return -1; 
			if (r1.getTimeStarted() == r2.getTimeStarted()) return 0;
			return 1; 
		}
	};
	
	public final int serial; 
	private RequestType type; 
	private long timeStarted; 
	private long timeEnded; 
	private int sizeOfRequest;  
	private int sizeOfResponse;
	
	
	public long getTimeStarted() {
		return timeStarted;
	}

	public void setStrackTrace(){
		StackTraceElement stt[] = Thread.currentThread().getStackTrace();
		tid = Thread.currentThread().getId(); 
		for (int i = 4 ; i < 14  && i < stt.length; i++){
			st[i] = stt[i].toString(); 
		}
	}
	
	public RequestLogEntry(){
		serial = RequestLogEntry.serialGenerator++; 
		initializeFields();
	}

	/**
	 * 
	 */
	public void initializeFields() {
		timeEnded = sizeOfResponse = sizeOfRequest = 0;
		timeStarted = System.currentTimeMillis();
		
	}
	
	public String st[] = new String[14]; 
	public long tid;  
	public RequestLogEntry(long timeStarted, long timeEnded, int sizeOfRequest,
			int sizeOfResponse, RequestType type) {
		serial = RequestLogEntry.serialGenerator++; 			
		this.type = type; 
		this.timeStarted = timeStarted;
		this.timeEnded = timeEnded;
		this.sizeOfRequest = sizeOfRequest;
		this.sizeOfResponse = sizeOfResponse;
		initializeFields(); 
	}

	
	public RequestLogEntry(RequestLogEntry r ){
		serial = RequestLogEntry.serialGenerator++; 			
		type = r.getType(); 
		timeStarted = r.getTimeStarted(); 
		timeEnded = r.getTimeEnded(); 
		sizeOfRequest = r.getSizeOfRequest(); 
		sizeOfResponse = r.getSizeOfResponse();
	}

	public RequestType getType() {
		return type;
	}

	public void setType(RequestType type) {
		this.type = type;
	}

	@Override
	public Object clone(){
		return new RequestLogEntry(this);  
	}

	public void setTimeStarted(long timeStarted) {
		this.timeStarted = timeStarted;
	}
	public long getTimeEnded() {
		return timeEnded;
	}
	public void setTimeEnded(long timeEnded) {
		this.timeEnded = timeEnded;
	}
	public int getSizeOfRequest() {
		return sizeOfRequest;
	}
	public void setSizeOfRequest(int sizeOfRequest) {
		this.sizeOfRequest = sizeOfRequest;
	}
	public int getSizeOfResponse() {
		return sizeOfResponse;
	}
	public void setSizeOfResponse(int sizeOfResponse) {
		this.sizeOfResponse = sizeOfResponse;
	}



	@Override
	public String toString() {
		return "RequestLogEntry [serial=" + serial + ", type=" + type
				+ ", timeStarted=" + timeStarted + ", timeEnded=" + timeEnded
				+ ", sizeOfRequest=" + sizeOfRequest + ", sizeOfResponse="
				+ sizeOfResponse + "]";
	}

	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + serial;
		result = prime * result + sizeOfRequest;
		result = prime * result + sizeOfResponse;
		result = prime * result + (int) (timeEnded ^ (timeEnded >>> 32));
		result = prime * result
				+ (int) (timeStarted ^ (timeStarted >>> 32));
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RequestLogEntry other = (RequestLogEntry) obj;
		if (serial != other.serial)
			return false;
		if (sizeOfRequest != other.sizeOfRequest)
			return false;
		if (sizeOfResponse != other.sizeOfResponse)
			return false;
		if (timeEnded != other.timeEnded)
			return false;
		if (timeStarted != other.timeStarted)
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	public void endedNow() {
		this.timeEnded = System.currentTimeMillis(); 
	}
}