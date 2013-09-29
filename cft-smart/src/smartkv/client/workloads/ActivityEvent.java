package smartkv.client.workloads;

import java.io.Serializable;



class PacketIn extends ActivityEvent{

	public PacketIn(EVENT_TYPE t) {
		super(t);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
}

public class ActivityEvent implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6892111080586172396L;
	
	
	public static ActivityEvent addLink(){
		return new ActivityEvent(EVENT_TYPE.LINK_ADDED);
	}
	
	public static ActivityEvent rmLink(){
		return new ActivityEvent(EVENT_TYPE.LINK_REMOVED);
	}
	
	public static ActivityEvent startNetwork(){
		return new ActivityEvent(EVENT_TYPE.START_NETWORK);
	}
	
	public static ActivityEvent stopNetwork(){
		return new ActivityEvent(EVENT_TYPE.STOP_NETWORK);
	}
	
	public static ActivityEvent newTopoInstance(){
		return new ActivityEvent(EVENT_TYPE.NEW_TOPOLOGY_INSTANCE);
	}
	
	public static ActivityEvent newSwitchTurnOff(){
		return new ActivityEvent(EVENT_TYPE.SWITCH_TURN_OFF);
	}
	public static ActivityEvent packetIn(String string){
		return new ActivityEvent(EVENT_TYPE.PACKET_IN, string); 
	}

	public  String p;

	public ActivityEvent(EVENT_TYPE t, String p) {
		this.type = t; 
		this.timeStart = System.currentTimeMillis();
		this.p = p; 
	}

	
	public ActivityEvent(EVENT_TYPE t){
		this.type = t; 
		this.timeStart = System.currentTimeMillis();
	}
	
	//TODO - unsigned int . 
	EVENT_TYPE type; 
	private int start; 
	private int end;
	public long timeStart; 
	public long timeEnd;
	public EVENT_TYPE getType() {
		return type;
	}
	public void setType(EVENT_TYPE type) {
		this.type = type;
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getEnd() {
		return end;
	}
	public void setEnd(int end) {
		this.end = end;
	}
	public long getTimeStart() {
		return timeStart;
	}
	public void setTimeStart(long timeStart) {
		this.timeStart = timeStart;
	}
	public long getTimeEnd() {
		return timeEnd;
	}
	public void setTimeEnd(long timeEnd) {
		this.timeEnd = timeEnd;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	@Override
	public String toString() {
		return "ActivityEvent [type=" + type + ", start=" + start + ", end="
				+ end + ", timeStart=" + timeStart + ", timeEnd=" + timeEnd
				+ "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + end;
		result = prime * result + start;
		result = prime * result + (int) (timeEnd ^ (timeEnd >>> 32));
		result = prime * result + (int) (timeStart ^ (timeStart >>> 32));
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
		ActivityEvent other = (ActivityEvent) obj;
		if (end != other.end)
			return false;
		if (start != other.start)
			return false;
		if (timeEnd != other.timeEnd)
			return false;
		if (timeStart != other.timeStart)
			return false;
		if (type != other.type)
			return false;
		return true;
	}
	public ActivityEvent(EVENT_TYPE type, int start, int end, long timeStart,
			long timeEnd) {
		super();
		this.type = type;
		this.start = start;
		this.end = end;
		this.timeStart = timeStart;
		this.timeEnd = timeEnd;
	}

	
	public static ActivityEvent removingLinks() {
		return new ActivityEvent(EVENT_TYPE.REMOVING_LINKS);
	}
	
	
}
