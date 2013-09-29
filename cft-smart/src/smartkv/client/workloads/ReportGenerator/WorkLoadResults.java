package smartkv.client.workloads.ReportGenerator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import smartkv.client.workloads.ActivityEvent;
import smartkv.client.workloads.EVENT_TYPE;
import smartkv.client.workloads.RequestLogEntry;
import smartkv.client.workloads.RequestLogWithDataInformation;
import smartkv.server.RequestType;
import smartkv.server.RequestType.SuperType;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;


public class WorkLoadResults {

	
	/**
	 * Help class to define predicates filtering RequesLogEntries based on their types. 
	 *
	 */
	public static final class  ContainsType implements Predicate<RequestLogEntry>{

		RequestType type=null;
		SuperType superType=null;
		
		public ContainsType(RequestType t){
			type = t;
		}
		public ContainsType(SuperType t){
			superType = t; 
		}
		@Override
		public boolean apply(RequestLogEntry input) {
			return  type == null ? superType == input.getType().type : type == input.getType();  
		}
	};
	
	public static Function<RequestLogEntry, Double> sizeOfRequest = new Function<RequestLogEntry, Double>(){
		@Override
		public Double apply(RequestLogEntry in){
			return new Long(in.getSizeOfRequest()).doubleValue(); 
		}
	}; 
	
	public static Function<RequestLogEntry, Double> sizeOfResponse = new Function<RequestLogEntry, Double>(){
		@Override
		public Double apply(RequestLogEntry in){
			return new Long(in.getSizeOfResponse()).doubleValue(); 
		}
	};
	private List<RequestLogEntry> requestLog;
	private List<ActivityEvent> activityLog;
	
	
	public WorkLoadResults(String pathToFile){
		deserializeResults(pathToFile);
	}
	
	public WorkLoadResults(List<RequestLogEntry> requestLog,
			List<ActivityEvent> activityLog2) {
		this.requestLog = requestLog; 
		this.activityLog = activityLog2; 
	}

	
	public List<RequestLogEntry> getRequestLog() {
		return requestLog;
	}
	public void setRequestLog(List<RequestLogEntry> requestLog) {
		this.requestLog = requestLog;
	}
	public List<ActivityEvent> getActivityLog() {
		return activityLog;
	}
	public void setActivityLog(List<ActivityEvent> activityLog) {
		this.activityLog = activityLog;
	}
	

	/*
	 * Helpfull gets.
	 */
	
	public List<RequestLogEntry> getWrites(){
		return filter( SuperType.WRITE);
	}
	public List<RequestLogEntry> getReads(){
		return filter( SuperType.READ);
	}

	//TODO - refactor this method, use the static class to filter . 
	public List<RequestLogEntry> filter(final SuperType t){
		return Lists.newArrayList(Iterators.filter(requestLog.iterator(), new Predicate<RequestLogEntry>(){
			@Override
			public boolean apply(RequestLogEntry input) {
				return input.getType().type == t; 
			}
		}));
	}
	
	
	public ListMultimap<ActivityEvent, RequestLogEntry> getLogGroupedByEvents(){
		ListMultimap<ActivityEvent, RequestLogEntry> result = ArrayListMultimap.create();
		for (final ActivityEvent event : activityLog){
			List<RequestLogEntry> requestsOfEvent = Lists.newArrayList(
			Iterators.filter(requestLog.iterator(), new Predicate<RequestLogEntry>(){
				@Override
				public boolean apply(RequestLogEntry op) {
					return op.serial >= event.getStart() && op.serial <= event.getEnd();    
				}
			}));
			result.putAll(event, requestsOfEvent);
		}
		return result;
	}
	
	
	/**
	 * The List inside the multimap is not a bug!
	 * @return
	 */
	public ListMultimap<EVENT_TYPE, Collection<RequestLogEntry>> getLogGroupedByEventType(){
		ListMultimap<ActivityEvent, RequestLogEntry> groupedByActivities = getLogGroupedByEvents();
		ListMultimap<EVENT_TYPE, Collection<RequestLogEntry>> results = ArrayListMultimap.create(); 

		for (Entry<ActivityEvent, Collection<RequestLogEntry>> en : groupedByActivities.asMap().entrySet()){
			results.put(en.getKey().getType(), en.getValue());
		}
		return results;
	}
	
	public List<List<RequestLogEntry>> getRequestsPerIntervalWithEmptyIntervals(final long interval, final SuperType type){
		List<RequestLogEntry> requests;
		requests = filterRequestsByType(type);
		
		Collections.sort(requests, RequestLogEntry.timeStartedComparisonASC);
		List<List<RequestLogEntry>> requestsByInterval = new ArrayList<List<RequestLogEntry>>();
		//Get first result ; 
		if (requests.size() > 0){
			{
				PeekingIterator<RequestLogEntry> it = Iterators.peekingIterator(requests.iterator());
				List<RequestLogEntry> intervalList = Lists.newArrayList();
				RequestLogEntry timeStart = requests.get(0);
				long timeEnded = timeStart.getTimeStarted() + interval;
				requestsByInterval.add(intervalList);
				for (; it.hasNext() ; intervalList = Lists.newArrayList(), timeEnded += interval, requestsByInterval.add(intervalList)){
					while (it.hasNext() && it.peek().getTimeEnded() <= timeEnded){
						intervalList.add(it.next());
					}
				}
			}
		}
		
		return requestsByInterval;
	}
	
	public long getTimeZero(){
		List<RequestLogEntry> l=Lists.newArrayList(requestLog);
		Collections.sort(l, RequestLogEntry.timeStartedComparisonASC);
		//XXx check if 0 exists. 
		return l.get(0).getTimeStarted();
	}
	
	/////////////PRIVATE///////////////////////
	
	@SuppressWarnings("unchecked")
	
	private void deserializeResults(String path) {
		
		try {
			requestLog = new LinkedList<RequestLogEntry>(); 
			activityLog = new LinkedList<ActivityEvent>(); 
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(path));
				try {
					requestLog= (List<RequestLogEntry>) in.readObject();
					activityLog = (List<ActivityEvent>) in.readObject();
				} catch (ClassNotFoundException e){
					System.err.println(path + "Contains strange data");
				}
				
				List<RequestLogEntry> requestLogFinal = Lists.newArrayList();
				
				/*int i=0;
				int j=0;
				for (ActivityEvent en : activityLog){
					if (en.getType().equals(EVENT_TYPE.LINK_REMOVED)){
						i++;
					}
					if (en.getType().equals(EVENT_TYPE.LINK_ADDED)){
						j++;
					}
				}
				System.out.println("link removals: " + i); 
				System.out.println("link additions: " + j);*/ 
				long timeZero = getTimeZero();
				
/*				List<List<RequestLogEntry>> enPeSec =  this.getRequestsPerIntervalWithEmptyIntervals(1000, SuperType.READ); 
				long max = 0; 
				int index = 0;
				for (int j = 500 ;  j < 750 ; j++){
					for (RequestLogEntry en  : enPeSec.get(j)){
						if (en.getType().equals(RequestType.GET_TABLE) && en.getSizeOfResponse() > 1000){
							if (en.getSizeOfResponse() > max ){
								max = en.getSizeOfResponse();
								index = j;
							}
								System.out.println(j + " -> " + en.getSizeOfResponse()); 
						}
					}
				}
				System.out.println("Max of " + max + " @"  + index); 
				System.exit(0); 
				*/
				/*
				 // TopologyManager. 
				 * for (RequestLogEntry en : requestLog){
					long time = (en.getTimeStarted() - timeZero); 
					if ( time > 1000 * 625 && time < 1000 * 627 && en.getType().isRead()){
						System.out.println(en);						
					}
				}*/
				
	
				/*//DeviceManager
				 * for (RequestLogEntry en : requestLog){
				if (//(en.getTimeStarted() - timeZero) > (250 * 1000) && 
						(en.getTimeStarted()- timeZero) < (1250 * 1000)){
					requestLogFinal.add(en);
				}
			}*/
			
	
				/*for (RequestLogEntry en : requestLog){
				if ((en.getTimeStarted() - timeZero) > (250 * 1000) && 
						(en.getTimeStarted()- timeZero) < (1100 * 1000)){
					requestLogFinal.add(en);
				}
			}*/
			System.out.println();
			System.out.println(requestLog.size()); 
			int cenas=0; 
			motherfucker: 
			for (RequestLogEntry en : requestLog){
				if ((en.getTimeStarted() - timeZero) > (ReportGenerator.BEGIN * 1000)
					&& (en.getTimeStarted() - timeZero) <= (ReportGenerator.END * 1000)
						){
					
					if (en instanceof RequestLogWithDataInformation){
						//TODO- i don't think there is a dummy table. 
						if (((RequestLogWithDataInformation) en).getTable().equals("DUMMY"))
							continue; 
					}
					/*for (String  e : en.st){
						if (e != null && e.matches(".*toString.*"))
							continue motherfucker; /// Ignore requests made to the data store triggered inside toString()   
					}*/
					cenas++; 
					requestLogFinal.add(en);
				}
			}
			System.out.println(cenas); 
			List<ActivityEvent> ev = Lists.newArrayList();
			for (ActivityEvent e : this.activityLog){
				if (e.getStart() >= requestLogFinal.get(0).serial){
					ev.add(e);
				}
			}
			
			activityLog = ev; 
			requestLog = requestLogFinal;


		} catch (FileNotFoundException e) {
			System.err.println("Could not read file: " + path + " which contains logged request data");
			e.printStackTrace(System.err);
			System.exit(-1);
		} catch (IOException e) {
			System.err.println("Could not read file: " + path + " which contains logged request data");
			e.printStackTrace(System.err);
			System.exit(-1); 
		}
	}
	
	
	/**
	 * @param sourceLog
	 * @param type
	 * @return
	 */
	private List<RequestLogEntry> filterRequestsByType(final SuperType type) {
		List<RequestLogEntry> requests;
		requests = new LinkedList<RequestLogEntry>();  
		for (RequestLogEntry r: requestLog){
			if (r.getType().type == type){
				requests.add(r);
			}
		}
		return requests;
	}
	
	
	private void setEnd(){
		for (int i= 0; i < activityLog.size()  -1; i++ ){
			activityLog.get(i).setTimeEnd(activityLog.get(i+1).getTimeStart());
		}
	}
	
	
	private static double getTreeTopoNetworkSizeFromPredefinedFileName(String name) {
		String[] s = name.split("logs.objects.");
		String values = s[1];
		String[] numbers  = values.split("\\.");

		int depth, fanout; 
		depth = Integer.parseInt(numbers[0]);
		fanout = Integer.parseInt(numbers[1]);
		
		return  ((Math.pow(fanout,depth) - 1)/(fanout -1));  
	}
}
