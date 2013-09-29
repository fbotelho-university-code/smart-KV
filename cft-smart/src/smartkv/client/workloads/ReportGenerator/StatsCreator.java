package smartkv.client.workloads.ReportGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.math3.stat.Frequency;

import smartkv.client.workloads.EVENT_TYPE;
import smartkv.client.workloads.RequestLogEntry;
import smartkv.server.RequestType;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;


 

public class StatsCreator {
	
	public static Stats requestSize(WorkLoadResults rs){
		return createStat(rs.getRequestLog(), WorkLoadResults.sizeOfRequest, null, "Request Size", "Size (bytes) of the messages sent to the database",rs );
	}
	
	public static Stats responseSize(WorkLoadResults rs){
		return createStat(rs.getRequestLog(), WorkLoadResults.sizeOfResponse, null, "Response Size" , "Size (bytes) of the messages received from the database",rs);
	}
	
	public static Stats writeRequestSize(WorkLoadResults rs){
		Stats s =  createStat(rs.getWrites(), WorkLoadResults.sizeOfRequest, null, "WRITE Request Size" , "Size (bytes) of the write operations messages sent to the datastore",rs);
		s.cdfTitle = "Write Request Size"; 
		return s;
	}
	
	public static Stats writeResponseSize(WorkLoadResults rs){
		return createStat(rs.getWrites(), WorkLoadResults.sizeOfResponse, null, "WRITE Response Size" , "Size (bytes) of the messages received from the datastore in  reply to writes",rs);
	}
	public static Stats readRequestSize(WorkLoadResults rs){
		return createStat(rs.getReads(), WorkLoadResults.sizeOfRequest, null, "READ Request Size" , "Size (bytes) of the read operations messages sent to the datastore",rs);
	}
	
	public static Stats readResponseSize(WorkLoadResults rs){
		Stats s = createStat(rs.getReads(), WorkLoadResults.sizeOfResponse, null, "READ Response Size" , "Size (bytes) of the messages received from the datastore in  reply to reads", rs);
		s.cdfTitle = "Read Response Size"; 
		return s; 
	}
	
	/**
	 * For a given stat this method will get the stats per / method type. 
	 * @param usedStat
	 * @return
	 */
	public static List<Stats> getPerMethodStats(Stats usedStat){
		List<Stats> stats = Lists.newArrayList();
		for (RequestType t : RequestType.values()){
			if (Iterators.any(usedStat.entriesUsed.iterator(), new WorkLoadResults.ContainsType(t))){
				stats.add(createStat(usedStat.entriesUsed, usedStat.fooUsed, new WorkLoadResults.ContainsType(t), t.name(), t.description, usedStat.resultsUsed));
			}
		}
		return stats; 
	}
	
	/**
	 * Get the frequency of method invocations of the database for a given stat. 
	 * @param usedStat
	 * @return
	 */
	public static Frequency getMethodCallStat(Stats usedStat){
		Frequency freq = new Frequency();
		for (RequestLogEntry en : usedStat.entriesUsed){
			freq.addValue(en.getType().name());
		}
		return freq; 
	}
	
	
	public static List<Stats> perEventStats(Stats usedStat){
		WorkLoadResults filteredRequestLogWr = new WorkLoadResults(usedStat.entriesUsed, usedStat.resultsUsed.getActivityLog()); 
		Multimap<EVENT_TYPE, Collection<RequestLogEntry>> mmap = filteredRequestLogWr.getLogGroupedByEventType();
		List<Stats> result = Lists.newArrayList(); 
		for (EVENT_TYPE t : EVENT_TYPE.values()){
			if (mmap.containsKey(t)){
				Stats stats = new Stats(t.name(), t.toString(), new ArrayList<RequestLogEntry>(),  usedStat.fooUsed, usedStat.resultsUsed);
				
				for (Collection<RequestLogEntry> vals : mmap.get(t)){
					double v = 0;
					for (RequestLogEntry r : vals){
						v += stats.fooUsed.apply(r);
					}
					stats.freq.addValue(v);
				}
				result.add(stats);
			}
		}
		return result;
	}
	
	
	private static Stats createStat(Collection<RequestLogEntry> entries, Function<RequestLogEntry, Double> getValueFoo, Predicate<RequestLogEntry> predicate, String title, String description, WorkLoadResults resultsUsed){
		Stats stats = new Stats(title,description, new ArrayList<RequestLogEntry>(), getValueFoo, resultsUsed);
		for (RequestLogEntry entry : entries){
			if (predicate == null || predicate.apply(entry)){
				addValueToStat(stats, entry);
				
			}
		}
		return stats; 
	}

	/**
	 * @param getValueFoo
	 * @param stats
	 * @param entry
	 */
	private static void addValueToStat(Stats stats,
			RequestLogEntry entry) {
		stats.entriesUsed.add(entry); 
		double value = stats.fooUsed.apply(entry); 
		stats.freq.addValue( value); 
		stats.dstats.addValue(value);
	}
	
}



