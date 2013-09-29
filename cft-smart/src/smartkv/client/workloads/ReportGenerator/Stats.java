package smartkv.client.workloads.ReportGenerator; 

import java.util.List;

import org.apache.commons.math3.stat.Frequency;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import smartkv.client.workloads.RequestLogEntry;

import com.google.common.base.Function;

public class Stats{
	private String title; 
	private String description;
	public final DescriptiveStatistics dstats = new DescriptiveStatistics(); 
	public final Frequency freq = new Frequency();
	public final List<RequestLogEntry> entriesUsed; 
	public final WorkLoadResults resultsUsed; 
	public final Function<RequestLogEntry, Double> fooUsed;
	int c = 0xE2; 
	public String cdfTitle;
	public String x = "Sizes (kB)"; 
	public String y = "P[x <= X]";
	
	
	public Stats(String title2, String description2, List<RequestLogEntry> list, Function<RequestLogEntry, Double> fooUsed, WorkLoadResults resultsUsed){
		title = title2; 
		description = description2; 
		entriesUsed = list; 
		this.fooUsed = fooUsed; 
		this.resultsUsed = resultsUsed; 
	}
	
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
}

class SimpleStat{
	public final DescriptiveStatistics ds = new DescriptiveStatistics(); 
	public final Frequency freq = new Frequency();
	
}