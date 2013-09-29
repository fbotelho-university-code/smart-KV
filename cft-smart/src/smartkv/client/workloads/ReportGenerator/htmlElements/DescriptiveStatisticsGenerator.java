package smartkv.client.workloads.ReportGenerator.htmlElements;


import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.collect.Lists;

public class DescriptiveStatisticsGenerator extends TableGenerator{
		
	public DescriptiveStatisticsGenerator(String title, String description, DescriptiveStatistics  stats) {
		super(title, description);
		columns = Lists.newArrayList("Stat", "Value");
		lines = Lists.newArrayList("Total", "Min", "Max", "Mean", "Std Dev", "Median");
		double[] values = {stats.getN(), stats.getMin(), stats.getMax(), stats.getMean(),stats.getStandardDeviation(), stats.getPercentile(50)};
		tableContents = Lists.newArrayList();
		for (int i = 0 ;  i < lines.size() ;i++ ){
			double[] v = {values[i]};
			tableContents.add(i,v); 
		}
		
		
	}
	
	@Override
	protected void renderValueLine(int line){
		switch(line){
		case 0: 
			startCellValue();
			out.append( String.format("%d", (long) tableContents.get(line)[0]));
			endCellValue();
			break; 
		default: 
			startCellValue(); 
			// Print frequency:
			out.append( String.format("%.2f", tableContents.get(line)[0]));
			endCellValue();
			break;
		}

	}
}
