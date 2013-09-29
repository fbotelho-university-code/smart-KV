package smartkv.datastore.workloads.ReportGenerator.htmlElements;

import org.apache.commons.math3.stat.Frequency;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

public class FrequencyGenerator extends TableGenerator{

	boolean accum =true;
	public FrequencyGenerator(String title, String description, Frequency freq, boolean accumallated) {
		super(title, description);
		if (accumallated)
			columns = Lists.newArrayList("X","Freq","P[x]","P[x <= X]");
		else{
			accum = false;
			columns = Lists.newArrayList("X","Freq","P[x]");
		}
		lines = Lists.newArrayList();
		tableContents = Lists.newArrayList(); 
		reduceVerbosityOfFrequency(freq);
	}


	/**
	 * This initializes the values and the lines on TableGenerator 
	 * @param freq The frequency data . 
	 */
	private void reduceVerbosityOfFrequency(Frequency freq) {
		int line = 0;
		PeekingIterator<Comparable<?>> it = Iterators.peekingIterator(freq.valuesIterator());
		double count=0, pct=0, cumPct =0; 
		while (it.hasNext()){
			Comparable<?> value = it.next();
			count = freq.getCount(value);
			pct = freq.getPct(value);
			cumPct = freq.getCumPct(value); 
			double max_value = 0.05 - pct; 
			Comparable<?> lastValueInInterval =null; //Represents that we have now a set since the percentage of the value is small (less than max_value) 
			while (it.hasNext() && (max_value - freq.getPct(it.peek()) ) >= 0 ){
				Comparable<?> nextValue = it.next();
				max_value -= freq.getPct(nextValue);
				count += freq.getCount(nextValue);
				pct += freq.getPct(nextValue);
				cumPct += freq.getPct(nextValue);  
				lastValueInInterval = nextValue; 
			}
			lines.add( lastValueInInterval == null ?  value.toString() : value.toString() + "-" + lastValueInInterval.toString()  );
			
			if (accum){
				double[] val = {count, pct, cumPct};
				tableContents.add(line,val);
			}
			else{
				double[] val = {count,pct} ;
				tableContents.add(line,val);
			}
			
			
			line++; 
		}
	}
	
	@Override
	protected void renderValueLine(int line){
		double[] values = tableContents.get(line);
		renderFrequency(values); 
		renderPercentage(values[1]);
		if (values.length == 3)
			renderPercentage(values[2]);
	}

	/**
	 * @param requests
	 */
	private void renderPercentage(double value) {
		startCellValue(); 
		// Print frequency:
		out.append( String.format("%3.2f", value * 100));
		endCellValue();
	}

	/**
	 * @param values
	 */
	private void renderFrequency(double[] values) {
		startCellValue(); 
		// Print frequency: 
		out.append( (int) values[0]); 
		endCellValue();
	}
	
}
