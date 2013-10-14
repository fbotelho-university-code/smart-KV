package smartkv.client.workloads.ReportGenerator.htmlElements;

import java.util.Iterator;

import smartkv.client.workloads.ActivityEvent;
import smartkv.client.workloads.EVENT_TYPE;
import smartkv.client.workloads.RequestLogEntry;
import smartkv.client.workloads.RequestLogWithDataInformation;
import smartkv.client.workloads.ReportGenerator.WorkLoadResults;

public class ListOperations extends SourceElement{
	WorkLoadResults rs;
	public ListOperations(String title, String description, WorkLoadResults rs) {
		super(title, description);
		this.rs = rs;
	}

	@Override
	public void renderBody() {
		begin_list();
		Iterator<ActivityEvent>  it = rs.getActivityLog().iterator();
		ActivityEvent current = it.hasNext() ? it.next(): null; 
		for (RequestLogEntry en  : rs.getRequestLog()){
			if ( (current != null) && current.getTimeStart() <=  en.getTimeStarted() ){
				render_element(current); 
				current = it.hasNext() ?  it.next(): null;  
			}
			render_element(en);
		}
		end_list(); 
	}

	private void render_element(ActivityEvent current) {
		if (current.getType().equals(EVENT_TYPE.PACKET_IN)){
			out.append("<div style=\"min-width:300px;min-height:100px;border:1px solid blue;\">");
			out.append( "(" + (current.timeStart - rs.getTimeZero()) + ")" +current.p);
			out.append("</div>"); 
		}
		System.out.println("--"); 
	}

	private void end_list(){
		out.append("</ul>");
	}
	private void begin_list() {
		out.append("<ul>");
	}
	
	public static int v =0; 
	
	private void render_element(RequestLogEntry en) {
		System.out.println("{ FlowSimulation." + (en.getType().isRead() ?  "READ_OP" : "WRITE_OP") +  "," + en.getSizeOfRequest() + "," + en.getSizeOfResponse() + "},"); 
		out.append("<li>");
		if (en instanceof RequestLogWithDataInformation){
			RequestLogWithDataInformation data = (RequestLogWithDataInformation) en;
			long time = en.getTimeStarted() - rs.getTimeZero(); 
			out.append( "(" + (time)  + " - " + en.tid  + ") - " + (en.getType().isRead() ? "(R) - " : "(W) - ") +  
				en.getType().name()  +  ", table = " + data.getTable() +    
				(!data.getKey().equals("-")?   ", key = " + data.getKey() :"") +
				(!data.getValue().equals("-") ?  ", value = " + data.getValue() : "") +
				(!data.getExistentValue().equals("-")? ", existentValue = " + data.getExistentValue() : "") +
				(data.getReturnedValue() != null && !data.getReturnedValue().equals("-")? ", return = " + data.getReturnedValue() : "") +
				 ", <b>request size = " + en.getSizeOfRequest() + ", response size =" + en.getSizeOfResponse() + "</b>" 
			);
		}
		else{
			out.append( "(" + (en.getTimeStarted() - rs.getTimeZero())  + " - " + en.tid  + ") - " + (en.getType().isRead() ? "(R) - " : "(W) - ") +  
					en.getType().name()  +      
					 ", request size = " + en.getSizeOfRequest() + ", response size =" + en.getSizeOfResponse() 
				);
		}
		out.append(" <a href=\"javascript:hideshow(document.getElementById('st" + v + "'))\">Hide/Show</a>");
		out.append("<br>"); 
		out.append(printStackTrace(en)); 
		out.append("<br><br><br>"); 
		
		out.append("</li>");
	}

	private String printStackTrace(RequestLogEntry en) {
		StringBuilder s = new StringBuilder();
		s.append("<div id=\"st"+  (v++)  +"\"  style=\"font:12px ; display: block\"> ");
		
		s.append("<ul>");
		for (int i =0 ; i <  en.st.length ;  i++){
			if (en.st[i] != null && accept(en.st[i])){
			s.append("<li>");
			s.append(en.st[i]);
			s.append("</li>");
			}
		}
		s.append("</ul>"); 
		s.append("</div>"); 
		return s.toString();
	}

	private boolean accept(String string) {
		return true; 
		//return //string.matches("net.floodlightcontroller.devicemanager.internal.*") ||  
				//(string.matches("net.floodlightcontroller.*") && !string.matches("net.floodlightcontroller.core.*")); 
	}
}
