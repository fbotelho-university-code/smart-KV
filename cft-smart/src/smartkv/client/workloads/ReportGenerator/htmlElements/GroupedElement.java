package smartkv.client.workloads.ReportGenerator.htmlElements;

import java.util.Collection;

import com.google.common.collect.Lists;

public class GroupedElement extends SourceElement{
	Collection<SourceElement> elements;
	
	public GroupedElement(String title, String description, Collection<SourceElement> elements) {
		super(title, description);
		this.elements = elements; 
	}

	public GroupedElement(String title, String description) {
		super(title, description);
		elements = Lists.newArrayList();
	}

	public void addElement(SourceElement e){
		this.elements.add(e); 
	}
	

	@Override
	public void renderBody() {
		startDiv("", "grouped");
		out.append("<table cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\" >");
		
		out.append("<tr >\n");
		for (SourceElement el: elements){
			out.append("<th style=\"background-color:white; \"> <h3>" + el.getTitle() + "</h3>\n" + el.getDescription() + "</th>\n");
		}
		out.append("</tr>\n");		
		for (SourceElement el: elements){
			out.append("<td style=\"background-color:white;\">");
			out.append(el.getBody()); 
			out.append("</td>\n");
		}
		out.append("</table>"); 
		endDiv(); 
	}
	
	
}

