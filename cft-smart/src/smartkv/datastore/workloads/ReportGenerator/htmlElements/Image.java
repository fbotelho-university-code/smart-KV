package smartkv.datastore.workloads.ReportGenerator.htmlElements;

import java.io.IOException;

import smartkv.datastore.workloads.ReportGenerator.Source;

import com.google.common.base.Function;

public class Image extends SourceElement{
	
	Function<String,String> saveFile; 
	String imgPath;
	String fullQualifiedPath; 
	
	/**
	 * 
	 * @param title A title of the element
	 * @param description A description
	 * @param rootPath the OutputFolder  
	 * @param imgName The suggested image nam3

	 * @param saveFile The function that will
	 * 
	 *  save the image file on the given an path. That function should return the path with the addition of the filetype 
	 */
	public Image(String title, String description, String imgPathRelativeToRoot) {
		super(title, description);
		this.imgPath = imgPathRelativeToRoot;
	}
	
	@Override
	public void renderBody(){
		//Outside knows how the save the image. Not us
		out.append("<img src=\""  + imgPath + "\" alt=\"" + this.getTitle() + "\">");
	}
	

}
