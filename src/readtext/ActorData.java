/*
 * Representation of the text file lines
 */
package readtext;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Group18
 */
public class ActorData {
	 
    public static Map<String, ActorData> Cache = new HashMap<String, ActorData>();
    private int filmed_in;
    private String name;
    
    public ActorData(String name, int filmed_in){
    	this.name = name;
    	this.filmed_in = filmed_in;
    }
      
    public String getName(){
        return this.name;
    } 
    
    public void setName(String value){
        this.name = value;
    } 
    
    public int getFilmedIn(){
        return this.filmed_in;
    }
    
    public void setFilmedIn(int value){
        this.filmed_in = value;
    }
}
