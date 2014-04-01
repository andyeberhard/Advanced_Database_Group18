/*
 * Representation of the text file lines
 */
package readtext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Group18
 */
public class RatingData {
	 
    public static ArrayList<RatingData> Cache = new ArrayList<RatingData>();
    private float rating;
    private String genre;
    private String title;
    
    public RatingData(String title, String genre, float rating){
    	this.title = title;
    	this.genre = genre;
    	this.rating = rating;
    }
      
    public String getTitle(){
        return this.title;
    } 
    
    public void setTitle(String value){
        this.title = value;
    } 
    
    public String getGenre(){
        return this.genre;
    } 
    
    public void setGenre(String value){
        this.genre = value;
    } 
    
    public float getRating(){
        return this.rating;
    }
    
    public void setRating(float value){
        this.rating = value;
    }
}
