/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package readtext;

/**
 *
 * @author Andreas Eberhard
 */
public class MovieData {
    private int year;
    private String [] split;
    private String title;
    private String [] genre = {"unknown"};;
    private float rating;
    private String [] actors = {"unknown"};
    
    public MovieData(String [] s){
        this.split = s;
        prepare();
    }
    
    private void prepare(){
        this.year = Integer.parseInt(split[0]);
        this.title  = split[1];
        this.rating = Float.parseFloat(split[2]);
        if(split.length > 3){
            if(!split[3].equals("")){
                this.genre = split[3].split("\\|");
            }
            
            if(split.length > 4){
                if(!split[4].equals("")){
                    this.actors = split[4].split("\\|");
                }
            }
        }   
    }
    
    public String[] getActors(){
        return this.actors;
    }
    
    private String getActorsAsString(){
        String as = "ACTORS: ";
        for(int i = 0; i < actors.length; i++){
            as += actors[i] + ", ";
        }
        
        return as;
    }
    
    private String getGenreAsString(){
        String gs = "GENRE: ";
        for(int i = 0; i < genre.length; i++){
            gs += genre[i] + ", ";
        }
        
        return gs;
    }
    
    public String description(){
        String des = "";
        des += "TITLE: " + this.title + " ";
        des += "YEAR: " + this.year + " ";
        des += "RATING: " + this.rating + " ";
        des += getGenreAsString();
        des += getActorsAsString();
        
        return des;
    }
    
}
