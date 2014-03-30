/*
 * Class to Read the text file
 */
package readtext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Andreas Eberhard
 */
public class ReadTextFromFile {
private BufferedReader read;

    
    public ReadTextFromFile(){
        try {
            File file = new File("D:\\Uni\\Advanced_Database\\movies_dump.txt");
            String encode = "ISO-8859-1";
            InputStream fis = new FileInputStream(file);
            Reader r = new InputStreamReader(fis, encode);
            read = new BufferedReader(r);
        } catch (FileNotFoundException | UnsupportedEncodingException ex) {
            Logger.getLogger(ReadTextFromFile.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
    public void showNLines(int n){
        for(int i = 0; i < n ; i++){
            String s = getNextLine();
            System.out.println(i + ": " +s);
        }
    }
    
    public void showEntries(ArrayList range){
        for(int i = 0; i < range.size(); i++){
            MovieData md = (MovieData)range.get(i);
            System.out.println(i + ": " + md.getDescription());
        }
    }
    
    public ArrayList fillArrayListWithRange(int fromYear, int toYear){
        ArrayList list = new ArrayList();
        String s = "";
        int line = -1;
        int actLines = -1;
        while((s = getNextLine()) != null){
            line++;
            String [] split = s.split("\t");
            int year = Integer.parseInt(split[0]);
            if(year >= fromYear && year <= toYear){
                actLines++;
                MovieData md = new MovieData(split);
                list.add(md);         
            }
        }
        /*System.out.println(line);
        System.out.println(list.size());*/
        return list;
    }
    
    private String getNextLine(){
        String s = "not readable";
        try {
            s = read.readLine();
        } catch (IOException ex) {
            Logger.getLogger(ReadTextFromFile.class.getName()).log(Level.SEVERE, null, ex);
        }
        return s;
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        ReadTextFromFile read = new ReadTextFromFile();
        //read.showNLines(100);   
        read.showEntries(read.fillArrayListWithRange(2010, 2014));
    }
}
