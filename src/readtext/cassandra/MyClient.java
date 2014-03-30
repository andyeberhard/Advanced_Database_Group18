/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package readtext.cassandra;


import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import readtext.MovieData;
import readtext.ReadTextFromFile;
/**
 *
 * @author Andreas Eberhard
 */
public class MyClient {
   
    private Cluster cluster;
    private Session session;
   
    private PreparedStatement insertmoviedesc;
    private PreparedStatement selectmoviedesc;
    
    public MyClient(String node){
        connect(node);
        init();
    }
    
    private void init(){
        insertmoviedesc = session.prepare("INSERT INTO movie_desc (title, description) VALUES (?, ?)");
        selectmoviedesc = session.prepare("SELECT description FROM movie_desc WHERE title=?");
    }
    
    public String getMovieDesc(String title){
        //BatchStatement batch = new BatchStatement();
        selectmoviedesc.bind(title);
        ResultSet results = session.execute(selectmoviedesc.bind(title));
        String result = "";
        for (Row row : results) {
            result += row.getString("description") + "\n";
        }
        return result;
    }  
    
    private void connect(String node) {
        java.util.Calendar cal = java.util.Calendar.getInstance();
        long start = cal.getTimeInMillis();
        cluster = Cluster.builder()
             .addContactPoint(node)
             .build();
        
        session = cluster.connect();
        session.execute("USE group18;");
        cal = java.util.Calendar.getInstance();
        long end = cal.getTimeInMillis();
        long diff = end - start;

        System.out.println("Connected. Time ellapsed: " + diff);
    }

    public void insertData_Movie_Desc(ArrayList data){
        int done = 0;
        BatchStatement batch = new BatchStatement();
        for(int i = 0; i < data.size(); i++){
            done++;
            MovieData md = (MovieData)data.get(i);
            batch.add(insertmoviedesc.bind(md.getTitle(), md.getDescription()));
            if(done > 999 ){
                session.execute(batch);
                System.out.println(i + " of batch done");
                done = 0;
                batch = new BatchStatement();
            }   
        }
        session.execute(batch);
        System.out.println("Inserting in movie_desc done.");
    }
    
    public void insertData_Actors(MovieData md){
        int done = 0;
        PreparedStatement ps = session.prepare("INSERT INTO actors (name, filmed_in) VALUES (?, ?)");
        BatchStatement batch = new BatchStatement();
        for(int i = 0; i < md.getActors().length; i++){
            done++;
            if(done > 999 ){
                session.execute(batch);
                System.out.println(i + " of batch done");
                done = 0;
                batch = new BatchStatement();
            }
            batch.add(ps.bind(md.getActors()[i], md.getTitle()));
        }
        System.out.println("Inserting in actors done.");
    }
    
    public void showMoviesData(){
        java.util.Calendar cal = java.util.Calendar.getInstance();
        long start = cal.getTimeInMillis();
        ResultSet res = session.execute("SELECT * FROM movies");
        cal = java.util.Calendar.getInstance();
        long end = cal.getTimeInMillis();
        long diff = end - start;
        System.out.println("time ellapsed: " + diff);
        for(Row row : res){
            System.out.println(row.toString());
        }
    }

    public void close() {
        cluster.close();
        session.close();
        System.out.println("Connection closed. ");
    }

    public static void main(String[] args) {
        MyClient client = new MyClient("54.185.23.182");
        /*ReadTextFromFile read = new ReadTextFromFile();
        client.insertData_Movie_Desc(read.fillArrayListWithRange(2010, 2014));*/
        
        System.out.println(client.getMovieDesc("\"Angel Beats!\" (2010) {Alive (#1.7)}"));
        
        client.close();
    }
}
    
