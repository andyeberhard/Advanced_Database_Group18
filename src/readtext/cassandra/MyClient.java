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
    private BoundStatement insertMovieDesc;

   
   
    public void connect(String node) {
        java.util.Calendar cal = java.util.Calendar.getInstance();
        long start = cal.getTimeInMillis();
        cluster = Cluster.builder()
             .addContactPoint(node)
             .build();
        /*Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", 
             metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
          System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
                host.getDatacenter(), host.getAddress(), host.getRack());
        }*/

        session = cluster.connect();
        session.execute("USE group18;");
        cal = java.util.Calendar.getInstance();
        long end = cal.getTimeInMillis();
        long diff = end - start;

        System.out.println("Connected. Time ellapsed: " + diff);
    }

    public void insertData_Movie_Desc(ArrayList data){
        int done = 0;
        PreparedStatement ps = session.prepare("INSERT INTO movie_desc (title, description) VALUES (?, ?)");
        BatchStatement batch = new BatchStatement();
        for(int i = 0; i < data.size(); i++){
            done++;
            if(done > 999 ){
                session.execute(batch);
                System.out.println(i + " of batch done");
                done = 0;
                batch = new BatchStatement();
            }
            MovieData md = (MovieData)data.get(i);
            batch.add(ps.bind(md.getTitle(), md.getDescription()));
        }
        session.execute(batch);
        System.out.println("Inserting in movie_desc done.");
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
        MyClient client = new MyClient();
        client.connect("54.185.23.182");
        ReadTextFromFile read = new ReadTextFromFile();
        client.insertData_Movie_Desc(read.fillArrayListWithRange(2010, 2014));
        client.close();
    }
}
    
