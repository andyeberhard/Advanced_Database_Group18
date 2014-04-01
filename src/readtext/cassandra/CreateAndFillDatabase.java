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
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import readtext.MovieData;
import readtext.ReadTextFromFile;
/**
 *
 * @author Group18
 */
public class CreateAndFillDatabase {
   
    private Cluster cluster;
    private Session session;
   
    private PreparedStatement insertmoviedesc;
    private PreparedStatement insertratings;
    private PreparedStatement insertactors;
    private PreparedStatement insertpopularity;
    private PreparedStatement selectactorfilmedin;
    private String createmovie_desc;
    private String createratings;
    private String createactors;
    private String createpopularity;
    private HashMap<String,Integer> actormap = new HashMap<String,Integer>();
    
    public CreateAndFillDatabase(String node){
        connect(node);
        init();
    }
    
    private void init(){
        //statments for creating tables
        createmovie_desc = "CREATE TABLE movie_desc (title varchar PRIMARY KEY, "
                + "description varchar)";
        createratings = "CREATE TABLE ratings (genre varchar, "
                + "rating float, "
                + "title varchar, "
                + "PRIMARY KEY (genre, rating, title))";
        createactors = "CREATE TABLE actors (name varchar PRIMARY KEY, filmed_in int)";
        createpopularity = "CREATE TABLE popularity (fake_field int, filmed_in int, "
                + "name varchar, PRIMARY KEY (fake_field, filmed_in, name))";
        createTables();
        
        //prepared Statments for inserts
        insertmoviedesc = session.prepare("INSERT INTO movie_desc (title, description) VALUES (?,?)");
        insertratings = session.prepare("INSERT INTO ratings (genre, rating, title) VALUES (?,?,?)");
        insertactors = session.prepare("INSERT INTO actors (name, filmed_in) VALUES(?,?)");
        insertpopularity = session.prepare("INSERT INTO popularity (fake_field, filmed_in, name) VALUES (1,?,?)");
        selectactorfilmedin = session.prepare("SELECT filmed_in FROM actors WHERE name=?");
    }
   
    public void insertData(ArrayList data){
        int done = 0;
        BatchStatement batch = new BatchStatement();
        for(int i = 0; i < data.size(); i++){
            done++;
            MovieData md = (MovieData)data.get(i);
            batch.add(insertmoviedesc.bind(md.getTitle(), md.getDescription()));
            String [] genre = md.getGenre();
            for(int j = 0; j < genre.length; j++){
                batch.add(insertratings.bind(genre[j], md.getRating(), md.getTitle()));
            }
            String [] act = md.getActors();
            for(int j = 0; j < act.length; j++){
                addActorToMap(act[j]);
            }
                
            if(done > 999 ){
                session.execute(batch);
                System.out.println(i + " of batch done");
                done = 0;
                batch = new BatchStatement();
            }   
        }
        session.execute(batch);
        fillActors();
        System.out.println("Inserting in movie_desc done.");
    }
    
    private void fillActors(){
        BatchStatement batch = new BatchStatement();
        int cnt = 0;
        int tot = 0;
        for (Map.Entry<String, Integer> entry : this.actormap.entrySet()) {
            if(!entry.getKey().equals("unknown")){
                cnt++;
                tot++;
                batch.add(insertactors.bind(entry.getKey(), entry.getValue()));
                batch.add(insertpopularity.bind(entry.getValue(), entry.getKey()));
                if(cnt > 10000 ){
                    session.execute(batch);
                    cnt = 0;
                    batch = new BatchStatement();
                }  
            }
        }
        System.out.println("inserted " + tot + " rows into popularity and actors");
        session.execute(batch);
        
    }
    
    private void addActorToMap(String name){
        //if(!name.equals("unknown")){
          
            Integer integer = this.actormap.get(name);
            if(integer == null){
                integer = 1;
            }
            else{
                integer++;
            }
            actormap.put(name, integer);
        //}
    }
    
    public int getFilmedIn(String actorName) {
		ResultSet results = session
				.execute(this.selectactorfilmedin.bind(actorName));

		if (results != null) {
			Row description = results.one();
			return description.getInt(0);
                    
		}
		return 0;
	}
    
    private void createTables(){
        try{
            session.execute(createmovie_desc);
            System.out.println("movie_desc created..");
        }catch(AlreadyExistsException e){
            System.out.println("movie_desc already exists, skipped..");
        }
        try{
            session.execute(createratings);
            System.out.println("ratings created..");
        }catch(AlreadyExistsException e){
            System.out.println("ratings already exists, skipped..");
        }
        try{
            session.execute(createactors);
            System.out.println("actors created..");
        }catch(AlreadyExistsException e){
            System.out.println("actors already exists, skipped..");
        }
        try{
            session.execute(createpopularity);
            System.out.println("popularity created..");
        }catch(AlreadyExistsException e){
            System.out.println("popularity already exists, skipped..");
        }
    }
    
    /*public String getMovieDesc(String title){
        selectmoviedesc.bind(title);
        ResultSet results = session.execute(selectmoviedesc.bind(title));
        String result = "";
        for (Row row : results) {
            result += row.getString("description") + "\n";
        }
        return result;
    }  */
    
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

    
    /*public void insertData_Movie_Desc(ArrayList data){
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
    }*/
    
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
        CreateAndFillDatabase client = new CreateAndFillDatabase("54.185.23.182");
        //CreateAndFillDatabase client = new CreateAndFillDatabase("localhost");
        ReadTextFromFile read = new ReadTextFromFile();
        
        client.insertData(read.fillArrayListWithRange(2010, 2014));
        client.close();
    }
}
    
