package readtext.cassandra;

import java.util.ArrayList;
import java.util.Map;

import readtext.ActorData;
import readtext.ReadTextFromFile;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 *
 * @author Group18
 */
public class LabClient {
	private Cluster cluster;
	private Session session;

	public LabClient(String node) {
		connect(node);
		init();
	}

	private void init() {
	}

	private void connect(String node) {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		long start = cal.getTimeInMillis();
		cluster = Cluster.builder().addContactPoint(node).build();

		session = cluster.connect();
		session.execute("USE group18;");
		cal = java.util.Calendar.getInstance();
		long end = cal.getTimeInMillis();
		long diff = end - start;

		System.out.println("Connected. Time ellapsed: " + diff);
	}

	public void close() {
		cluster.close();
		session.close();
		System.out.println("Connection closed. ");
	}

	public Object getMovie(String title) {
		ResultSet results = session
				.execute("SELECT description FROM movie_desc WHERE title = '"
						+ title + "'");

		if (results != null) {
			/*Row description = results.one();
			return description;*/
                    return results;
		}

		return null;
	}

	public Object getTopMovies(String genre) {
		ResultSet results = session
				.execute("SELECT title, rating FROM Ratings WHERE genre = '"
						+ genre + "' ORDER BY rating DESC LIMIT 30;");

		if (results != null) {
			/*Row description = results.one();
			return description;*/
                    return results;
		}
		return null;
	}

	public Object getTopActors() {
		ResultSet results = session.execute("SELECT name, filmed_in FROM popularity WHERE"
                        + " fake_field=1 ORDER BY filmed_in DESC LIMIT 10;");

		if (results != null) {
			/*Row description = results.one();
			return description;*/
                    return results;
		}
		return null;
	}

	public void addMovie(String title, 
			int year, 
			int rating, 
			String[] genres,
			String[] actors) {
		
		if (getMovie(title) != null) {
			System.out.println("Movie already exists");
			return;
		}
		BatchStatement batch = new BatchStatement();
		PreparedStatement insertMovieDesc = session
				.prepare("INSERT INTO Movie_desc (title, description) VALUES (?, ?)");
		PreparedStatement insertRating = session
				.prepare("INSERT INTO Ratings (genre, rating, title) VALUES (?, ?, ?)");
		PreparedStatement insertActor = session
				.prepare("INSERT INTO Actors (name, filmed_in) VALUES (?, ?)");
		PreparedStatement insertPopularity = session
				.prepare("INSERT INTO Popularity (fake_field, name, filmed_in) VALUES (1, ?, ?)");
		PreparedStatement deletePopularity = session
				.prepare("DELETE FROM Popularity WHERE fake_field=1 AND filmed_in=? AND name='?'");

		String description = "TITLE: " + title + ";YEAR: " + year + ";"
				+ "RATING: " + rating + "; GENRES: " + genres.toString() + ";"
				+ "ACTORS: " + actors.toString() + ";";

		batch.add(insertMovieDesc.bind(title, description));

		for (int i = 0; i < genres.length; i++) {
			batch.add(insertRating.bind(genres[i], rating, title));
		}

		for (int i = 0; i < actors.length; i++) {
			int filmedIn = getFilmedIn(actors[i]);
			if (filmedIn == 0) {
				batch.add(deletePopularity.bind(filmedIn, actors[i]));
			}
			filmedIn += 1;
			batch.add(insertActor.bind(actors[i], filmedIn));
			batch.add(insertPopularity.bind(actors[i], filmedIn));
		}

		session.execute(batch);
	}
	
	public void deleteMovie(String title, 
			int year, 
			int rating, 
			String[] genres,
			String[] actors) {
		
		if (getMovie(title) == null) {
			System.out.println("Movie not exists");
			return;
		}
		
		BatchStatement batch = new BatchStatement();
		PreparedStatement deleteMovieDesc = session
				.prepare("DELETE FROM Movie_desc WHERE title=?");
		PreparedStatement deleteRating = session
				.prepare("DELETE FROM Ratings WHERE genre='?' and rating=? and title='?'");
		PreparedStatement insertActor = session
				.prepare("INSERT INTO Actors (name, filmed_in) VALUES (?, ?)");
		PreparedStatement insertPopularity = session
				.prepare("INSERT INTO Popularity (fake_field, name, filmed_in) VALUES (1, ?, ?)");
		PreparedStatement deletePopularity = session
				.prepare("DELETE FROM Popularity WHERE fake_field=1 AND filmed_in=? AND name='?'");

		String description = "TITLE: " + title + ";YEAR: " + year + ";"
				+ "RATING: " + rating + "; GENRES: " + genres.toString() + ";"
				+ "ACTORS: " + actors.toString() + ";";

		batch.add(deleteMovieDesc.bind(title, description));

		for (int i = 0; i < genres.length; i++) {
			batch.add(deleteRating.bind(genres[i], rating, title));
		}

		for (int i = 0; i < actors.length; i++) {
			int filmedIn = getFilmedIn(actors[i]);
			if (filmedIn == 0) {
				batch.add(deletePopularity.bind(filmedIn, actors[i]));
			}
			filmedIn -= 1;
			batch.add(insertActor.bind(actors[i], filmedIn));
			batch.add(insertPopularity.bind(actors[i], filmedIn));
		}

		session.execute(batch);
	}

	public int getFilmedIn(String actorName) {
		ResultSet results = session
				.execute("SELECT filmed_in FROM actors WHERE name='"
						+ actorName + "'");

		if (results != null) {
			Row description = results.one();
			return description.getInt(0);
                    
		}
		return 0;
	}

        private static void printResults(ResultSet result){
            int i = 0;
            for(Row row : result){
                i++;
                System.out.println(i + " " + row.toString());
            }
        }
        
	public static void main(String[] args) {
		//LabClient labClient = new LabClient("54.185.30.189");
                LabClient labClient = new LabClient("localhost");
                System.out.println("\n" + "Get the movie Shoggoth (2012)"+ "\n");
		printResults((ResultSet)labClient.getMovie("Shoggoth (2012)"));
                System.out.println("\n" +"Get the 30 top rated movies from genre Musical"+ "\n");
		printResults((ResultSet)labClient.getTopMovies("Musical"));
                System.out.println("\n" + "Get the 10 top Actors who played in most movies" + "\n");
		printResults((ResultSet)labClient.getTopActors());
		labClient.close();
	}
}
