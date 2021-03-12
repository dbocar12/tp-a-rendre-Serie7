package model;
import java.util.HashSet;
import java.util.Set;

public class Movie {
    private String title ;
    private int releaseYear ;
    
    private Set<Actor> actors = new HashSet<>() ;
    
    public Movie(String title, int releaseYear) {
        this.title = title;
        this.releaseYear = releaseYear;
    }
    
    public String title() {
        return this.title ;
    }
    
    public int releaseYear() {
        return this.releaseYear ;
    }
    
    public void addActor(Actor actor) {
        this.actors.add(actor) ;
    }
    
    public Set<Actor> actors() {
        return this.actors ;
    }
    
    @Override
    public String toString() {
        return "Movie{" + "title=" + title + ", releaseYear=" + releaseYear + ", actors=" + actors + '}';
    }
}
