package org.hakimbocar;
import model.Actor;
import model.Movie;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;


public class ActorsAndMovies {

    public static void main(String[] args) {

        ActorsAndMovies actorsAndMovies = new ActorsAndMovies();
        Set<Movie> movies = actorsAndMovies.readMovies();

		/* Question 1 : How many movies are referenced in this file ? */
        System.out.println("Number of movies: " + movies.size());
        System.out.println("=======================================================");

		/* Question 2 : How many actors are referenced in this file ? */
        long numberOfActors = 
        movies.stream()
			.flatMap(movie -> movie.actors().stream())
			.distinct()
			.count();
        System.out.println("Number of actors: " + numberOfActors);
		System.out.println("=======================================================");

		/* Question 3 :  How many release years in this file ? */
		long numberOfReleaseYears = movies.stream().map(Movie::releaseYear).distinct().count();
        System.out.println("number of release years: " + numberOfReleaseYears);
		System.out.println("=======================================================");

		/* Question 4 :  Find the earliest release year and the latest */
		IntSummaryStatistics statistics = movies.stream().mapToInt(Movie::releaseYear).summaryStatistics();
		int earliestYear = statistics.getMin();
		int latestYear = statistics.getMax();
		System.out.println("The earliest year: "+earliestYear);
		System.out.println("The latest year: "+latestYear);
		System.out.println("=======================================================");


		/* Question 5 :  In which year was the greatest number of movies released? What is this number ? */
		Entry<Integer, Long> numberOfMoviesPerYear = movies.stream().collect(Collectors.groupingBy(
				Movie::releaseYear, Collectors.counting()
		))
				.entrySet().stream()
				.max(Entry.comparingByValue())
				.get();

		int yearWithTheGreatestNumberOfMovies = numberOfMoviesPerYear.getKey();
		long numberOfMoviesInThisYear = numberOfMoviesPerYear.getValue();
		System.out.println("Year with the greatest number of movies: "+yearWithTheGreatestNumberOfMovies);
		System.out.println("The greatest number of movies: "+numberOfMoviesInThisYear);
		System.out.println("=======================================================");


		/* Question 6 :  Which movie has the greatest number of actors? What is this movie and what is this number? */

		Movie movieWithGreatestNumberOfActors =
				movies.stream().max(Comparator.comparing(movie -> movie.actors().size())).get();
		int greatestNumberOfActors = movieWithGreatestNumberOfActors.actors().size();
		String titleOfTheMovieWithTheGreatestNumActors = movieWithGreatestNumberOfActors.title();
		System.out.println("Movie with the greatest # actors: " + titleOfTheMovieWithTheGreatestNumActors);
		System.out.println("Number of actors: " + greatestNumberOfActors);
		System.out.println("==================================================");


		/* Question 7 :  Which actor has he played in the most movies? */
		// actor that played in the greatest number of movies
		Map.Entry<Actor, Long> actorPerMovie =
				movies.stream().flatMap(movie -> movie.actors().stream())
						.collect(Collectors.groupingBy(
								Function.identity(), Collectors.counting()
						))
						.entrySet().stream()
						.max(Map.Entry.comparingByValue())
						.get();

		Actor actorPlayedInTheGreatestNumberOfMovies = actorPerMovie.getKey();
		long numberOfMoviePlayedByThatActor = actorPerMovie.getValue();
		System.out.println(actorPlayedInTheGreatestNumberOfMovies+" played in \nthe most movies.");
		System.out.println("He played in "+numberOfMoviePlayedByThatActor+" movies.");
		System.out.println("=======================================================");


		/* Question 8 : Let's answer the question 7 by using only one collector.
		* Use then this collector to determine which actor has played in the
		* greatest number of movies in one year.
		* */

		Collector<Movie, ?, Map.Entry<Actor, Long>> collector =
				Collectors.collectingAndThen(
						Collectors.flatMapping(
								movie -> movie.actors().stream(),
								Collectors.groupingBy(
										Function.identity(), Collectors.counting()
								)
						),
						map -> map.entrySet().stream()
								.max(Map.Entry.comparingByValue())
								.get());

		Map.Entry<Integer, Map.Entry<Actor, Long>> entry =
				movies.stream()
						.collect(
								Collectors.groupingBy(
										Movie::releaseYear, collector
								)
						)
						.entrySet().stream()
						.max(Map.Entry.comparingByValue(Map.Entry.comparingByValue()))
						.get();

		int whichYear = entry.getKey();
		Actor mostSeenActor = entry.getValue().getKey();
		long numberOfMoviePlayed = entry.getValue().getValue();

		System.out.println(mostSeenActor+" played in \nthe most movies in the year "+whichYear);
		System.out.println("He played in "+numberOfMoviePlayed+" movies.");
		System.out.println("=======================================================");

		/* Question 9 : We want to know the two actors who have played the most together.
		* For that, we want to build a Stream <Map.Entry <Actor, Actor >> of
		* all the actors who have played together.
		* */

		Comparator<Actor> cmpActor = Comparator.comparing(Actor::lastName).thenComparing(Actor::firstName);





		BiFunction<Movie, Actor, Stream<Map.Entry<Actor, Actor>>> ourMap =
				(movie, actor1) -> movie.actors().stream()
						.filter(actor2 -> cmpActor.compare(actor2, actor1) < 0)
						.map(actor2 -> Map.entry(actor2,actor1));



		Arrays.stream(ourMap.apply(movieWithGreatestNumberOfActors, mostSeenActor).toArray()).forEach((s)->
				System.out.println(s));

		Function<Movie, Stream<Map.Entry<Actor, Actor>>> movieToActors =
				movie -> movie.actors().stream()
						.flatMap(actor -> ourMap.apply(movie, actor));



		Map.Entry<Map.Entry<Actor, Actor>, Long> entry3 =
				movies.stream()
						.flatMap(movieToActors)
						.collect(Collectors.groupingBy(
								Function.identity(), Collectors.counting()
						))
						.entrySet().stream()
						.max(Map.Entry.comparingByValue())
						.get();


		// how many pairs of actors can be constructed from this file?
		long numberOfPairs =
				movies.stream()
						.flatMap(movieToActors)
						.count();

		//How many unique pairs are there?
		long numberOfUniquePairs =
				movies.stream()
						.flatMap(movieToActors)
						.distinct()
						.count();

		System.out.println("The number of pairs of actor can be constructed: "+numberOfPairs);
		System.out.println("The number of unique pairs: "+numberOfUniquePairs);
		System.out.println("=======================================================");


		// the two actors that played the most together
		Actor oneOfTwoActors = entry3.getKey().getKey();
		Actor theSecondActor = entry3.getKey().getValue();
		System.out.println("The two actors that played the most together:\n-"+oneOfTwoActors+"\n-"+theSecondActor);
		System.out.println("=======================================================");

		// the two actors who played the most together for a year.
		Collector<Movie, ?, Map.Entry<Map.Entry<Actor, Actor>, Long>> collector2 =
				Collectors.collectingAndThen(
						Collectors.flatMapping(
								movieToActors,
								Collectors.groupingBy(
										Function.identity(), Collectors.counting()
								)
						),
						map -> map.entrySet().stream()
								.max(Map.Entry.comparingByValue())
								.get()
				);

		Map.Entry<Integer, Map.Entry<Map.Entry<Actor, Actor>, Long>> entry4 =
				movies.stream()
						.collect(Collectors.groupingBy(
								Movie::releaseYear, collector2
						))
						.entrySet().stream()
						.max(Map.Entry.comparingByValue(Map.Entry.comparingByValue()))
						.get();

		int year = entry.getKey();
		long numberOfActorWhoPlayedTheMostTogetherForAYear = entry.getValue().getValue();
		Actor actor1ForAYear = entry4.getValue().getKey().getKey();
		Actor actor2ForAYear = entry4.getValue().getKey().getValue();
		System.out.println("The number of actors who played\nthe most together for a year: "+numberOfActorWhoPlayedTheMostTogetherForAYear);
		System.out.println("In Year: "+year);
		System.out.println(actor1ForAYear+" \n"+actor2ForAYear+"\nThey played the most together");
		System.out.println("=======================================================");

	}

    public Set<Movie> readMovies() {

        Function<String, Stream<Movie>> toMovie =
                line -> {
                    String[] elements = line.split("/");
                    String title = elements[0].substring(0, elements[0].lastIndexOf("(")).trim();
                    String releaseYear = elements[0].substring(elements[0].lastIndexOf("(") + 1, elements[0].lastIndexOf(")"));
                    if (releaseYear.contains(",")) {
                        // Movies with a coma in their title are discarded
                    	int indexOfComa = releaseYear.indexOf(",");
                    	releaseYear = releaseYear.substring(0, indexOfComa);
                        // return Stream.empty();
                    }
                    Movie movie = new Movie(title, Integer.parseInt(releaseYear));


                    for (int i = 1; i < elements.length; i++) {
                        String[] name = elements[i].split(", ");
                        String lastName = name[0].trim(); 
                        String firstName = "";
                        if (name.length > 1) {
                            firstName = name[1].trim();
                        }

                        Actor actor = new Actor(lastName, firstName);
                        movie.addActor(actor);
                    }
                    return Stream.of(movie);
                };

        try (FileInputStream fis = new FileInputStream("files/movies-mpaa.txt.gz");
             GZIPInputStream gzis = new GZIPInputStream(fis);
             InputStreamReader reader = new InputStreamReader(gzis);
             BufferedReader bufferedReader = new BufferedReader(reader);
             Stream<String> lines = bufferedReader.lines()
		) {

            return lines.flatMap(toMovie).collect(Collectors.toSet());

        } catch (IOException e) {
            System.out.println("e.getMessage() = " + e.getMessage());
        }

        return Set.of();
    }
}
