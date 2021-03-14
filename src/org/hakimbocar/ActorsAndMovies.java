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
        System.out.println("Number of release years: " + numberOfReleaseYears);
        System.out.println("=======================================================");

        /* Question 4 :  Find the earliest release year and the latest */
        IntSummaryStatistics statistics = movies.stream().mapToInt(Movie::releaseYear).summaryStatistics();
        int earliestYear = statistics.getMin();
        int latestYear = statistics.getMax();
        System.out.println("The earliest year: " + earliestYear);
        System.out.println("The latest year  : " + latestYear);
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
        System.out.println("Year with the greatest number of released movies: " + yearWithTheGreatestNumberOfMovies);
        System.out.println("The number of movies released: " + numberOfMoviesInThisYear);
        System.out.println("=======================================================");


        /* Question 6 :  Which movie has the greatest number of actors? What is this movie and what is this number? */

        Movie movieWithGreatestNumberOfActors =
                movies.stream().max(Comparator.comparing(movie -> movie.actors().size())).get();
        int greatestNumberOfActors = movieWithGreatestNumberOfActors.actors().size();
        String titleOfTheMovieWithTheGreatestNumActors = movieWithGreatestNumberOfActors.title();
        System.out.println("Movie with the greatest # actors: " + titleOfTheMovieWithTheGreatestNumActors);
        System.out.println("Number of actors: " + greatestNumberOfActors);
        System.out.println("=======================================================");

        /* Question 7 :  Which actor has played in the most movies? */
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
        System.out.println(actorPlayedInTheGreatestNumberOfMovies + " played in the most movies.");
        System.out.println("He played in " + numberOfMoviePlayedByThatActor + " movies.");
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

        System.out.println(mostSeenActor + " played in the most movies in One Year.\nIt was in: " + whichYear);
        System.out.println("He played in " + numberOfMoviePlayed + " movies.");
        System.out.println("=======================================================");

        /* Question 9 : We want to know the two actors who have played the most together.
         * For that, we want to build a Stream <Map.Entry <Actor, Actor >> of
         * all the actors who have played together.
         * */

        /* Question 9.a
         * Create Comparator of Actor , which compares actors by last name, then by first name.
         * We will use this comparator in the following, to create only sorted pairs.
         */
        Comparator<Actor> cmpActor = Comparator.comparing(Actor::lastName).thenComparing(Actor::firstName);


        /* Question 9.b
         * First create a BiFunction <Stream <Actor>, Actor,Stream<Map.Entry <Actor, Actor >>>
         * which takes as parameter a Stream <Actor> and an Actor, and returns
         * the matching pairs.
         * For example, for the stream {A, B, C} and for actor D,
         * it will return (A, D), (B, D) and (C, D) in a stream.
         */

        BiFunction<Stream<Actor>, Actor, Stream<Map.Entry<Actor, Actor>>> prettyMapActor =
                (streamActor, actor1) -> streamActor
                        .filter(actor2 -> cmpActor.compare(actor2, actor1) < 0)
                        .map(actor2 -> Map.entry(actor2, actor1));

        // Let's make a test
        System.out.println("Test of: BiFunction<Stream<Actor>, Actor, Stream<Map.Entry<Actor,Actor>>>");
        System.out.println("Display the matching pairs Test:");
        List<Movie> m = new ArrayList<>(movies);
        Stream actorStream = m.get(2).actors().stream();
        Stream<Map.Entry<Actor, Actor>> streamOfMapActor = prettyMapActor.apply(actorStream, mostSeenActor);
        streamOfMapActor.forEach((s -> System.out.println("(" + s.getKey() + " , " + s.getValue() + ")")));
        System.out.println("=========================================================");


        /* Question 9.c
         * Then create a Function <Movie, Stream <Actor>> which returns the stream of
         * actors who play in this movie.
         */
        Function<Movie, Stream<Actor>> myMovieToActors =
                movie -> movie.actors().stream();

        // Let's make a test: display the values of Stream<Actor>
        Stream<Actor> testActor = myMovieToActors.apply(m.get(1)); // m.get(1) : take the second movie on the List<Movie> m
        System.out.println("Test of: Function<Movie, Stream<Actor>>\nDisplay the stream of actors who play in : " + m.get(1).title());
        testActor.forEach((System.out::println));
        System.out.println("=========================================================");

        /* Question 9.d
         * So we can deduce a BiFunction <Movie, Actor, Stream<Map.Entry <Actor, Actor >>>
         * which takes a movie and an actor and return the stream of the corresponding actor pairs.
         */

        BiFunction<Movie, Actor, Stream<Map.Entry<Actor, Actor>>> ourMapMovieAndActorToStreamOfActorPairs =
                (movie, actor1) -> prettyMapActor.apply(movie.actors().stream(), actor1);


        /* Question 9.e
         * Deduce a Function <Movie, Stream<Map.Entry <Actor, Actor >> which takes a
         * movie as a parameter and returns the stream of the actor pairs in that movie.
         */

        Function<Movie, Stream<Map.Entry<Actor, Actor>>> movieToActors =
                movie -> movie.actors().stream()
                        .flatMap(actor -> ourMapMovieAndActorToStreamOfActorPairs.apply(movie, actor));

        // Let's take a look by creating Stream<Map.Entry<Actor, Actor>> and display its values
        Stream<Map.Entry<Actor, Actor>> entry5MovieToActors =
                movies.stream()
                        .flatMap(movieToActors)
                        .limit(4); // limit to four lines:

        System.out.println("Display the Stream<Map.Entry<Actor, Actor>> pairs Limit:4");
        entry5MovieToActors.forEach((s -> System.out.println("(" + s.getKey() + " , " + s.getValue() + ")")));
        System.out.println("=========================================================");

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

        System.out.println("The number of pairs of actor can be constructed: " + numberOfPairs);
        System.out.println("The number of unique pairs: " + numberOfUniquePairs);
        System.out.println("=======================================================");

        Map.Entry<Map.Entry<Actor, Actor>, Long> entry3 =
                movies.stream()
                        .flatMap(movieToActors)
                        .collect(Collectors.groupingBy(
                                Function.identity(), Collectors.counting()
                        ))
                        .entrySet().stream()
                        .max(Map.Entry.comparingByValue())
                        .get();

        // the two actors that played the most together
        Actor oneOfTwoActors = entry3.getKey().getKey();
        Actor theSecondActor = entry3.getKey().getValue();
        System.out.println("The two actors that played the most together:\n-" + oneOfTwoActors + "\n-" + theSecondActor);
        System.out.println("=======================================================");

        /*Question 10 */
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

        int year = entry4.getKey();
        Actor actor1ForAYear = entry4.getValue().getKey().getKey();
        Actor actor2ForAYear = entry4.getValue().getKey().getValue();
        System.out.println(actor1ForAYear + " & " + actor2ForAYear +
                "\nplayed the most together in " + year);
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
