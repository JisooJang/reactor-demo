package com.example.reactordemo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@SpringBootTest
class ReactorDemoApplicationTests {

    class Player {
        private String firstName;
        private String lastName;

        Player(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }

        @Override
        public boolean equals(Object player) {
            if(!(player instanceof Player))
                return false;

            Player playerObj = (Player) player;
            return this.firstName.equals(playerObj.firstName) &&
                    this.lastName.equals(playerObj.lastName);
        }
    }

    @Test
    void createFlux() {
        Flux<String> fruitFlux = Flux
                .just("apple", "banana", "grapefruit", "mango", "watermelon");

        fruitFlux.subscribe(f -> System.out.println("Here's some fruit : " + f));

        StepVerifier.create(fruitFlux)
                .expectNext("apple")
                .expectNext("banana")
                .expectNext("grapefruit")
                .expectNext("mango")
                .expectNext("watermelon")
                .verifyComplete();
    }

    @Test
    void createFluxFromArray() {
        String[] fruit = new String[]{"aa", "bb", "cc", "dd", "ee"};
        Flux<String> fruitFlux = Flux.fromArray(fruit);

        StepVerifier.create(fruitFlux)
                .expectNext("aa")
                .expectNext("bb")
                .expectNext("cc")
                .expectNext("dd")
                .expectNext("ee")
                .verifyComplete();
    }

    @Test
    void createFluxFromStream() {
        Stream<String> words = Stream.of("aa", "bb", "cc", "dd");
        Flux<String> wordsFlux = Flux.fromStream(words);
        StepVerifier.create(wordsFlux)
                .expectNext("aa")
                .expectNext("bb")
                .expectNext("cc")
                .expectNext("dd")
                .verifyComplete();
    }

    @Test
    void createFluxFromRange() {
        Flux<Integer> nums = Flux.range(0, 5);
        StepVerifier.create(nums)
                .expectNext(0)
                .expectNext(1)
                .expectNext(2)
                .expectNext(3)
                .expectNext(4)
                .verifyComplete();
    }

    @Test
    void createFluxInterval() {
        Flux<Long> intervalFlux =
                Flux.interval(Duration.ofSeconds(1)).take(5);

        StepVerifier.create(intervalFlux)
                .expectNext(0L)
                .expectNext(1L)
                .expectNext(2L)
                .expectNext(3L)
                .expectNext(4L)
                .verifyComplete();
    }

    @Test
    void mergeFlux() {
        Flux<String> flux1 = Flux.just("A", "B", "C")
                .delayElements(Duration.ofMillis(500));
        Flux<String> flux2 = Flux.just("a", "b", "c")
                .delaySubscription(Duration.ofMillis(250))
                .delayElements(Duration.ofMillis(500));

        Flux<String> mergeFlux = flux1.mergeWith(flux2);

        StepVerifier.create(mergeFlux)
                .expectNext("A")
                .expectNext("a")
                .expectNext("B")
                .expectNext("b")
                .expectNext("C")
                .expectNext("c")
                .verifyComplete();
    }

    @Test
    void zipFlux() {
        Flux<String> characterFlux = Flux.just("AAA", "BBB", "CCC");
        Flux<String> foodFlux = Flux.just("aaa", "bbb", "ccc");

        Flux<Tuple2<String,String>> zippedFlux = Flux.zip(characterFlux, foodFlux);

        StepVerifier.create(zippedFlux)
                .expectNextMatches(p ->
                        p.getT1().equals("AAA") &&
                        p.getT2().equals("aaa"))
                .expectNextMatches(p ->
                        p.getT1().equals("BBB") &&
                        p.getT2().equals("bbb"))
                .expectNextMatches(p ->
                        p.getT1().equals("CCC") &&
                        p.getT2().equals("ccc"))
                .verifyComplete();
    }

    @Test
    void zipFluxToObject() {
        Flux<String> characterFlux = Flux.just("blue", "pink", "white");
        Flux<String> objFlux = Flux.just("sky", "dress", "tissue");

        Flux<String> zippedFlux = Flux.zip(characterFlux, objFlux, (c, f) -> f + " matches " + c);

        StepVerifier.create(zippedFlux)
                .expectNext("sky matches blue")
                .expectNext("dress matches pink")
                .expectNext("tissue matches white")
                .verifyComplete();
    }

    @Test
    void firstFlux() {
        Flux<String> slowFlux = Flux.just("Kanye West", "YG", "Young Thug")
                .delaySubscription(Duration.ofMillis(100));
        Flux<String> fastFlux = Flux.just("Joey bada$$", "Travis Scott", "Kendrick Lamar");

        Flux<String> firstFlux = Flux.first(slowFlux, fastFlux);

        StepVerifier.create(firstFlux)
                .expectNext("Joey bada$$")
                .expectNext("Travis Scott")
                .expectNext("Kendrick Lamar")
                .verifyComplete();
    }

    @Test
    void filterFlux() {
        Flux<String> filterFlux = Flux.just("Kanye", "Joey", "Dojacat", "Gucci", "Migos", " ", "Joey")
                .distinct()
                .filter(s -> !s.contains(" "));
        StepVerifier.create(filterFlux)
                .expectNext("Kanye")
                .expectNext("Joey")
                .expectNext("Dojacat")
                .expectNext("Gucci")
                .expectNext("Migos")
                .verifyComplete();
    }

    @Test
    void map() {
        // map() -> sync (데이터 발행시 동기적으로 각 항목이 순차적으로 매핑됨)
        Flux<Player> playerFlux = Flux.just("Michael Jordan", "Scottie Pippen", "Steve Kerr")
                .map(n -> {
                    String[] split = n.split("\\s");
                    return new Player(split[0], split[1]);
                });

        StepVerifier.create(playerFlux)
                .expectNext(new Player("Michael", "Jordan"))
                .expectNext(new Player("Scottie", "Pippen"))
                .expectNext(new Player("Steve", "Kerr"))
                .verifyComplete();
    }

    @Test
    void flatMap() {
        Flux<Player> playerFlux = Flux
                .just("Michael Jordan", "Scottie Pippen", "Steve Kerr")
                .flatMap(n -> Mono.just(n)
                    .map(p -> {
                        String[] split = p.split("\\s");
                        return new Player(split[0], split[1]);
                    })
                .subscribeOn(Schedulers.parallel()) // map() 오퍼레이션이 병렬 스레드로 수행됨. 비동기적으로 병행 수행.
                );

        // flatMap()이나 subscribeOn()을 사용하면 다수의 병행 스레드에 작업을 분할하여 스트림의 처리량을 증가시킬 수 있다.
        // 작업이 병행 수행되어 어떤 작업이 먼저 끝날지 순서가 보장이 되지 않음.

        List<Player> playerList = Arrays.asList(
                new Player("Michael", "Jordan"),
                new Player("Scottie", "Pippen"),
                new Player("Steve", "Kerr")
        );

        StepVerifier.create(playerFlux)
                .expectNextMatches(playerList::contains)
                .expectNextMatches(playerList::contains)
                .expectNextMatches(playerList::contains)
                .verifyComplete();
    }

    @Test
    void buffer() {
        Flux<String> fruitFlux = Flux.just("strawberry", "blueberry", "apple", "mellon", "banana");
        Flux<List<String>> bufferedFlux = fruitFlux.buffer(3); // List<String>타입의 Flux 반환

        StepVerifier.create(bufferedFlux)
                .expectNext(Arrays.asList("strawberry", "blueberry", "apple"))
                .expectNext(Arrays.asList("mellon", "banana"))
                .verifyComplete();

        Flux.just("strawberry", "blueberry", "apple", "mellon", "banana")
                .buffer(3) // List<String>타입의 Flux 반환
                .flatMap(x ->
                        Flux.fromIterable(x) // List의 각 요소 타입(String)의 Flux 반환
                        .map(String::toUpperCase)
                        .subscribeOn(Schedulers.parallel())
                        .log()
                ).subscribe();
    }

    @Test
    void collectMap() {
        Flux<String> animalFlux = Flux.just("dog", "cat", "rabbit", "lion", "elephant");
        Mono<Map<Character, String>> animalMapMono = animalFlux.collectMap(s -> s.charAt(0));

        StepVerifier
                .create(animalMapMono)
                .expectNextMatches(map -> map.size() == 5 &&
                        map.get('d').equals("dog") &&
                        map.get('c').equals("cat") &&
                        map.get('r').equals("rabbit") &&
                        map.get('l').equals("lion") &&
                        map.get('e').equals("elephant"))
                .verifyComplete();
    }

}
