package com.example.reactordemo;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.stream.Stream;

@SpringBootTest
class ReactorDemoApplicationTests {

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

}
