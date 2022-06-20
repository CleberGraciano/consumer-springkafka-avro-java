package br.com.springkafka.consumer;

import br.com.springkafka.Car;
import br.com.springkafka.People;
import br.com.springkafka.domain.Book;
import br.com.springkafka.repositories.CarRepository;
import br.com.springkafka.repositories.PeopleRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

@Slf4j
@Component
@AllArgsConstructor
@KafkaListener(topics = "${topic.name}")
public class KafkaConsumer {

    private final PeopleRepository peopleRepository;
    private final CarRepository carRepository;

    @KafkaHandler(isDefault = false)
    public void consumer(People people, Acknowledgment ack){


        log.info(people.toString());

        var peopleEntity = br.com.springkafka.domain.People.builder().build();

        peopleEntity.setId(people.getId().toString());
        peopleEntity.setName(people.getName().toString());
        peopleEntity.setCpf(people.getCpf().toString());
        peopleEntity.setBooks(people.getBooks().stream().map(book->
                        Book.builder().people(peopleEntity).name(book.toString()).build()
                ).collect(Collectors.toList())
        );

        peopleRepository.save(peopleEntity);

        ack.acknowledge();
    }

    @KafkaHandler(isDefault = false)
    public void consumerCar(Car car, Acknowledgment ack) {

        var carEntity = br.com.springkafka.domain.Car.builder().build();

        carEntity.setId(car.getId().toString());
        carEntity.setName(car.getName().toString());
        carEntity.setModel(car.getModel().toString());
        carEntity.setBrand(car.getBrand().toString());

        carRepository.save(carEntity);

        log.info("Message consumida!!"+car);
        ack.acknowledge();
    }

    @KafkaHandler(isDefault = true)
    public void consumerObject(Object object, Acknowledgment ack) {
        log.info("Message falha!!"+object);
        ack.acknowledge();
    }

}
