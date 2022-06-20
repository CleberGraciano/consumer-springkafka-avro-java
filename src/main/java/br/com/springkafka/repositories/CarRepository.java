package br.com.springkafka.repositories;

import br.com.springkafka.domain.Car;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CarRepository extends JpaRepository<Car, String> {


}
