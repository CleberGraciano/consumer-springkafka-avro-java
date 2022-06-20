package br.com.springkafka.repositories;

import br.com.springkafka.domain.People;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PeopleRepository extends JpaRepository<People, String> {


}
