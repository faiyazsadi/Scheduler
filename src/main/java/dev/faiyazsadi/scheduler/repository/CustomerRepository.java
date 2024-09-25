package dev.faiyazsadi.scheduler.repository;

import dev.faiyazsadi.scheduler.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer, Long> {
}
