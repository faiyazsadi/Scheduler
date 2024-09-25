package dev.faiyazsadi.scheduler.processor;

import dev.faiyazsadi.scheduler.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public class CustomItemProcessor implements ItemProcessor<Customer, Customer> {
    @Override
    public Customer process(Customer customer) throws Exception {
        return customer;
    }
}
