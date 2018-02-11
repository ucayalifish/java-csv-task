package ru.ffyud.trials.csvreporter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.ffyud.trials.csvdata.DataService;

@SpringBootApplication
public class CsvReporterApplication {
  @Bean
  DataService dataService(@Autowired JdbcTemplate jdbcTemplate) {
    return new DataService(jdbcTemplate);
  }

  public static void main(String[] args) {
    SpringApplication.run(CsvReporterApplication.class, args);
  }
}
