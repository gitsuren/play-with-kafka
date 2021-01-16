package com.surendra.kakfaoffsetmgmt;

import com.surendra.kakfaoffsetmgmt.dao.BookRepository;
import com.surendra.kakfaoffsetmgmt.domain.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.annotation.PostConstruct;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

@SpringBootApplication
public class KakfaOffsetMgmtApplication implements CommandLineRunner {

    @Autowired
    private BookRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final Logger log = LoggerFactory.getLogger(KakfaOffsetMgmtApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KakfaOffsetMgmtApplication.class, args);
    }

    @Override
    public void run(String... args) {

        log.info("StartApplication...");

        repository.save(new Book("Java"));
        repository.save(new Book("Node"));
        repository.save(new Book("Python"));

        System.out.println("\nfindAll()");
        repository.findAll().forEach(x -> System.out.println(x));

        System.out.println("\nfindById(1L)");
        repository.findById(1l).ifPresent(x -> System.out.println(x));

        System.out.println("\nfindByName('Node')");
        repository.findByName("Node").forEach(x -> System.out.println(x));

    }

    @PostConstruct
    private void initDB() {
        System.out.println("*********** create table tss_data and tss_offset and insert test data *****************");
        String sqlStmt[] = {
                "drop table tss_data if exists",
                "create table tss_data (skey varchar(50), svalue varchar(50), topic_name varchar(50), partitionn int, offsett int, created_date timestamp default CURRENT_TIMESTAMP)",
                "drop table tss_offsets if exists",
                "create table tss_offsets(topic_name varchar(50), partitionn int, offsett int)",
                "insert into tss_offsets values('SensorTopic', 0, 0)",
                "insert into tss_offsets values('SensorTopic', 1, 0)",
                "insert into tss_offsets values('SensorTopic', 2, 0)",
        };

        Arrays.asList(sqlStmt)
                .stream()
                .forEach(
                        sql ->
                        {
                            System.out.println(sql);
                            jdbcTemplate.execute(sql);
                        }
                );

        System.out.println("*******Fetching the table tss_data and tss_offset********");

        jdbcTemplate.query("select skey, svalue from tss_data",
                (rs, i) -> {
                    System.out.println(String.format("skey:%s, sValue:%s", rs.getString("skey"), rs.getString("svalue")));
                    return null;
                });

        jdbcTemplate.query("select topic_name, partitionn, offsett from tss_offsets",
                new RowMapper<Object>() {
                    @Override
                    public Object mapRow(ResultSet rs, int i) throws SQLException {
                        System.out.println(String.format("topic_name:%s, partition: %s, offset: %S",
                                rs.getString("topic_name"),
                                rs.getString("partitionn"),
                                rs.getString("offsett")));
                        return null;
                    }
                });
    }
}
