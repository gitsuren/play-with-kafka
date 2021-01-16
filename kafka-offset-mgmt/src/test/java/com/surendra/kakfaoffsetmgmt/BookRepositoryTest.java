package com.surendra.kakfaoffsetmgmt;

import com.surendra.kakfaoffsetmgmt.dao.BookRepository;
import com.surendra.kakfaoffsetmgmt.domain.Book;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@DataJpaTest
public class BookRepositoryTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private BookRepository repository;

    @Test
    public void testFindByName() {

        entityManager.persist(new Book("C++"));

        List<Book> books = repository.findByName("C++");
        assertEquals(1, books.size());

        assertThat(books).extracting(Book::getName).containsOnly("C++");

    }

}
