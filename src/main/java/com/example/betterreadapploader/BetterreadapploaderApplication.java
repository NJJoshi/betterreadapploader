package com.example.betterreadapploader;

import com.example.betterreadapploader.author.Author;
import com.example.betterreadapploader.author.AuthorRepository;
import com.example.betterreadapploader.book.Book;
import com.example.betterreadapploader.book.BookRepository;
import com.example.betterreadapploader.connection.DataStaxAstraProperties;
import io.netty.handler.codec.DateFormatter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadapploaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String workDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterreadapploaderApplication.class, args);
    }

    @PostConstruct
    public void start() {
        initAuthors();
        initWorks();
    }

    /**
     * This is necessary to have the Spring Boot app use the Astra secure bundle
     * to connect to the database
     */
    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

    private void initAuthors() {
        System.out.println("Started parsing Authors file dump");
        Path path = Paths.get(authorDumpLocation);
        try(Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                try {
                    //Read and parse the line
                    String jsonString = line.substring(line.indexOf("{"));
                    JSONObject jsonObject = new JSONObject(jsonString);

                    //Construct Author Object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/",""));

                    //Persist it using AuthorRepository
                    authorRepository.save(author);
                } catch(JSONException je) {
                    System.out.println("Exception occurred while parsing json string:" + line);
                    je.printStackTrace();
                }
            });
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
        System.out.println("Ends parsing Authors file dump");
    }

    private void initWorks() {
        System.out.println("Started parsing works file dump");
        Path path = Paths.get(workDumpLocation);
        DateTimeFormatter dt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try(Stream<String> lines = Files.lines(path)) {
            lines.limit(5).forEach(line -> {
                try {
                    //Read and parse the line
                    String jsonString = line.substring(line.indexOf("{"));
                    JSONObject jsonObject = new JSONObject(jsonString);

                    //Construct Book Object
                    Book book = new Book();
                    book.setId(jsonObject.getString("key").replace("/works/", ""));
                    book.setName(jsonObject.optString("title"));

                    JSONObject descJsonObj = jsonObject.optJSONObject("description");
                    if(descJsonObj != null) {
                        book.setDescription(descJsonObj.optString("value"));
                    }

                    JSONObject dtObj = jsonObject.optJSONObject("created");
                    if(dtObj != null) {
                        String value = dtObj.optString("value");
                        try {
                            book.setPublishDate( LocalDate.parse(value, dt));
                        } catch (DateTimeParseException pe) {
                            System.out.println("Unable to parse date value");
                            pe.printStackTrace();
                        }
                    }

                    JSONArray coversJsonObj = jsonObject.optJSONArray("covers");
                    if(coversJsonObj != null) {
                        List<String> coverIds = new ArrayList<>();
                        for(int i = 0; i < coversJsonObj.length(); i++) {
                            String val = coversJsonObj.getString(i);
                            coverIds.add(val);
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorArrObj = jsonObject.optJSONArray("authors");
                    if(authorArrObj != null) {
                        List<String> authorIds = new ArrayList<>();
                        for (int i = 0; i < authorArrObj.length(); i++) {
                            JSONObject indexObj = authorArrObj.getJSONObject(i);
                            JSONObject authorObj = indexObj.getJSONObject("author");
                            authorIds.add(authorObj.getString("key").replace("/authors/", ""));
                        }
                        book.setAuthorIds(authorIds);

                        List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                    if(!optionalAuthor.isPresent()) return "Unknown Author";
                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }
                    bookRepository.save(book);
                } catch(JSONException je) {
                    System.out.println("Exception occurred while parsing json string:" + line);
                    je.printStackTrace();
                }
            });
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
        System.out.println("Ends parsing works file dump");
    }
}
