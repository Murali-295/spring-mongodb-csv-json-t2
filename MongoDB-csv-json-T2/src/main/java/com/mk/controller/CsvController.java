package com.mk.controller;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

@RestController
public class CsvController {

	private MongoClient mongoClient = MongoClients.create();
	private MongoDatabase mongoDatabase;

	@GetMapping("/get")
	public String store() throws CsvValidationException, IOException {

		CSVReader csvReader = new CSVReader(new FileReader("MOVIES.csv"));
		String[] header = csvReader.readNext();

		for (String string : header) {

			System.out.println(string);
		}

		String[] data;

		mongoDatabase = mongoClient.getDatabase("movies_t2");
		while ((data = csvReader.readNext()) != null) {

			Map<String, String> map1 = new HashMap<>();
			for (int i = 0; i < data.length; i++) {
				map1.put(header[i], data[i]);
			}
			Document document = new Document();
			document.put(data[11], map1);

			mongoDatabase.getCollection("movies_t2").insertOne(document);
		}
		csvReader.close();
		return "Data stored successfully...";
	}

	@GetMapping("/returnData/{title}")
	public List<Document> getMovie(@PathVariable String title) {
		mongoDatabase = mongoClient.getDatabase("movies_t2");
		// to store the data and return to user
		List<Document> documents = new ArrayList<>();
		MongoCollection<Document> document = mongoDatabase.getCollection("movies_t2");
		// iterating over document to add data to list
		for (Document doc : document.find()) {
			for (String st : doc.keySet()) {
				if (st.equals(title)) {

					doc.remove("_id");
					documents.add(doc);
					break;
				}
			}
		}

		return documents;
	}

	@DeleteMapping("/delete/{title}")
	public String deleteMovie(@PathVariable String title) {
		mongoDatabase = mongoClient.getDatabase("movies_t2");
		MongoCollection<Document> documents = mongoDatabase.getCollection("movies_t2");
		documents.drop();
		return "Movie deleted successfully...";
	}

	@PostMapping("/add")
	public String addMovie(@RequestBody Document document) {
		mongoDatabase = mongoClient.getDatabase("movies_t2");
		MongoCollection<Document> documents = mongoDatabase.getCollection("movies_t2");
		documents.insertOne(document);
		return "Movie added successfully...";
	}

	@GetMapping("/json")
	public String getJson() throws CsvValidationException, IOException {
		File file = new File("movies.json");
		CSVReader csvReader1 = new CSVReader(new FileReader("MOVIES.csv"));
		String[] header = csvReader1.readNext();

		String[] data;
		HashMap<String, Object> map = new HashMap<>();

		while ((data = csvReader1.readNext()) != null) {

			HashMap<String, String> map1 = new HashMap<>();
			for (int i = 0; i < data.length; i++) {
				map1.put(header[i], data[i]);
			}
			map.put(data[11], map1);
		}
		Document doc = new Document(map);
		Writer writer = new FileWriter(file);
		writer.append(doc.toJson());
		writer.close();
		csvReader1.close();
		return "converted to json successfully...";
	}

}
