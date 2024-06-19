package com.example.SpringAzureESDemoApp.controller;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.SpringAzureESDemoApp.service.ESService;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;

@RestController
@RequestMapping("/main")
@CrossOrigin(origins = "*")
public class ESController {
	
	@Autowired
	private ElasticsearchClient client;
	
	@Autowired
	private ESService service;
	
	@GetMapping("/testConnection")
	public ResponseEntity<String> testConnection() {
	    try {
	        // Execute a simple search query
	         SearchResponse<Object> response = client.search(SearchRequest.of(s -> s.index("ipl_data").size(1)), Object.class);
	        return ResponseEntity.ok("Connection successful. Found " + response.hits().total().value() + " hits.");
	    } catch (Exception e) {
	        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
	    }
	}
	
	@GetMapping("/searchData")
	public ResponseEntity<List<String>> searchByField() {
	    try {
	        String index = "ipl_data";
	        String fieldName = "city";
	        String searchValue = "Pune";

	        List<String> resultSources = service.searchByMatchQuery(index, fieldName, searchValue);

	        return ResponseEntity.ok(resultSources);
	    } catch (IOException e) {
	        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
	                .body(Collections.singletonList("Error searching: " + e.getMessage()));
	    }
	}
	
	@GetMapping("/venue")
	public List<String> getVenues() throws ElasticsearchException, IOException{
		List<String> venues=service.getUniqueVenue();
		
		return venues;
	}
	
	@GetMapping("/getAllSeason")
	public List<Long> getAllSeasons() throws ElasticsearchException, IOException{
		List<Long> season=service.getAllSeasons();
		System.out.println("season -> "+season);
		return season;
	}
	
	@GetMapping("/season")
	public List<Long> getSeasons() throws ElasticsearchException, IOException{
		List<Long> season=service.getAllSeasons();
		
		return season;
	}

	@GetMapping("/getAllTeams")
	public List<String> getAllTeams() throws ElasticsearchException, IOException{
		List<String> teams=service.getAllTeams();
		
		return teams;
	}
	
	@GetMapping("/team")
	public List<String> getTeams() throws ElasticsearchException, IOException{
		List<String> teams=service.getAllTeams();
		
		return teams;
	}
	
	@GetMapping("/city")
	public List<String> getCity() throws ElasticsearchException, IOException{
		List<String> city=service.getAllCities();
		
		return city;
	}
	
	@GetMapping("/umpire")
	public List<String> getUmpire() throws ElasticsearchException, IOException{
		List<String> umpire=service.getAllUmpire();
		
		return umpire;
	}
	
	@GetMapping("/getAllKind")
	public List<String> getAllKind() throws ElasticsearchException, IOException{
		List<String> kind=service.getAllKinds();
		
		return kind;
	}
}
