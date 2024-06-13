package com.example.SpringAzureESDemoApp.service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;

@Service
public class ESService {

	@Autowired
	private ElasticsearchClient client;
	
	public  List<String> searchByMatchQuery(String index, String fieldName, String searchValue) throws IOException {
	    SearchResponse<Object> searchResponse = client.search(s ->
	            s.index(index)
	             .query(q -> q.match(m -> m.field(fieldName).query(searchValue))),
	            Object.class);

	    return searchResponse.hits().hits().stream()
	            .map(hit -> hit.source().toString())
	            .collect(Collectors.toList());
	}
}
