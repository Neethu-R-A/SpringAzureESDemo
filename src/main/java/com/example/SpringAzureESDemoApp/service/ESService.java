package com.example.SpringAzureESDemoApp.service;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.util.NamedValue;

@Service
public class ESService {

	@Autowired
	private ElasticsearchClient client;

	public List<String> searchByMatchQuery(String index, String fieldName, String searchValue) throws IOException {
		SearchResponse<Object> searchResponse = client.search(
				s -> s.index(index).query(q -> q.match(m -> m.field(fieldName).query(searchValue))), Object.class);

		return searchResponse.hits().hits().stream().map(hit -> hit.source().toString()).collect(Collectors.toList());
	}

	public List<String> getUniqueVenue() throws ElasticsearchException, IOException {
		SearchResponse<Object> searchResponse = client.search(
				s -> s.index("ipl_data").size(0).aggregations("unique_venues", ag -> ag.terms(t -> t.field("venue"))),
				Object.class);

		return searchResponse.aggregations().get("unique_venues").sterms().buckets().array().stream()
				.map(bucket -> bucket.key().stringValue()).collect(Collectors.toList());
	}

	public List<Long> getAllSeasons() throws ElasticsearchException, IOException {
		SearchResponse<Object> searchResponse = client.search(
				s -> s.index("ipl_data").size(0).aggregations("unique_seasons",
						ag -> ag.terms(t -> t.field("season")
								.order(new NamedValue<>("_key", SortOrder.Asc)))),
				Object.class);

		return searchResponse.aggregations().get("unique_seasons").lterms().buckets().array().stream().map(b -> b.key())
				.collect(Collectors.toList());

//		return searchResponse.aggregations().get("unique_seasons").lterms().buckets().array().stream().map(b -> Long.toString(b.key())).collect(Collectors.toList());
	}
	
	public List<String> getAllTeams() throws ElasticsearchException, IOException{
		SearchResponse<Object> search=client.search(s->s.index("ipl_data").size(0)
				.aggregations("unique_teams",ag->ag.terms(t->t.field("winningTeam")))
				, Object.class);
		
		return search.aggregations().get("unique_teams").sterms().buckets().array().stream().map(b->b.key().stringValue()).collect(Collectors.toList());
	}
	
	public List<String> getAllCities() throws ElasticsearchException, IOException{
		SearchResponse<Object> search=client.search(s->s.index("ipl_data").size(0)
				.aggregations("city",ag->ag.terms(t->t.field("city")))
				, Object.class);
		
		return search.aggregations().get("city").sterms().buckets().array().stream().map(b->b.key().stringValue()).collect(Collectors.toList());
	
	}
	
	public List<String> getAllUmpire() throws ElasticsearchException, IOException{
		SearchResponse<Object> search1=client.search(s->s.index("ipl_data").size(0)
				.aggregations("umpire1",ag->ag.terms(t->t.field("umpire1")))
				, Object.class);
		
		SearchResponse<Object> search2=client.search(s->s.index("ipl_data").size(0)
				.aggregations("umpire2",ag->ag.terms(t->t.field("umpire2")))
				, Object.class);
		
		List<String> umpire1 = search1.aggregations().get("umpire1").sterms().buckets().array().stream().map(b->b.key().stringValue()).collect(Collectors.toList());
		
		List<String> umpire2 = search2.aggregations().get("umpire2").sterms().buckets().array().stream().map(b->b.key().stringValue()).collect(Collectors.toList());
	
//		umpire2.addAll(umpire1);
		
		Set<String> umpire1Set = new HashSet<String>(umpire1);
		Set<String> umpire2Set = new HashSet<String>(umpire2);
		umpire2Set.addAll(umpire1Set);
		
		List<String> combinedUmpires = umpire2Set.stream().collect(Collectors.toList());
		
		return combinedUmpires;
	}
	
	public List<String> getAllKinds() throws ElasticsearchException, IOException{
		SearchResponse<Object> search=client.search(s->s.index("ipl_data").size(0)
				.aggregations("kind",ag->ag.terms(t->t.field("kind")))
				, Object.class);
		
		return search.aggregations().get("kind").sterms().buckets().array().stream().map(b->b.key().stringValue()).collect(Collectors.toList());
	
	}

}
