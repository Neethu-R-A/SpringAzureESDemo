package com.example.SpringAzureESDemoApp.config;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@Configuration("es")
public class ESClient {

//	private String hostname = "https://my-deployment.es.centralindia.azure.elastic-cloud.com";
//    private String apiKeyId = "uH8yBpABKnNa1xTT2EmW";
//    private String apiKeySecret = "aQzPgblYQy2xGwjqfWOp3Q";

    private String hostname="https://benelux-elasticsearch.es.northeurope.azure.elastic-cloud.com";
    private String apiKeyId = "FnXmkZAB91y0iX8f1bJx";
    private String apiKeySecret = "z4av4lWwQ4qRqmw_nLOeLg";
    
    @Bean
    public ElasticsearchClient getElasticSearchClient() {
        String apiKeyAuth = apiKeyId + ":" + apiKeySecret;
        String base64ApiKey = Base64.getEncoder().encodeToString(apiKeyAuth.getBytes(StandardCharsets.UTF_8));
        
        RestClient restClient = RestClient.builder(HttpHost.create(hostname))
            .setDefaultHeaders(new BasicHeader[]{ new BasicHeader("Authorization", "ApiKey " + base64ApiKey) })
            .build();

        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        return new ElasticsearchClient(transport);
    }
}
