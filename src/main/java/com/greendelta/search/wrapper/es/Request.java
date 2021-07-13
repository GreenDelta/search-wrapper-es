package com.greendelta.search.wrapper.es;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.greendelta.search.wrapper.es.Search.EsRequest;

class Request implements EsRequest {

	private final SearchRequestBuilder request;

	Request(Client client, String indexName) {
		request = client.prepareSearch(indexName);
	}

	@Override
	public void setFrom(int from) {
		request.setFrom(from);
	}

	@Override
	public void setSize(int size) {
		request.setSize(size);
	}

	@Override
	public void addSort(String field, SortOrder order) {
		request.addSort(field, order);
	}

	@Override
	public void addAggregation(AggregationBuilder aggregation) {
		request.addAggregation(aggregation);
	}

	@Override
	public void setQuery(QueryBuilder query) {
		request.setQuery(query);
	}

	@Override
	public Response execute() throws IOException {
		return new Response(request.execute().actionGet());
	}

}
