package com.greendelta.lca.search.elasticsearch;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import com.greendelta.lca.search.aggregations.SearchAggregation;
import com.greendelta.lca.search.aggregations.TermsAggregation;

class EsAggregations {

	static TermQueryBuilder getQuery(SearchAggregation aggregation, String value) {
		switch (aggregation.type) {
		case TermsAggregation.TYPE:
			return getQuery((TermsAggregation) aggregation, value);
		default:
			return null;
		}
	}
	
	static TermsAggregationBuilder getBuilder(SearchAggregation aggregation) {
		switch (aggregation.type) {
		case TermsAggregation.TYPE:
			return getBuilder((TermsAggregation) aggregation);
		default:
			return null;
		}		
	}

	private static TermQueryBuilder getQuery(TermsAggregation aggregation, String value) {
		return QueryBuilders.termQuery(aggregation.field, value);
	}

	private static TermsAggregationBuilder getBuilder(TermsAggregation aggregation) {
		return AggregationBuilders.terms(aggregation.name).field(aggregation.field);
	}

}
