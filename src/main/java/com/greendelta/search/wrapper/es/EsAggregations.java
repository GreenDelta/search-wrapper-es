package com.greendelta.search.wrapper.es;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import com.greendelta.search.wrapper.aggregations.SearchAggregation;
import com.greendelta.search.wrapper.aggregations.TermsAggregation;

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
		return AggregationBuilders.terms(aggregation.name).field(aggregation.field).size(Integer.MAX_VALUE);
	}

}
