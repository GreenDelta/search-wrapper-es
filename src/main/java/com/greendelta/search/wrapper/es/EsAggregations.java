package com.greendelta.search.wrapper.es;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import com.greendelta.search.wrapper.SearchFilterValue;
import com.greendelta.search.wrapper.aggregations.RangeAggregation;
import com.greendelta.search.wrapper.aggregations.SearchAggregation;
import com.greendelta.search.wrapper.aggregations.TermsAggregation;

class EsAggregations {

	static QueryBuilder getQuery(SearchAggregation aggregation, SearchFilterValue value) {
		switch (aggregation.type) {
		case TermsAggregation.TYPE:
			return getQuery((TermsAggregation) aggregation, value);
		case RangeAggregation.TYPE:
			return getQuery((RangeAggregation) aggregation, value);
		default:
			return null;
		}
	}

	static AggregationBuilder getBuilder(SearchAggregation aggregation) {
		switch (aggregation.type) {
		case TermsAggregation.TYPE:
			return getBuilder((TermsAggregation) aggregation);
		case RangeAggregation.TYPE:
			return getBuilder((RangeAggregation) aggregation);
		default:
			return null;
		}
	}

	private static TermQueryBuilder getQuery(TermsAggregation aggregation, SearchFilterValue value) {
		return QueryBuilders.termQuery(aggregation.field, value.value.toString());
	}

	private static RangeQueryBuilder getQuery(RangeAggregation aggregation, SearchFilterValue value) {
		Object[] v = (Object[]) value.value;
		return QueryBuilders.rangeQuery(aggregation.field).from(v[0]).to(v[1]);
	}

	private static TermsAggregationBuilder getBuilder(TermsAggregation aggregation) {
		return AggregationBuilders.terms(aggregation.name).field(aggregation.field).size(Integer.MAX_VALUE);
	}

	private static RangeAggregationBuilder getBuilder(RangeAggregation aggregation) {
		RangeAggregationBuilder builder = AggregationBuilders.range(aggregation.name).field(aggregation.field);
		for (Double[] range : aggregation.ranges) {
			if (range[0] == null) {
				builder.addUnboundedTo(range[1]);
			} else if (range[1] == null) {
				builder.addUnboundedFrom(range[0]);
			} else {
				builder.addRange(range[0], range[1]);
			}
		}
		return builder;
	}

}
