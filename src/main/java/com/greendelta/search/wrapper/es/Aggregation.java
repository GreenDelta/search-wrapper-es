package com.greendelta.search.wrapper.es;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;

import com.greendelta.search.wrapper.aggregations.RangeAggregation;
import com.greendelta.search.wrapper.aggregations.SearchAggregation;
import com.greendelta.search.wrapper.aggregations.TermsAggregation;

class Aggregation {

	static AggregationBuilder builder(SearchAggregation aggregation) {
		AggregationBuilder builder = createBuilder(aggregation);
		if (isNested(aggregation.field)) {
			builder = nest(builder, aggregation);
		}
		return builder;
	}

	private static AggregationBuilder createBuilder(SearchAggregation aggregation) {
		switch (aggregation.type) {
		case TERM:
			return termsBuilder((TermsAggregation) aggregation);
		case RANGE:
			return rangeBuilder((RangeAggregation) aggregation);
		default:
			return null;
		}
	}

	private static AggregationBuilder termsBuilder(TermsAggregation aggregation) {
		return AggregationBuilders.terms(aggregation.name).field(aggregation.field).size(Integer.MAX_VALUE);
	}

	private static AggregationBuilder rangeBuilder(RangeAggregation aggregation) {
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

	private static AggregationBuilder nest(AggregationBuilder builder, SearchAggregation aggregation) {
		String path = aggregation.field;
		String name = aggregation.name;
		builder.subAggregation(AggregationBuilders.reverseNested(name + "-r"));
		while (path.contains(".")) {
			name += "-n";
			path = path.substring(0, path.lastIndexOf("."));
			builder = AggregationBuilders.nested(name, path).subAggregation(builder);
		}
		return builder;
	}

	private static boolean isNested(String field) {
		return field.contains(".");
	}

}
