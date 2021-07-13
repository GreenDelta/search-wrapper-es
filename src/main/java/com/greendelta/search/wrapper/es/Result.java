package com.greendelta.search.wrapper.es;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

import com.greendelta.search.wrapper.SearchFilterType;
import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.SearchResult;
import com.greendelta.search.wrapper.aggregations.RangeAggregation;
import com.greendelta.search.wrapper.aggregations.TermsAggregation;
import com.greendelta.search.wrapper.aggregations.results.AggregationResult;
import com.greendelta.search.wrapper.aggregations.results.AggregationResultBuilder;
import com.greendelta.search.wrapper.aggregations.results.AggregationResultEntry;
import com.greendelta.search.wrapper.es.Search.EsResponse;

class Result {

	private static final String RANGE_TYPE = "range";
	private static final String NESTED_TYPE = "nested";

	static List<AggregationResult> aggregations(EsResponse response) {
		return addAggregations(response, response.getAggregations());
	}

	static List<AggregationResult> addAggregations(EsResponse response, List<Aggregation> aggreagtions) {
		List<AggregationResult> results = new ArrayList<>();
		for (Aggregation aggregation : aggreagtions) {
			if (!aggregation.getType().equals(NESTED_TYPE)) {
				results.add(addAggregation(response, aggregation));
			} else {
				results.addAll(addAggregations(response, ((ParsedNested) aggregation).getAggregations().asList()));
			}
		}
		return results;
	}

	private static AggregationResult addAggregation(EsResponse response, Aggregation aggregation) {
		AggregationResultBuilder builder = new AggregationResultBuilder();
		builder.name(aggregation.getName()).type(mapType(aggregation.getType()));
		putEntries(response, aggregation, builder);
		return builder.build();
	}

	private static SearchFilterType mapType(String type) {
		if (type == null)
			return null;
		switch (type) {
		case StringTerms.NAME:
		case LongTerms.NAME:
		case DoubleTerms.NAME:
			return TermsAggregation.TYPE;
		case RANGE_TYPE:
			return RangeAggregation.TYPE;
		default:
			return SearchFilterType.UNKNOWN;
		}
	}

	private static void putEntries(EsResponse response, Aggregation aggregation, AggregationResultBuilder builder) {
		long totalCount = 0;
		switch (aggregation.getType()) {
		case StringTerms.NAME:
		case LongTerms.NAME:
		case DoubleTerms.NAME:
			for (Bucket bucket : response.getTermBuckets(aggregation)) {
				long count = getCount(bucket.getDocCount(), bucket.getAggregations().asList());
				builder.addEntry(new AggregationResultEntry(bucket.getKeyAsString(), count));
				totalCount += count;
			}
			builder.totalCount(totalCount);
			break;
		case RANGE_TYPE:
			for (org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket : response
					.getRangeBuckets(aggregation)) {
				Object[] data = new Object[] { bucket.getFrom(), bucket.getTo() };
				long count = getCount(bucket.getDocCount(), bucket.getAggregations().asList());
				builder.addEntry(new AggregationResultEntry(bucket.getKeyAsString(), count, data));
				totalCount += count;
			}
			builder.totalCount(totalCount);
			break;
		}
	}

	private static long getCount(long bucketCount, List<Aggregation> aggregations) {
		for (Aggregation aggregation : aggregations)
			if (aggregation instanceof ParsedReverseNested)
				return ((ParsedReverseNested) aggregation).getDocCount();
		return bucketCount;
	}

	static void extend(SearchResult<Map<String, Object>> result, long totalHits,
			SearchQuery searchQuery) {
		result.resultInfo.totalCount = totalHits;
		result.resultInfo.currentPage = searchQuery.getPage();
		result.resultInfo.pageSize = searchQuery.getPageSize();
		long totalCount = result.resultInfo.totalCount;
		if (searchQuery.getPageSize() != 0) {
			int pageCount = (int) totalCount / searchQuery.getPageSize();
			if ((totalCount % searchQuery.getPageSize()) != 0) {
				pageCount = 1 + pageCount;
			}
			result.resultInfo.pageCount = pageCount;
		}
	}

}
