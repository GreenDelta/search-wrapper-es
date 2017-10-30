package com.greendelta.search.wrapper.es;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.sort.SortOrder;

import com.greendelta.search.wrapper.Conjunction;
import com.greendelta.search.wrapper.MultiSearchFilter;
import com.greendelta.search.wrapper.SearchFilter;
import com.greendelta.search.wrapper.SearchFilterValue;
import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.SearchResult;
import com.greendelta.search.wrapper.SearchSorting;
import com.greendelta.search.wrapper.aggregations.SearchAggregation;
import com.greendelta.search.wrapper.aggregations.results.AggregationResult;
import com.greendelta.search.wrapper.aggregations.results.AggregationResultBuilder;
import com.greendelta.search.wrapper.aggregations.results.TermEntryBuilder;

class EsSearch {

	static SearchResult<Map<String, Object>> search(SearchQuery searchQuery, Client client, String indexName) {
		try {
			SearchRequestBuilder request = client.prepareSearch(indexName);
			setupPaging(request, searchQuery);
			setupSorting(request, searchQuery);
			setupQuery(request, searchQuery);
			return search(request, searchQuery);
		} catch (Exception e) {
			e.printStackTrace();
			return new SearchResult<>();
		}
	}

	private static void setupPaging(SearchRequestBuilder request, SearchQuery searchQuery) {
		int start = (searchQuery.getPage() - 1) * searchQuery.getPageSize();
		if (start > 0) {
			request.setFrom(start);
		}
		if (!searchQuery.isPaged()) {
			request.setSize(10000);
		} else {
			if (searchQuery.getPageSize() > 0) {
				request.setSize(searchQuery.getPageSize());
			} else {
				request.setSize(SearchQuery.DEFAULT_PAGE_SIZE);
			}
		}
	}

	private static void setupSorting(SearchRequestBuilder request, SearchQuery searchQuery) {
		for (Entry<String, SearchSorting> entry : searchQuery.getSortBy().entrySet()) {
			SortOrder value = entry.getValue() == SearchSorting.ASC ? SortOrder.ASC : SortOrder.DESC;
			request.addSort(entry.getKey(), value);
		}
	}

	private static void setupQuery(SearchRequestBuilder request, SearchQuery searchQuery) {
		for (SearchAggregation aggregation : searchQuery.getAggregations()) {
			request.addAggregation(EsAggregations.getBuilder(aggregation));
		}
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		QueryBuilder current = null;
		int count = 0;
		for (SearchFilter filter : searchQuery.getFilters()) {
			SearchAggregation aggregation = searchQuery.getAggregation(filter.field);
			QueryBuilder q = toQuery(filter, aggregation);
			if (q == null)
				continue;
			count++;
			current = q;
			query.must(q);
		}
		for (MultiSearchFilter filter : searchQuery.getMultiFilters()) {
			QueryBuilder q = toQuery(filter);
			if (q == null)
				continue;
			count++;
			current = q;
			query.must(q);
		}
		if (count == 0) {
			request.setQuery(QueryBuilders.matchAllQuery());
		} else if (count == 1) {
			request.setQuery(current);
		} else {
			request.setQuery(query);
		}
	}

	private static QueryBuilder toQuery(SearchFilter filter, SearchAggregation aggregation) {
		if (filter.values.isEmpty())
			return null;
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		QueryBuilder last = null;
		int count = 0;
		for (SearchFilterValue value : filter.values) {
			if (value.value instanceof String && value.value.toString().isEmpty())
				continue;
			if (aggregation == null) {
				last = getQuery(filter.field, value);
			} else {
				last = EsAggregations.getQuery(aggregation, value.value.toString());
			}
			if (filter.conjunction == Conjunction.AND) {
				query.must(last);
			} else if (filter.conjunction == Conjunction.OR) {
				query.should(last);
			}
			count++;
		}
		if (count == 0)
			return null;
		if (count == 1)
			return last;
		return query;
	}

	private static QueryBuilder toQuery(MultiSearchFilter filter) {
		if (filter.values.isEmpty())
			return null;
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		QueryBuilder last = null;
		int count = 0;
		for (String field : filter.fields) {
			int innerCount = 0;
			BoolQueryBuilder inner = QueryBuilders.boolQuery();
			for (SearchFilterValue value : filter.values) {
				if (value.value instanceof String && value.value.toString().isEmpty())
					continue;
				last = getQuery(field, value);
				if (filter.conjunction == Conjunction.AND) {
					inner.must(last);
				} else if (filter.conjunction == Conjunction.OR) {
					inner.should(last);
				}
				innerCount++;
			}
			if (innerCount == 1) {
				query.should(last);
			} else {
				query.should(inner);
				last = inner;
			}
			count++;
		}
		if (count == 0)
			return null;
		if (count == 1)
			return last;
		return query;
	}

	private static QueryBuilder getQuery(String field, SearchFilterValue value) {
		switch (value.type) {
		case FROM:
			return rangeQuery(field).from(value.value);
		case TO:
			return rangeQuery(field).to(value.value);
		case WILDCART:
			return wildcardQuery(field, value.value.toString());
		case PHRASE:
			if (!(value.value instanceof Collection))
				return matchPhraseQuery(field, value.value);
			Collection<?> values = (Collection<?>) value.value;
			BoolQueryBuilder query = QueryBuilders.boolQuery();
			for (Object v : values) {
				query.should(matchPhraseQuery(field, v));
			}
			return query;
		default:
			return matchAllQuery();
		}
	}

	private static SearchResult<Map<String, Object>> search(SearchRequestBuilder request, SearchQuery searchQuery) {
		SearchResult<Map<String, Object>> result = new SearchResult<>();
		SearchResponse response = null;
		boolean doContinue = true;
		long totalHits = 0;
		while (doContinue) {
			if (!searchQuery.isPaged()) {
				request.setFrom(result.data.size());
			}
			response = request.execute().actionGet();
			SearchHit[] hits = response.getHits().getHits();
			for (SearchHit hit : hits) {
				result.data.add(hit.getSource());
			}
			totalHits = response.getHits().getTotalHits();
			doContinue = !searchQuery.isPaged() && result.data.size() != totalHits;
		}
		if (response.getAggregations() != null) {
			for (Aggregation aggregation : response.getAggregations().asList()) {
				result.aggregations.add(toResult(aggregation));
			}
		}
		result.resultInfo.count = result.data.size();
		extendResultInfo(result, totalHits, searchQuery);
		return result;
	}

	private static AggregationResult toResult(Aggregation aggregation) {
		AggregationResultBuilder builder = new AggregationResultBuilder();
		builder.name(aggregation.getName()).type(mapType(aggregation.getType()));
		putSpecificData(aggregation, builder);
		return builder.build();
	}

	private static String mapType(String type) {
		if (type == null)
			return null;
		switch (type) {
		case StringTerms.NAME:
			return "TERM";
		default:
			return "UNKNOWN";
		}
	}

	private static void putSpecificData(Aggregation aggregation, AggregationResultBuilder builder) {
		switch (aggregation.getType()) {
		case StringTerms.NAME:
			StringTerms terms = (StringTerms) aggregation;
			long totalCount = 0;
			for (Bucket bucket : terms.getBuckets()) {
				TermEntryBuilder entryBuilder = new TermEntryBuilder();
				entryBuilder.key(bucket.getKeyAsString()).count(bucket.getDocCount());
				builder.addEntry(entryBuilder.build());
				totalCount += bucket.getDocCount();
			}
			builder.totalCount(totalCount);
			break;
		}
	}

	private static void extendResultInfo(SearchResult<Map<String, Object>> result, long totalHits,
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
