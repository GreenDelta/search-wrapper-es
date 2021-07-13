package com.greendelta.search.wrapper.es;

import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.linearDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;

import com.greendelta.search.wrapper.Conjunction;
import com.greendelta.search.wrapper.LinearDecayFunction;
import com.greendelta.search.wrapper.MultiSearchFilter;
import com.greendelta.search.wrapper.SearchFilter;
import com.greendelta.search.wrapper.SearchFilterValue;
import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.aggregations.SearchAggregation;
import com.greendelta.search.wrapper.score.Score;

class Query {

	static QueryBuilder builder(SearchQuery searchQuery) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		QueryBuilder current = null;
		int count = 0;
		for (SearchFilter filter : searchQuery.getFilters()) {
			List<SearchAggregation> aggregations = searchQuery.getAggregationsByField(filter.field);
			QueryBuilder q = toQuery(filter, aggregations);
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
		QueryBuilder finalQuery = null;
		if (count == 0) {
			finalQuery = QueryBuilders.matchAllQuery();
		} else if (count == 1) {
			finalQuery = current;
		} else {
			finalQuery = query;
		}
		finalQuery = addScores(finalQuery, searchQuery);
		return finalQuery;
	}

	private static QueryBuilder addScores(QueryBuilder query, SearchQuery searchQuery) {
		if (searchQuery.getScores().isEmpty())
			return query;
		List<FilterFunctionBuilder> functions = new ArrayList<>();
		for (int i = 0; i < searchQuery.getScores().size(); i++) {
			Score score = searchQuery.getScores().get(i);
			functions.add(new FilterFunctionBuilder(scriptFunction(Script.from(score))));
		}
		for (int i = 0; i < searchQuery.getFunctions().size(); i++) {
			LinearDecayFunction function = searchQuery.getFunctions().get(i);
			functions.add(new FilterFunctionBuilder(linearDecayFunction(function.fieldName, function.origin,
					function.scale, function.offset, function.decay)));
		}
		return functionScoreQuery(query, functions.toArray(new FilterFunctionBuilder[functions.size()]));
	}

	private static QueryBuilder toQuery(SearchFilter filter, List<SearchAggregation> aggregations) {
		if (filter.values.isEmpty())
			return null;
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		QueryBuilder last = null;
		int count = 0;
		for (SearchFilterValue value : filter.values) {
			if (value.value instanceof String && value.value.toString().isEmpty())
				continue;
			if (aggregations.isEmpty()) {
				last = Query.builder(filter.field, value);
			} else if (aggregations.size() == 1) {
				last = Query.builder(aggregations.get(0), value);
			} else {
				BoolQueryBuilder aQuery = QueryBuilders.boolQuery();
				for (SearchAggregation aggregation : aggregations) {
					aQuery.must(Query.builder(aggregation, value));
				}
				last = aQuery;
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
				last = Query.builder(field, value);
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

	private static QueryBuilder builder(SearchAggregation aggregation, SearchFilterValue value) {
		QueryBuilder builder = createBuilder(aggregation.field, value);
		return decorate(builder, aggregation.field, value);
	}

	private static QueryBuilder builder(String field, SearchFilterValue value) {
		QueryBuilder builder = createBuilder(field, value);
		return decorate(builder, field, value);
	}

	private static QueryBuilder createBuilder(String field, SearchFilterValue value) {
		switch (value.type) {
		case TERM:
			return termsQuery(field, value);
		case PHRASE:
			return phraseQuery(field, value);
		case WILDCART:
			return wildcardQuery(field, value);
		case RANGE:
			return rangeQuery(field, value);
		default:
			return matchAllQuery();
		}
	}

	private static QueryBuilder termsQuery(String field, SearchFilterValue value) {
		if (!(value.value instanceof Collection))
			return QueryBuilders.termQuery(field, value.value);
		Collection<?> terms = (Collection<?>) value.value;
		return QueryBuilders.termsQuery(field, terms);
	}

	private static QueryBuilder phraseQuery(String field, SearchFilterValue value) {
		if (!(value.value instanceof Collection))
			return matchPhraseQuery(field, value.value);
		Collection<?> phrases = (Collection<?>) value.value;
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		for (Object p : phrases) {
			query.should(matchPhraseQuery(field, p));
		}
		return query;
	}

	private static QueryBuilder wildcardQuery(String field, SearchFilterValue value) {
		return QueryBuilders.wildcardQuery(field, value.value.toString());
	}

	private static QueryBuilder rangeQuery(String field, SearchFilterValue value) {
		Object[] v = (Object[]) value.value;
		return QueryBuilders.rangeQuery(field).from(v[0]).to(v[1]);
	}

	private static QueryBuilder nest(QueryBuilder builder, String field) {
		String path = field;
		while (path.contains(".")) {
			path = path.substring(0, path.lastIndexOf("."));
			builder = QueryBuilders.nestedQuery(path, builder, ScoreMode.Total);
		}
		return builder;
	}

	private static QueryBuilder decorate(QueryBuilder builder, String field, SearchFilterValue value) {
		if (value.boost != null) {
			builder = builder.boost(value.boost);
		}
		if (isNested(field)) {
			builder = nest(builder, field);
		}
		return builder;
	}

	private static boolean isNested(String field) {
		return field.contains(".");
	}

}
