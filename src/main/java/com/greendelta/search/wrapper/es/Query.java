package com.greendelta.search.wrapper.es;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.index.query.functionscore.LinearDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;

import com.greendelta.search.wrapper.Conjunction;
import com.greendelta.search.wrapper.MultiSearchFilter;
import com.greendelta.search.wrapper.SearchFilterValue;
import com.greendelta.search.wrapper.SearchQuery;

class Query {

	static QueryBuilder create(SearchQuery searchQuery) {
		BoolQueryBuilder bool = QueryBuilders.boolQuery();
		searchQuery.getFilters().forEach(filter -> {
			QueryBuilder query = create(filter.field, filter.conjunction, filter.values);
			append(bool, query, Conjunction.AND);
		});
		searchQuery.getMultiFilters().forEach(filter -> {
			QueryBuilder query = query(filter);
			append(bool, query, Conjunction.AND);
		});
		QueryBuilder query = simplify(bool);
		if (query == null) {
			query = QueryBuilders.matchAllQuery();
		}
		return score(query, searchQuery);
	}

	private static QueryBuilder create(String field, Conjunction conjunction, Set<SearchFilterValue> values) {
		if (values.isEmpty())
			return null;
		BoolQueryBuilder bool = QueryBuilders.boolQuery();
		values.forEach(value -> {
			QueryBuilder query = create(field, value);
			append(bool, query, conjunction);
		});
		return simplify(bool);
	}

	private static QueryBuilder query(MultiSearchFilter filter) {
		if (filter.values.isEmpty())
			return null;
		BoolQueryBuilder bool = QueryBuilders.boolQuery();
		filter.fields.forEach(field -> {
			QueryBuilder query = create(field, filter.conjunction, filter.values);
			append(bool, query, Conjunction.OR);
		});
		return simplify(bool);
	}

	private static void append(BoolQueryBuilder boolQuery, QueryBuilder query, Conjunction conjunction) {
		if (query == null)
			return;
		if (conjunction == Conjunction.AND) {
			boolQuery.must(query);
		} else if (conjunction == Conjunction.OR) {
			boolQuery.should(query);
		}
	}

	private static QueryBuilder simplify(BoolQueryBuilder query) {
		int queries = query.must().size() + query.should().size();
		if (queries == 0)
			return null;
		if (queries == 1 && query.must().isEmpty())
			return query.should().get(0);
		if (queries == 1 && query.should().isEmpty())
			return query.must().get(0);
		return query;
	}

	private static QueryBuilder create(String field, SearchFilterValue value) {
		QueryBuilder builder = builder(field, value);
		if (builder == null)
			return null;
		return decorate(builder, field, value);
	}

	private static QueryBuilder builder(String field, SearchFilterValue value) {
		if (value.value == null || value.value.toString().isEmpty())
			return null;
		switch (value.type) {
		case TERM:
			return terms(field, value);
		case PHRASE:
			return phrase(field, value);
		case WILDCARD:
			return wildcard(field, value);
		case RANGE:
			return range(field, value);
		default:
			return null;
		}
	}

	private static QueryBuilder terms(String field, SearchFilterValue value) {
		List<Object> terms = toCollection(value.value);
		if (terms.size() == 1)
			return QueryBuilders.termQuery(field, terms.get(0));
		return QueryBuilders.termsQuery(field, terms);
	}

	private static QueryBuilder phrase(String field, SearchFilterValue value) {
		List<Object> phrases = toCollection(value.value);
		if (phrases.size() == 1)
			return QueryBuilders.matchPhraseQuery(field, phrases.get(0));
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		phrases.forEach(phrase -> {
			query.should(QueryBuilders.matchPhraseQuery(field, phrase));
		});
		return query;
	}

	private static QueryBuilder wildcard(String field, SearchFilterValue value) {
		return QueryBuilders.wildcardQuery(field, value.value.toString());
	}

	private static QueryBuilder range(String field, SearchFilterValue value) {
		Object[] v = (Object[]) value.value;
		return QueryBuilders.rangeQuery(field).from(v[0]).to(v[1]);
	}

	private static List<Object> toCollection(Object value) {
		if (value == null)
			return null;
		if (value instanceof String && value.toString().isEmpty())
			return null;
		if (value instanceof Collection)
			return filterEmpty((Collection<?>) value);
		return Collections.singletonList(value);
	}

	private static List<Object> filterEmpty(Collection<?> values) {
		return values.stream()
				.filter(value -> value != null && !value.toString().isEmpty())
				.collect(Collectors.toList());
	}

	private static QueryBuilder decorate(QueryBuilder query, String field, SearchFilterValue value) {
		if (query == null)
			return null;
		if (value.boost != null) {
			query = query.boost(value.boost);
		}
		if (field.contains(".")) {
			query = nest(query, field);
		}
		return query;
	}

	private static QueryBuilder nest(QueryBuilder query, String field) {
		String path = field;
		while (path.contains(".")) {
			path = path.substring(0, path.lastIndexOf("."));
			query = QueryBuilders.nestedQuery(path, query, ScoreMode.Total);
		}
		return query;
	}

	private static QueryBuilder score(QueryBuilder query, SearchQuery searchQuery) {
		if (searchQuery.getScores().isEmpty())
			return query;
		List<FilterFunctionBuilder> functions = new ArrayList<>();
		searchQuery.getScores().forEach(score -> {
			ScriptScoreFunctionBuilder script = ScoreFunctionBuilders.scriptFunction(Script.from(score));
			functions.add(new FilterFunctionBuilder(script));
		});
		searchQuery.getFunctions().forEach(function -> {
			LinearDecayFunctionBuilder script = ScoreFunctionBuilders.linearDecayFunction(function.fieldName,
					function.origin, function.scale, function.offset, function.decay);
			functions.add(new FilterFunctionBuilder(script));
		});
		return QueryBuilders.functionScoreQuery(query, functions.toArray(new FilterFunctionBuilder[functions.size()]));
	}

}
