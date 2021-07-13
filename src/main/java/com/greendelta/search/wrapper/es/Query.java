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
import com.greendelta.search.wrapper.score.Score;

class Query {

	static QueryBuilder builder(SearchQuery searchQuery) {
		BoolQueryBuilder bool = QueryBuilders.boolQuery();
		for (SearchFilter filter : searchQuery.getFilters()) {
			QueryBuilder query = toQuery(filter);
			if (query == null)
				continue;
			bool.must(query);
		}
		for (MultiSearchFilter filter : searchQuery.getMultiFilters()) {
			if (filter.values.isEmpty())
				continue;
			QueryBuilder query = toQuery(filter);
			if (query == null)
				continue;
			bool.must(query);
		}
		if (!bool.must().isEmpty())
			return addScores(simplify(bool), searchQuery);
		return addScores(QueryBuilders.matchAllQuery(), searchQuery);
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

	private static QueryBuilder toQuery(SearchFilter filter) {
		if (filter.values.isEmpty())
			return null;
		BoolQueryBuilder bool = QueryBuilders.boolQuery();
		for (SearchFilterValue value : filter.values) {
			if (value.value instanceof String && value.value.toString().isEmpty())
				continue;
			QueryBuilder query = builder(filter.field, value);
			append(bool, query, filter.conjunction);
		}
		return simplify(bool);
	}

	private static QueryBuilder toQuery(MultiSearchFilter filter) {
		if (filter.values.isEmpty())
			return null;
		BoolQueryBuilder outerBool = QueryBuilders.boolQuery();
		for (String field : filter.fields) {
			BoolQueryBuilder innerBool = QueryBuilders.boolQuery();
			for (SearchFilterValue value : filter.values) {
				if (value.value instanceof String && value.value.toString().isEmpty())
					continue;
				QueryBuilder query = builder(field, value);
				append(innerBool, query, filter.conjunction);
			}
			outerBool.should(innerBool);
		}
		return simplify(outerBool);
	}

	private static void append(BoolQueryBuilder boolQuery, QueryBuilder query, Conjunction conjunction) {
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
		if (phrases.size() == 1)
			return matchPhraseQuery(field, phrases.iterator().next());
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		phrases.forEach(phrase -> query.should(matchPhraseQuery(field, phrase)));
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
