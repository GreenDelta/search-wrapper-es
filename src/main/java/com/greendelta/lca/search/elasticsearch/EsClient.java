package com.greendelta.lca.search.elasticsearch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import com.greendelta.lca.search.SearchClient;
import com.greendelta.lca.search.SearchQuery;
import com.greendelta.lca.search.SearchResult;

public class EsClient implements SearchClient {

	private static final String INDEX_NAME = "datasets";

	private final Client client;

	public EsClient(Client client) {
		this.client = client;
	}

	@Override
	public SearchResult<Map<String, Object>> search(SearchQuery searchQuery) {
		return EsSearch.search(searchQuery, client, INDEX_NAME);
	}

	@Override
	public void create(Map<String, Object> settings) {
		boolean exists = client.admin().indices().prepareExists(INDEX_NAME).execute().actionGet().isExists();
		if (exists)
			return;
		String indexSettings = EsSettings.getConfig(settings);
		CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
		request.settings(Settings.builder().loadFromSource(indexSettings, XContentType.JSON).put("number_of_shards", 1));
		client.admin().indices().create(request).actionGet();
		Map<String, String> mappings = EsSettings.getMappings(settings);
		for (String indexType : mappings.keySet()) {
			PutMappingRequest mappingRequest = Requests.putMappingRequest(INDEX_NAME);
			mappingRequest.type(indexType).source(mappings.get(indexType), XContentType.JSON);
			client.admin().indices().putMapping(mappingRequest).actionGet();
		}
	}

	@Override
	public void index(String indexType, String id, Map<String, Object> content) {
		client.index(indexRequest(indexType, id, content)).actionGet();
	}

	@Override
	public void index(String indexType, Map<String, Map<String, Object>> contentsById) {
		index(Collections.singletonMap(indexType, contentsById));
	}

	@Override
	public void index(Map<String, Map<String, Map<String, Object>>> contentsByIdByType) {
		BulkRequestBuilder builder = client.prepareBulk();
		for (String indexType : contentsByIdByType.keySet()) {
			Map<String, Map<String, Object>> contentsById = contentsByIdByType.get(indexType);
			for (String id : contentsById.keySet()) {
				Map<String, Object> content = contentsById.get(id);
				builder.add(indexRequest(indexType, id, content));
			}
		}
		client.bulk(builder.request()).actionGet();
	}

	private IndexRequest indexRequest(String indexType, String id, Map<String, Object> content) {
		IndexRequestBuilder builder = client.prepareIndex(INDEX_NAME, indexType, id);
		builder.setOpType(OpType.INDEX).setSource(content);
		return builder.request();
	}

	@Override
	public void remove(String indexType, String id) {
		client.delete(deleteRequest(indexType, id)).actionGet();
	}

	@Override
	public void remove(String indexType, Set<String> ids) {
		remove(Collections.singletonMap(indexType, ids));
	}

	@Override
	public void remove(Map<String, Set<String>> idsByType) {
		BulkRequestBuilder bulk = client.prepareBulk();
		for (String indexType : idsByType.keySet()) {
			Set<String> ids = idsByType.get(indexType);
			for (String id : ids) {
				bulk.add(deleteRequest(indexType, id));
			}
		}
		client.bulk(bulk.request()).actionGet();

	}

	private DeleteRequest deleteRequest(String indexType, String id) {
		return client.prepareDelete(INDEX_NAME, indexType, id).request();
	}

	@Override
	public Map<String, Object> get(String indexType, String id) {
		GetRequestBuilder builder = client.prepareGet(INDEX_NAME, indexType, id);
		GetResponse response = client.get(builder.request()).actionGet();
		if (response == null)
			return null;
		Map<String, Object> source = response.getSource();
		if (source == null || source.isEmpty())
			return null;
		return source;
	}

	@Override
	public List<Map<String, Object>> get(String indexType, Set<String> ids) {
		return get(Collections.singletonMap(indexType, ids));
	}

	@Override
	public List<Map<String, Object>> get(Map<String, Set<String>> idsByType) {
		MultiGetRequestBuilder builder = client.prepareMultiGet();
		for (String indexType : idsByType.keySet()) {
			Set<String> ids = idsByType.get(indexType);
			builder.add(INDEX_NAME, indexType, ids);
		}
		MultiGetResponse response = client.multiGet(builder.request()).actionGet();
		if (response == null)
			return null;
		List<Map<String, Object>> results = new ArrayList<>();
		Iterator<MultiGetItemResponse> it = response.iterator();
		while (it.hasNext()) {
			GetResponse resp = it.next().getResponse();
			if (resp == null)
				continue;
			Map<String, Object> source = resp.getSource();
			if (source == null || source.isEmpty())
				continue;
			results.add(source);
		}
		return results;
	}

	@Override
	public void delete() {
		client.admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
	}

}
