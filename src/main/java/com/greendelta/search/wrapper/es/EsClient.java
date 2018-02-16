package com.greendelta.search.wrapper.es;

import java.util.ArrayList;
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

import com.greendelta.search.wrapper.SearchClient;
import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.SearchResult;

public class EsClient implements SearchClient {

	private final Client client;
	private final String indexName;
	private final String indexType;

	public EsClient(Client client, String indexName, String indexType) {
		this.client = client;
		this.indexName = indexName;
		this.indexType = indexType;
	}

	@Override
	public SearchResult<Map<String, Object>> search(SearchQuery searchQuery) {
		return EsSearch.search(searchQuery, client, indexName);
	}

	@Override
	public void create(Map<String, String> settings) {
		boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (exists)
			return;
		String indexSettings = settings.get("config");
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		request.settings(Settings.builder().loadFromSource(indexSettings, XContentType.JSON).put("number_of_shards", 1));
		client.admin().indices().create(request).actionGet();
		String mapping = settings.get("mapping");
		PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName);
		mappingRequest.type(indexType).source(mapping, XContentType.JSON);
		client.admin().indices().putMapping(mappingRequest).actionGet();
	}

	@Override
	public void index(String id, Map<String, Object> content) {
		client.index(indexRequest(id, content)).actionGet();
	}

	@Override
	public void index(Map<String, Map<String, Object>> contentsById) {
		BulkRequestBuilder builder = client.prepareBulk();
		for (String id : contentsById.keySet()) {
			Map<String, Object> content = contentsById.get(id);
			builder.add(indexRequest(id, content));
		}
		client.bulk(builder.request()).actionGet();
	}

	private IndexRequest indexRequest(String id, Map<String, Object> content) {
		IndexRequestBuilder builder = client.prepareIndex(indexName, indexType, id);
		builder.setOpType(OpType.INDEX).setSource(content);
		return builder.request();
	}

	@Override
	public void remove(String id) {
		client.delete(deleteRequest(id)).actionGet();
	}

	@Override
	public void remove(Set<String> ids) {
		BulkRequestBuilder bulk = client.prepareBulk();
		for (String id : ids) {
			bulk.add(deleteRequest(id));
		}
		client.bulk(bulk.request()).actionGet();
	}

	private DeleteRequest deleteRequest(String id) {
		return client.prepareDelete(indexName, indexType, id).request();
	}

	@Override
	public boolean has(String id) {
		GetRequestBuilder builder = client.prepareGet(indexName, indexType, id);
		GetResponse response = client.get(builder.request()).actionGet();
		if (response == null)
			return false;
		return response.isExists();
	}
	
	@Override
	public Map<String, Object> get(String id) {
		GetRequestBuilder builder = client.prepareGet(indexName, indexType, id);
		GetResponse response = client.get(builder.request()).actionGet();
		if (response == null)
			return null;
		Map<String, Object> source = response.getSource();
		if (source == null || source.isEmpty())
			return null;
		return source;
	}

	@Override
	public List<Map<String, Object>> get(Set<String> ids) {
		MultiGetRequestBuilder builder = client.prepareMultiGet();
		builder.add(indexName, indexType, ids);
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
		boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (!exists)
			return;
		client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
	}

}
