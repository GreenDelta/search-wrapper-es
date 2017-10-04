package com.greendelta.lca.search.elasticsearch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
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
	public SearchResult search(SearchQuery searchQuery) {
		return EsSearch.search(searchQuery, client, INDEX_NAME);
	}

	@Override
	public void create(Map<String, Object> settings) {
		boolean exists = client.admin().indices().prepareExists(INDEX_NAME).execute().actionGet().isExists();
		if (exists)
			return;
		String indexSettings = EsSettings.getConfig(settings);
		CreateIndexRequest createRequest = new CreateIndexRequest(INDEX_NAME).settings(Settings.builder()
				.loadFromSource(indexSettings, XContentType.JSON).put("number_of_shards", 1));
		client.admin().indices().create(createRequest).actionGet();
		Map<String, String> mappings = EsSettings.getMappings(settings);
		for (String indexType : mappings.keySet()) {
			PutMappingRequest mappingRequest = Requests.putMappingRequest(INDEX_NAME).type(indexType)
					.source(mappings.get(indexType), XContentType.JSON);
			client.admin().indices().putMapping(mappingRequest).actionGet();
		}
	}

	@Override
	public void index(String indexType, String id, Map<String, Object> content) {
		IndexRequest request = client.prepareIndex(INDEX_NAME, indexType, id).setOpType(OpType.INDEX)
				.setSource(content).request();
		client.index(request).actionGet();
	}

	@Override
	public void remove(String indexType, String id) {
		client.prepareDelete(INDEX_NAME, indexType, id).execute().actionGet();
	}

	@Override
	public Map<String, Object> get(String indexType, String id) {
		GetResponse response = client.prepareGet(INDEX_NAME, indexType, id).execute().actionGet();
		if (response == null)
			return null;
		Map<String, Object> source = response.getSource();
		if (source == null || source.isEmpty())
			return null;
		return source;
	}

	@Override
	public List<Map<String, Object>> get(String indexType, Set<String> ids) {
		MultiGetResponse response = client.prepareMultiGet().add(INDEX_NAME, indexType, ids).execute().actionGet();
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
		DeleteIndexRequest request = new DeleteIndexRequest(INDEX_NAME);
		client.admin().indices().delete(request).actionGet();
	}

}
