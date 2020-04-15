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
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

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
	public Set<String> searchIds(SearchQuery searchQuery) {
		return EsSearch.searchIds(searchQuery, client, indexName);
	}

	@Override
	public void create(Map<String, String> settings) {
		boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (exists)
			return;
		String indexSettings = settings.get("config");
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		request.settings(
				Settings.builder().loadFromSource(indexSettings, XContentType.JSON).put("number_of_shards", 1));
		client.admin().indices().create(request).actionGet();
		String mapping = settings.get("mapping");
		PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName);
		mappingRequest.type(indexType).source(mapping, XContentType.JSON);
		client.admin().indices().putMapping(mappingRequest).actionGet();
	}

	@Override
	public void index(String id, Map<String, Object> content) {
		client.index(indexRequest(id, content, true)).actionGet();
	}

	@Override
	public void index(Map<String, Map<String, Object>> contentsById) {
		BulkRequestBuilder builder = client.prepareBulk();
		for (String id : contentsById.keySet()) {
			Map<String, Object> content = contentsById.get(id);
			builder.add(indexRequest(id, content, false));
		}
		client.bulk(builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	private IndexRequest indexRequest(String id, Map<String, Object> content, boolean refresh) {
		IndexRequestBuilder builder = client.prepareIndex(indexName, indexType, id);
		builder.setOpType(OpType.INDEX).setSource(content);
		if (refresh) {
			builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return builder.request();
	}

	@Override
	public void update(String id, Map<String, Object> update) {
		client.update(updateRequest(id, update, true)).actionGet();
	}

	@Override
	public void update(String id, String script, Map<String, Object> parameters) {
		client.update(updateRequest(id, script, parameters, true)).actionGet();
	}

	@Override
	public void update(Set<String> ids, Map<String, Object> update) {
		BulkRequestBuilder builder = client.prepareBulk();
		for (String id : ids) {
			builder.add(updateRequest(id, update, false));
		}
		client.bulk(builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	@Override
	public void update(Set<String> ids, String script, Map<String, Object> parameters) {
		BulkRequestBuilder builder = client.prepareBulk();
		for (String id : ids) {
			builder.add(updateRequest(id, script, parameters, false));
		}
		client.bulk(builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	@Override
	public void update(Map<String, Map<String, Object>> updatesById) {
		BulkRequestBuilder builder = client.prepareBulk();
		for (String id : updatesById.keySet()) {
			Map<String, Object> update = updatesById.get(id);
			builder.add(updateRequest(id, update, false));
		}
		client.bulk(builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	private UpdateRequest updateRequest(String id, Map<String, Object> content, boolean refresh) {
		UpdateRequestBuilder builder = client.prepareUpdate(indexName, indexType, id);
		builder.setDoc(content);
		if (refresh) {
			builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return builder.request();
	}

	private UpdateRequest updateRequest(String id, String script, Map<String, Object> parameters, boolean refresh) {
		UpdateRequestBuilder builder = client.prepareUpdate(indexName, indexType, id);
		builder.setScript(new Script(ScriptType.INLINE, "painless", script, parameters));
		if (refresh) {
			builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return builder.request();
	}

	@Override
	public void remove(String id) {
		client.delete(deleteRequest(id, true)).actionGet();
	}

	@Override
	public void remove(Set<String> ids) {
		BulkRequestBuilder bulk = client.prepareBulk();
		for (String id : ids) {
			bulk.add(deleteRequest(id, false));
		}
		client.bulk(bulk.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	private DeleteRequest deleteRequest(String id, boolean refresh) {
		DeleteRequestBuilder builder = client.prepareDelete(indexName, indexType, id);
		if (refresh) {
			builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return builder.request();
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
	public void clear() {
		boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (!exists)
			return;
		Map<String, Object> mapping = client.admin().indices().prepareGetMappings(indexName).execute().actionGet()
				.getMappings().get(indexName).get(indexType).getSourceAsMap();
		client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		request.settings(Settings.builder().put("max_result_window", 2147483647).put("number_of_shards", 1));
		client.admin().indices().create(request).actionGet();
		PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName);
		mappingRequest.type(indexType).source(mapping);
		client.admin().indices().putMapping(mappingRequest).actionGet();
	}

	@Override
	public void delete() {
		boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (!exists)
			return;
		client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
	}

}
