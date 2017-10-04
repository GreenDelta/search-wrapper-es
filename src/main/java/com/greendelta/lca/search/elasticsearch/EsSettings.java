package com.greendelta.lca.search.elasticsearch;

import java.util.HashMap;
import java.util.Map;

public class EsSettings {

	public static final String CONFIG = "config";
	public static final String MAPPINGS = "mappings";

	static String getConfig(Map<String, Object> settings) {
		if (!settings.containsKey(CONFIG))
			return "{}";
		Object config = settings.get(CONFIG);
		if (config == null)
			return "{}";
		if (!(config instanceof String))
			throw new IllegalArgumentException("If config is set it must be of type String, but was of type "
					+ config.getClass().getCanonicalName());
		return config.toString();
	}

	@SuppressWarnings("unchecked")
	static Map<String, String> getMappings(Map<String, Object> settings) {
		if (!settings.containsKey(MAPPINGS))
			return new HashMap<>();
		Object mappings = settings.get(MAPPINGS);
		if (mappings == null)
			return new HashMap<>();
		if (!(mappings instanceof Map))
			throw new IllegalArgumentException(
					"If mappings are set it must be of type Map<String, String>, but was of type "
							+ mappings.getClass().getCanonicalName());
		Map<?, ?> map = (Map<?, ?>) mappings;
		for (Object key : map.keySet())
			if (key == null)
				throw new IllegalArgumentException("If mappings are set keys can't be null or empty");
			else if (!(key instanceof String))
				throw new IllegalArgumentException(
						"If mappings are set it must be of type Map<String, String>, but at least one key was of type "
								+ key.getClass().getCanonicalName());
		for (Object value : map.values())
			if (value == null)
				throw new IllegalArgumentException("If mappings are set values can't be null or empty");
			else if (!(value instanceof String))
				throw new IllegalArgumentException(
						"If mappings are set it must be of type Map<String, String>, but at least one value was of type "
								+ value.getClass().getCanonicalName());
		try {
			return (Map<String, String>) map;
		} catch (ClassCastException e) {
			Map<String, String> converted = new HashMap<>();
			for (Object key : map.keySet()) {
				converted.put(key.toString(), map.get(key).toString());
			}
			return converted;
		}
	}

}
