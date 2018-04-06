package com.greendelta.search.wrapper.es;

import java.util.List;

import com.greendelta.search.wrapper.score.Case;
import com.greendelta.search.wrapper.score.Comparator;
import com.greendelta.search.wrapper.score.Condition;
import com.greendelta.search.wrapper.score.Field;
import com.greendelta.search.wrapper.score.Score;

class EsScript {

	static String from(Score score) {
		if (score.getCases().length == 0)
			return "return 1;";
		String script = getMethods(score);
		script += "def[] fieldValues = new def[" + score.fields.size() + "];";
		script += "def[] values = new def[" + score.fields.size() + "];";
		for (int i = 0; i < score.fields.size(); i++) {
			Field field = score.fields.get(i);
			String escaper = field.value instanceof String ? "\"" : "";
			String numDef = field.value instanceof Long ? "L" : "";
			script += "fieldValues[" + i + "] = doc['" + field.name + "'].getValue();";
			script += "values[" + i + "] = " + escaper + "" + field.value + "" + escaper + numDef + ";";
		}
		script += "int fieldClass = toClass(fieldValues, values);";
		script += "int valueClass = toClass(values, values);";
		script += "int classDifference = (int) Math.abs(fieldClass - valueClass);";
		script += cases(score.getCases());
		return script;
	}

	private static String getClassificationMethod(Score score) {
		String script = "int toClass(def[] fieldValues, def[] values) {";
		if (score.classes.isEmpty()) {
			script += "return 0;";
		} else {
			int classes = 0;
			for (int i = 0; i < score.classes.size(); i++) {
				List<Condition> conditions = score.classes.get(i);
				if (!conditions.isEmpty()) {
					classes++;
					script += conditions(conditions, i);
				}
			}
			if (classes == 0) {
				script += "return 0;";
			} else {
				script += "return " + classes + ";";
			}
		}
		script += "}";
		return script;
	}

	static String cases(Case[] cases) {
		String script = "";
		for (Case c : cases) {
			if (!c.conditions.isEmpty()) {
				script += conditions(c.conditions, c.weight);
			} else {
				script += "return " + c.weight + ";";
				break;
			}
		}
		return script;
	}

	static String conditions(List<Condition> conditions, Object value) {
		String script = "if (";
		boolean firstCondition = true;
		for (Condition con : conditions) {
			if (!firstCondition) {
				script += " && ";
			}
			if (con.comparator == Comparator.EQUALS) {
				script += con.value1 + ".equals(" + con.value2 + ")";
			} else {
				String numDef1 = con.value1 instanceof Long ? "L" : "";
				String numDef2 = con.value2 instanceof Long ? "L" : "";
				script += con.value1 + numDef1 + " " + toString(con.comparator) + " " + con.value2 + numDef2;
			}
			firstCondition = false;
		}
		script += ") { return " + value + "; } ";
		return script;
	}

	private static String getDistanceMethod() {
		String script = "double toRad(double degree) { return degree * Math.PI / 180; }";
		script += "double getDistance(double lat1, double lon1, double lat2, double lon2) { ";
		script += "double earthRadius = 6371;";
		script += "double rdLat = toRad(lat2-lat1);";
		script += "double rdLon = toRad(lon2-lon1);";
		script += "double rLat1 = toRad(lat1);";
		script += "double rLat2 = toRad(lat2);";
		script += "double a = Math.sin(rdLat/2) * Math.sin(rdLat/2) ";
		script += "+ Math.sin(rdLon/2) * Math.sin(rdLon/2) * Math.cos(rLat1) * Math.cos(rLat2);";
		script += "double b = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));";
		script += "return earthRadius * b;";
		script += "}";
		return script;
	}

	private static String getMethods(Score score) {
		String script = getClassificationMethod(score);
		script += getDistanceMethod();
		script += "String substring(String value, int from, int to) { return value.substring(from, to); }";
		script += "int indexOf(String value, String phrase) { return value.indexOf(phrase); }";
		script += "int lastIndexOf(String value, String phrase) { return value.lastIndexOf(phrase); }";
		script += "double abs(double value) { return Math.abs(value); }";
		script += "double min(double v1, double v2) { return Math.min(v1, v2); }";
		return script;
	}

	private static String toString(Comparator comparator) {
		switch (comparator) {
		case IS:
			return "==";
		case IS_LESS_THAN:
			return "<";
		case IS_LESS_OR_EQUAL_THAN:
			return "<=";
		case IS_GREATER_THAN:
			return ">";
		case IS_GREATER_OR_EQUAL_THAN:
			return ">=";
		default:
			return "==";
		}
	}

}
