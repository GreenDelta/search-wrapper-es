package com.greendelta.search.wrapper.es;

import com.greendelta.search.wrapper.score.Case;
import com.greendelta.search.wrapper.score.Comparator;
import com.greendelta.search.wrapper.score.Condition;
import com.greendelta.search.wrapper.score.Field;
import com.greendelta.search.wrapper.score.Score;

class EsScript {

	static String from(Score score) {
		double dWeight = score.getDefaultWeight();
		if (score.getCases().length == 0)
			return "return " + dWeight + ";";
		String s = getMethods(score);
		s += "def[] fieldValues = new def[" + score.fields.size() + "];";
		s += "def[] values = new def[" + score.fields.size() + "];";
		for (int i = 0; i < score.fields.size(); i++) {
			Field field = score.fields.get(i);
			String escaper = field.value instanceof String ? "\"" : "";
			String numDef = field.value instanceof Long ? "L" : "";
			s += "fieldValues[" + i + "] = doc['" + field.name + "'].getValue();";
			if (field.lowerLimit != null) {
				s += "if (fieldValues[" + i + "] < " + field.lowerLimit + ") { return " + dWeight + "; }";
			}
			if (field.upperLimit != null) {
				s += "if (fieldValues[" + i + "] >  " + field.upperLimit + ") { return " + dWeight + "; }";
			}
			s += "values[" + i + "] = " + escaper + "" + field.value + "" + escaper + numDef + ";";
		}
		s += cases(score);
		return s;
	}

	static String cases(Score score) {
		String s = "";
		boolean hadElse = false;
		for (Case c : score.getCases()) {
			if (!c.conditions.isEmpty()) {
				s += conditions(c);
			} else {
				s += "return " + c.weight + ";";
				hadElse = true;
				break;
			}
		}
		if (!hadElse) {
			s += "return " + score.getDefaultWeight() + ";"; // default case
		}
		return s;
	}

	static String conditions(Case scoreCase) {
		String s = "if (";
		boolean firstCondition = true;
		for (Condition con : scoreCase.conditions) {
			if (!firstCondition) {
				s += " && ";
			}
			if (con.comparator == Comparator.EQUALS) {
				s += "(" + con.value1 + ") != null && " + (con.value1) + ".equals(" + con.value2 + ")";
			} else {
				String numDef1 = con.value1 instanceof Long ? "L" : "";
				String numDef2 = con.value2 instanceof Long ? "L" : "";
				s += con.value1 + numDef1 + " " + toString(con.comparator) + " " + con.value2 + numDef2;
			}
			firstCondition = false;
		}
		s += ") { return " + scoreCase.weight + "; } ";
		return s;
	}

	private static String getDistanceMethod() {
		String s = "double toRad(double degree) { return degree * Math.PI / 180; }";
		s += "double getDistance(double lat1, double lon1, double lat2, double lon2) { ";
		s += "double earthRadius = 6371;";
		s += "double rdLat = toRad(lat2-lat1);";
		s += "double rdLon = toRad(lon2-lon1);";
		s += "double rLat1 = toRad(lat1);";
		s += "double rLat2 = toRad(lat2);";
		s += "double a = Math.sin(rdLat/2) * Math.sin(rdLat/2) + Math.sin(rdLon/2) * Math.sin(rdLon/2) * Math.cos(rLat1) * Math.cos(rLat2);";
		s += "double b = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));";
		s += "return earthRadius * b;";
		s += "}";
		return s;
	}

	private static String getMethods(Score score) {
		String s = getDistanceMethod();
		s += "String substring(String value, int from, int to) { if (value == null || from == -1 || to == -1) { return null; } return value.substring(from, to); }";
		s += "int indexOf(String value, String phrase) { if (value == null || phrase == null) { return -1; } return value.indexOf(phrase); }";
		s += "int lastIndexOf(String value, String phrase) { if (value == null || phrase == null) { return -1; } return value.lastIndexOf(phrase); }";
		s += "double abs(double value) { return Math.abs(value); }";
		s += "double min(double v1, double v2) { return Math.min(v1, v2); }";
		return s;
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
