package com.greendelta.search.wrapper.es;

import com.greendelta.search.wrapper.score.GeographyScore;

class GeographyScript {

	static String from(GeographyScore score) {
		if (score.getCases().length == 0)
			return "return 1;";
		String script = getDistanceMethod();
		String latVar = score.field + "_lat";
		String lonVar = score.field + "_lon";
		script += "double " + latVar + " = doc['" + score.field + "'].getValue().getLat();";
		script += "double " + lonVar + " = doc['" + score.field + "'].getValue().getLon();";
		script += "double valueLat = " + score.value.latitude + ";";
		script += "double valueLon = " + score.value.longitude + ";";
		script += "double deltaLat = Math.abs(valueLat - " + latVar + ");";
		script += "double deltaLon = Math.abs(valueLon - " + lonVar + ");";
		script += "double distance = getDistance(" + latVar + ", " + lonVar + ", valueLat, valueLon);";
		script += EsScript.from(score.getCases());
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

}
