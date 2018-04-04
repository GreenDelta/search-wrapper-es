package com.greendelta.search.wrapper.es;

import com.greendelta.search.wrapper.score.DateRangeScore;

class DateRangeScript {

	static String from(DateRangeScore score) {
		if (score.getCases().length == 0)
			return "return 1;";
		String script = "def format = new SimpleDateFormat('yyyy-MM-dd');";
		script += "format.setTimeZone(TimeZone.getTimeZone('UTC'));";
		script += "double " + score.lowerField + " = doc['" + score.lowerField + "'].getValue().getMillis() / 31536000000d;";
		script += "double " + score.upperField + " = doc['" + score.upperField + "'].getValue().getMillis() / 31536000000d;";
		script += "double _value_ = format.parse('" + score.value + "').getTime() / 31536000000d;";
		script += "double compareTo;";
		script += "if (Math.abs(_value_ - " + score.lowerField + ") < Math.abs(_value_ - " + score.upperField + " )) {";
		script += "compareTo = " + score.lowerField + ";";
		script += "} else {";
		script += "compareTo = " + score.upperField + ";";
		script += "}";
		script += "double distance = Math.abs(_value_ - compareTo);";
		script += EsScript.from(score.getCases());
		return script;
	}

}
