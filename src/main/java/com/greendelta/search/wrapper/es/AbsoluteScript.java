package com.greendelta.search.wrapper.es;

import com.greendelta.search.wrapper.score.AbsoluteScore;

class AbsoluteScript {

	static String from(AbsoluteScore score) {
		if (score.getCases().length == 0)
			return "return 1;";
		String script = "double " + score.field + " = doc['" + score.field + "'].getValue();";
		script += EsScript.from(score.getCases());
		return script;
	}

}
