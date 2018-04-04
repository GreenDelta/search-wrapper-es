package com.greendelta.search.wrapper.es;

import com.greendelta.search.wrapper.score.Case;
import com.greendelta.search.wrapper.score.Condition;

class EsScript {

	static String from(Case[] cases) {
		String script = "";
		for (Case c : cases) {
			if (!c.conditions.isEmpty()) {
				script += "if (";
				boolean firstCondition = true;
				for (Condition con : c.conditions) {
					if (!firstCondition) {
						script += " && ";
					}
					script += con.field + " " + con.comparator + " ";
					if (con.limit != null) {
						script += con.limit.toString();
					} else {
						script += con.otherField;
					}
					firstCondition = false;
				}
				script += ") {";
			}
			script += "return " + c.weight + ";";
			if (!c.conditions.isEmpty()) {
				script += "}";
			} else {
				break;
			}
		}
		return script;
	}

}
