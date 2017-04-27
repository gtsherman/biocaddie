package edu.gslis.biocaddie.main;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;

import edu.gslis.indexes.IndexWrapperIndriImpl;
import edu.gslis.output.FormattedOutputTrecEval;
import edu.gslis.queries.GQueries;
import edu.gslis.queries.GQueriesIndriImpl;
import edu.gslis.queries.GQuery;
import edu.gslis.searchhits.SearchHits;
import edu.gslis.utils.Stopper;
import edu.gslis.utils.config.Configuration;
import edu.gslis.utils.config.SimpleConfiguration;

public class RunDefault {

	public static void main(String[] args) {
		Configuration config = new SimpleConfiguration();
		config.read(args[0]);
		
		IndexWrapperIndriImpl index = new IndexWrapperIndriImpl(config.get("index"));

		Stopper stopper = new Stopper(config.get("stoplist"));

		GQueries queries = new GQueriesIndriImpl();
		queries.read(config.get("queries"));
		
		Writer outputWriter = new BufferedWriter(new OutputStreamWriter(System.out));
		FormattedOutputTrecEval output = FormattedOutputTrecEval.getInstance("expansionRM3", outputWriter);

		for (GQuery query : queries) {
			query.applyStopper(stopper);
			SearchHits results = index.runQuery(query, 50);
			output.write(results, query.getTitle());
		}
	}

}
