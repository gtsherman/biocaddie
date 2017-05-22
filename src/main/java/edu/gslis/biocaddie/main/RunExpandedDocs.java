package edu.gslis.biocaddie.main;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.gslis.biocaddie.runners.ExpandedDocRunner;
import edu.gslis.queries.GQueries;
import edu.gslis.queries.GQueriesIndriImpl;
import edu.gslis.queries.GQuery;
import edu.gslis.scoring.DocScorer;
import edu.gslis.utils.Stopper;
import edu.gslis.utils.config.Configuration;
import edu.gslis.utils.config.SimpleConfiguration;

public class RunExpandedDocs {
	
	public static DocScorer origDocScorer;
	public static DocScorer origDocZeroMuScorer;

	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration config = new SimpleConfiguration();
		config.read(args[0]);
		
		String index = config.get("index");
		String pubmedIndex = config.get("pubmed-index");
		String wikiIndex = config.get("wiki-index");

		Stopper stopper = new Stopper(config.get("stoplist"));

		GQueries queries = new GQueriesIndriImpl();
		queries.read(config.get("queries"));

		
		String expModelLoc = config.get("expansion-rm-models");
		String expModelLoc2 = config.get("expansion-rm-models-2");
		
		String outDir = config.get("expansion-sweep-dir"); 
		
		int threads = 15;
		if (config.get("threads") != null) {
			threads = Integer.parseInt(config.get("threads"));
		}

		ExecutorService executor = Executors.newFixedThreadPool(threads);
		for (GQuery query : queries) {
			ExpandedDocRunner runner = new ExpandedDocRunner();
			runner.setExpModelLocations(expModelLoc, expModelLoc2);
			runner.setIndex(index, pubmedIndex, wikiIndex);
			runner.setOutDir(outDir);
			runner.setQuery(query);
			runner.setStopper(stopper);
			executor.execute(runner);
		}	
		
		executor.shutdown();
		executor.awaitTermination(10, TimeUnit.DAYS);
		System.err.println("Completed");

	}

}
