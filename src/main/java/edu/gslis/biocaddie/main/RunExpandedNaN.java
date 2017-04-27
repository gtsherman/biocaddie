package edu.gslis.biocaddie.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import edu.gslis.docscoring.support.IndexBackedCollectionStats;
import edu.gslis.indexes.IndexWrapperIndriImpl;
import edu.gslis.output.FormattedOutputTrecEval;
import edu.gslis.queries.GQueries;
import edu.gslis.queries.GQueriesIndriImpl;
import edu.gslis.queries.GQuery;
import edu.gslis.scoring.DirichletDocScorer;
import edu.gslis.scoring.DocScorer;
import edu.gslis.scoring.expansion.RelevanceModelScorer;
import edu.gslis.scoring.queryscoring.QueryLikelihoodQueryScorer;
import edu.gslis.scoring.queryscoring.QueryScorer;
import edu.gslis.searchhits.SearchHit;
import edu.gslis.searchhits.SearchHits;
import edu.gslis.textrepresentation.FeatureVector;
import edu.gslis.utils.Stopper;
import edu.gslis.utils.config.Configuration;
import edu.gslis.utils.config.SimpleConfiguration;

public class RunExpandedNaN {
	
	public static DocScorer origDocScorer;
	public static DocScorer origDocZeroMuScorer;

	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration config = new SimpleConfiguration();
		config.read(args[0]);
		
		IndexWrapperIndriImpl index = new IndexWrapperIndriImpl(config.get("index"));
		String pubmedIndex = config.get("pubmed-index");
		String wikiIndex = config.get("wiki-index");

		Stopper stopper = new Stopper(config.get("stoplist"));

		GQueries queries = new GQueriesIndriImpl();
		queries.read(config.get("queries"));
		
		IndexBackedCollectionStats origCS = new IndexBackedCollectionStats();
		origCS.setStatSource(index);
		IndexBackedCollectionStats pubmedCS = new IndexBackedCollectionStats();
		pubmedCS.setStatSource(pubmedIndex);
		IndexBackedCollectionStats wikiCS = new IndexBackedCollectionStats();
		wikiCS.setStatSource(wikiIndex);

		origDocScorer = new DirichletDocScorer(origCS);
		origDocZeroMuScorer = new DirichletDocScorer(0, origCS);
		
		String expModelLoc = config.get("expansion-rm-models");
		String expModelLoc2 = config.get("expansion-rm-models-2");
		
		Writer outputWriter = new BufferedWriter(new OutputStreamWriter(System.out));
		FormattedOutputTrecEval output = FormattedOutputTrecEval.getInstance("expansionRM3", outputWriter);
		
		GQuery tempquery = null;
		for (GQuery q : queries) {
			if (q.getTitle().equals("EA4")) tempquery = q;
		}
		GQuery query = tempquery;
		query.applyStopper(stopper);

		SearchHits initialResults;
		//synchronized(this) {
			initialResults = index.runQuery(query, 5);
		//}
		
		// Collect vocab
		Map<String, FeatureVector> origTerms = new HashMap<String, FeatureVector>();
		Map<String, FeatureVector> pubmedTerms = new HashMap<String, FeatureVector>();
		Map<String, FeatureVector> wikiTerms = new HashMap<String, FeatureVector>();
		initialResults.hits().stream().parallel().forEach(doc -> origTerms.put(doc.getDocno(),
				getTermWeights(doc, query, stopper)));
		initialResults.hits().stream().parallel().forEach(doc -> pubmedTerms.put(doc.getDocno(),
				readModel(doc, expModelLoc)));
		initialResults.hits().stream().parallel().forEach(doc -> wikiTerms.put(doc.getDocno(),
				readModel(doc, expModelLoc2)));
		
		query.getFeatureVector().normalize();
		
		Set<String> vocab = new HashSet<String>();
		for (SearchHit doc : initialResults) {
			FeatureVector origTermWeights = origTerms.get(doc.getDocno());
			FeatureVector pubmedTermWeights = pubmedTerms.get(doc.getDocno());
			FeatureVector wikiTermWeights = wikiTerms.get(doc.getDocno());

			vocab.addAll(origTermWeights.getFeatures());
			vocab.addAll(pubmedTermWeights.getFeatures());
			vocab.addAll(wikiTermWeights.getFeatures());
		}
		
		double origWeight = 0.0;
		double pubmedWeight = 0.0;
		double wikiWeight = 1.0;
					
				FeatureVector rmVec = new FeatureVector(null);
				for (SearchHit doc : initialResults) {
					System.err.println(doc);
					FeatureVector origTermWeights = origTerms.get(doc.getDocno());
					FeatureVector pubmedTermWeights = pubmedTerms.get(doc.getDocno());
					FeatureVector wikiTermWeights = wikiTerms.get(doc.getDocno());

					double queryPMScore = 1.0;
					Iterator<String> queryIt = query.getFeatureVector().iterator();
					while (queryIt.hasNext()) {
						String term = queryIt.next();
						double termScore = pubmedTermWeights.getFeatureWeight(term);
						if (termScore == 0.0) {
							//synchronized(this) {
								termScore = (pubmedCS.termCount(term) + 1) / pubmedCS.getTokCount();
							System.err.println("(pubmed) " + term + " not in " + doc.getDocno() + ", score: " + termScore);
							//}
						}
						queryPMScore *= termScore;
					}
					double queryPubmedScore = queryPMScore;
					System.err.println("query pm score: " + queryPubmedScore);

					double queryWScore = 1.0;
					queryIt = query.getFeatureVector().iterator();
					while (queryIt.hasNext()) {
						String term = queryIt.next();
						double termScore = wikiTermWeights.getFeatureWeight(term);
						if (termScore == 0.0) {
							//synchronized(this) {
								termScore = (wikiCS.termCount(term) + 1) / wikiCS.getTokCount();
							System.err.println("(wiki) " + term + " not in " + doc.getDocno() + ", score: " + termScore);
							//}
						}
						queryWScore *= termScore;
					}
					double queryWikiScore = queryWScore;
					System.err.println("query wiki score: " + queryPubmedScore);

					vocab
						.stream()
						//.parallel()
						.forEach(term -> rmVec.addTerm(term, 
								origWeight * origTermWeights.getFeatureWeight(term) + 
								pubmedWeight * pubmedTermWeights.getFeatureWeight(term) * queryPubmedScore +
								wikiWeight * wikiTermWeights.getFeatureWeight(term) * queryWikiScore));
				}
				
				int fbTerms = 10;
					FeatureVector clippedRMVec = new FeatureVector(null);
					Iterator<String> termIt = rmVec.iterator();
					int i = 0;
					while (i < fbTerms && termIt.hasNext()) {
						String term = termIt.next();
						clippedRMVec.addTerm(term, rmVec.getFeatureWeight(term));
						i++;
					}

					double lam = 0.0;
			
						FeatureVector rm3Vec = FeatureVector.interpolate(query.getFeatureVector(), clippedRMVec, lam);
						System.err.println(rm3Vec.toString(5));

						GQuery newQuery = new GQuery();
						newQuery.setTitle(query.getTitle());
						newQuery.setFeatureVector(rm3Vec);
						
						int mu = 2500;
						//synchronized(this) {
							index.setMu(mu);
						//}
						
						System.err.println("Query: " + newQuery);
						
						SearchHits results = index.runQuery(newQuery, 1000);
						output.write(results, newQuery.getTitle());

	}
	
	public static FeatureVector getTermWeights(SearchHit doc, GQuery query, Stopper stopper) {
		FeatureVector origTerms = new FeatureVector(null);

		QueryScorer queryScorer = new QueryLikelihoodQueryScorer(origDocScorer);
		DocScorer rmScorer; 
		//synchronized(this) {
			rmScorer = new RelevanceModelScorer(origDocZeroMuScorer,
				Math.exp(queryScorer.scoreQuery(query, doc)));

			// Score each term
			for (String term : doc.getFeatureVector().getFeatures()) {
				if (stopper != null && stopper.isStopWord(term)) {
					continue;
				}
				origTerms.addTerm(term, rmScorer.scoreTerm(term, doc));
			}
		//}
		
		return origTerms;
	}

	public static FeatureVector readModel(SearchHit doc, String location) {
		FeatureVector model = new FeatureVector(null);
		try {
			Scanner scanner = new Scanner(new File(location + File.separator + doc.getDocno() + ".stopped"));
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] parts = line.split(" ");
				model.addTerm(parts[2].trim(), Double.parseDouble(parts[0]));
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			System.err.print("Can't find " + location + File.separator + doc.getDocno());
			//System.exit(-1);
		}
		return model;
	}

}
