package edu.gslis.biocaddie.runners;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
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
import edu.gslis.queries.GQuery;
import edu.gslis.scoring.DirichletDocScorer;
import edu.gslis.scoring.DocScorer;
import edu.gslis.scoring.queryscoring.QueryLikelihoodQueryScorer;
import edu.gslis.searchhits.SearchHit;
import edu.gslis.searchhits.SearchHits;
import edu.gslis.textrepresentation.FeatureVector;
import edu.gslis.utils.Stopper;

public class RMRunnerInterp implements Runnable {
	
	private GQuery query;
	private IndexWrapperIndriImpl index;
	private Stopper stopper;
	private int fbDocs;
	private String expModelLoc;
	private String expModelLoc2;
	private IndexBackedCollectionStats origCS;
	private IndexBackedCollectionStats pubmedCS;
	private IndexBackedCollectionStats wikiCS;
	private String outdir;
	
	public static DocScorer origDocScorer;
	public static DocScorer origDocZeroMuScorer;

	Writer outputWriter = new BufferedWriter(new OutputStreamWriter(System.out));
	FormattedOutputTrecEval output = FormattedOutputTrecEval.getInstance("expansionRM3", outputWriter);
	
	public void setQuery(GQuery query) {
		this.query = query;
	}
	
	public void setIndex(String index, String pubmedIndex, String wikiIndex) {
		this.index = new IndexWrapperIndriImpl(index);
		origCS = new IndexBackedCollectionStats();
		origCS.setStatSource(index);
		pubmedCS = new IndexBackedCollectionStats();
		pubmedCS.setStatSource(pubmedIndex);
		wikiCS = new IndexBackedCollectionStats();
		wikiCS.setStatSource(wikiIndex);

		origDocScorer = new DirichletDocScorer(origCS);
		origDocZeroMuScorer = new DirichletDocScorer(0, origCS);
	}
	
	public void setStopper(Stopper stopper) {
		this.stopper = stopper;
	}
	
	public void setFeedbackDocs(int fbDocs) {
		this.fbDocs = fbDocs;
	}
	
	public void setExpModelLocations(String expModelLoc, String expModelLoc2) {
		this.expModelLoc = expModelLoc;
		this.expModelLoc2 = expModelLoc2;
	}
	
	public void setOutDir(String outdir) {
		this.outdir = outdir;
	}

	public void run() {
		query.applyStopper(stopper);

		SearchHits initialResults = index.runQuery(query, fbDocs);
		
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
		
		for (int origW = 0; origW <= 10; origW++) {
			double origWeight = origW / 10.0;
			
			for (int pubmedW = origW; pubmedW <= 10; pubmedW++) {

				double pubmedWeight = pubmedW / 10.0;
				double wikiWeight = (10 - pubmedW) / 10.0;
					
				FeatureVector rmVec = new FeatureVector(null);
				for (SearchHit doc : initialResults) {
					FeatureVector origTermWeights = origTerms.get(doc.getDocno());
					FeatureVector pubmedTermWeights = pubmedTerms.get(doc.getDocno());
					FeatureVector wikiTermWeights = wikiTerms.get(doc.getDocno());
					
					QueryLikelihoodQueryScorer queryScorer = new QueryLikelihoodQueryScorer(origDocScorer);
					double queryOrigScore = queryScorer.scoreQuery(query, doc);

					double queryPMScore = 1.0;
					Iterator<String> queryIt = query.getFeatureVector().iterator();
					while (queryIt.hasNext()) {
						String term = queryIt.next();
						double termScore = pubmedTermWeights.getFeatureWeight(term);
						/*if (termScore == 0.0) {
							termScore = (pubmedCS.termCount(term) + 1) / pubmedCS.getTokCount();
						}*/
						queryPMScore *= termScore;
					}
					double queryPubmedScore = queryPMScore;

					double queryWScore = 1.0;
					queryIt = query.getFeatureVector().iterator();
					while (queryIt.hasNext()) {
						String term = queryIt.next();
						double termScore = wikiTermWeights.getFeatureWeight(term);
						/*if (termScore == 0.0) {
							termScore = (wikiCS.termCount(term) + 1) / wikiCS.getTokCount();
						}*/
						queryWScore *= termScore;
					}
					double queryWikiScore = queryWScore;

					double queryScore = origWeight * queryOrigScore + 
							pubmedWeight * queryPubmedScore + 
							wikiWeight * queryWikiScore;
					
					vocab
						.stream()
						//.parallel()
						.forEach(term -> rmVec.addTerm(term, 
								origWeight * origTermWeights.getFeatureWeight(term) + 
										pubmedWeight * pubmedTermWeights.getFeatureWeight(term) +
										wikiWeight * wikiTermWeights.getFeatureWeight(term) + 1 *
								queryScore));
				}
				rmVec.normalize();
				
				for (int fbTerms : new int[] {5, 10, 20, 50}) {
					FeatureVector clippedRMVec = new FeatureVector(null);
					for (String term : rmVec) {
						clippedRMVec.addTerm(term, rmVec.getFeatureWeight(term));
					}
					clippedRMVec.clip(fbTerms);

					for (int lambda = 0; lambda <= 10; lambda += 1) {
						double lam = lambda / 10.0;
			
						FeatureVector rm3Vec = FeatureVector.interpolate(query.getFeatureVector(), clippedRMVec, lam);

						GQuery newQuery = new GQuery();
						newQuery.setTitle(query.getTitle());
						newQuery.setFeatureVector(rm3Vec);
						
						for (int mu : new int[] {50, 250, 500, 1000, 5000, 10000, 2500}) {
							index.setMu(mu);
							
							String run = "d" + fbDocs + "_t" + fbTerms + "_o" + origWeight + "_p" + pubmedWeight + "_w" + wikiWeight + "_l" + lam + "_m" + mu;
							output.setRunId(run);
							try {
								output.setWriter(new FileWriter(outdir + File.separator + run, true));
							} catch (IOException e) {
								System.err.println("Error setting file writer");
								e.printStackTrace(System.err);
								System.exit(-1);
							}
							
							SearchHits results = index.runQuery(newQuery, 1000);
							synchronized(this) {
								output.write(results, newQuery.getTitle());
								if (Double.isNaN(results.getHit(0).getScore())) {
									System.err.println("Got NaN:");
									System.err.println("Params: " + run);
									System.err.println("Query: " + newQuery);
								}
							}
						}
					}
				}
			}
		}
		
	}

	public FeatureVector getTermWeights(SearchHit doc, GQuery query, Stopper stopper) {
		FeatureVector origTerms = new FeatureVector(null);

		DocScorer rmScorer = origDocZeroMuScorer;

		// Score each term
		for (String term : doc.getFeatureVector().getFeatures()) {
			if (stopper != null && stopper.isStopWord(term)) {
				continue;
			}
			origTerms.addTerm(term, rmScorer.scoreTerm(term, doc));
		}
		
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
