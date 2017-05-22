package edu.gslis.biocaddie.runners;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
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

public class ExpandedDocRunner implements Runnable {
	
	private GQuery query;
	private IndexWrapperIndriImpl index;
	private Stopper stopper;
	private String expModelLoc;
	private String expModelLoc2;
	private IndexBackedCollectionStats origCS;
	private String outdir;
	
	public static DocScorer origDocScorer;

	public void setQuery(GQuery query) {
		this.query = query;
	}
	
	public void setIndex(String index, String pubmedIndex, String wikiIndex) {
		this.index = new IndexWrapperIndriImpl(index);
		origCS = new IndexBackedCollectionStats();
		origCS.setStatSource(index);

		origDocScorer = new DirichletDocScorer(origCS);
	}
	
	public void setStopper(Stopper stopper) {
		this.stopper = stopper;
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

		SearchHits initialResults = index.runQuery(query, 1000);
		
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
		
		for (int origW = 0; origW <= 10; origW++) {
			double origWeight = origW / 10.0;
			
			for (int pubmedW = 0; pubmedW <= 10 - origW; pubmedW++) {

				double pubmedWeight = pubmedW / 10.0;
				double wikiWeight = (10 - (pubmedW + origW)) / 10.0;
					
				for (SearchHit doc : initialResults) {
					
					FeatureVector origTermWeights = origTerms.get(doc.getDocno());
					FeatureVector pubmedTermWeights = pubmedTerms.get(doc.getDocno());
					FeatureVector wikiTermWeights = wikiTerms.get(doc.getDocno());
					
					FeatureVector z = new FeatureVector(null);
					Set<String> vocab = new HashSet<String>();
					vocab.addAll(origTermWeights.getFeatures());
					vocab.addAll(pubmedTermWeights.getFeatures());
					vocab.addAll(wikiTermWeights.getFeatures());
					Iterator<String> features = vocab.iterator();
					while(features.hasNext()) {
						String feature = features.next();
						double weight  = 0.0;
						weight = origWeight*origTermWeights.getFeatureWeight(feature) + 
								pubmedWeight*pubmedTermWeights.getFeatureWeight(feature) +
								wikiWeight*wikiTermWeights.getFeatureWeight(feature);
						z.addTerm(feature, weight);
					}
					
					SearchHit expandedDoc = new SearchHit();
					expandedDoc.setDocno(doc.getDocno());
					expandedDoc.setFeatureVector(z);
					
					QueryLikelihoodQueryScorer queryScorer = new QueryLikelihoodQueryScorer(origDocScorer);
					double queryScore = Math.exp(queryScorer.scoreQuery(query, expandedDoc));

					doc.setScore(queryScore);
				}
				initialResults.rank();
				
				String run = "o" + origWeight + "_p" + pubmedWeight + "_w" + wikiWeight;
				synchronized(this) {
					try {
						Writer outputWriter = new FileWriter(outdir + File.separator + run, true);
						FormattedOutputTrecEval output = new FormattedOutputTrecEval();
						output.setRunId(run);
						output.setWriter(outputWriter);
						output.write(initialResults, query.getTitle());
						outputWriter.close();
					} catch (IOException e) {
						System.err.println("Error setting file writer");
						e.printStackTrace(System.err);
						System.exit(-1);
					}
				}
			}
		}
		
	}

	public FeatureVector getTermWeights(SearchHit doc, GQuery query, Stopper stopper) {
		FeatureVector origTerms = new FeatureVector(null);

		DocScorer rmScorer = origDocScorer;

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
			System.err.println("Missing model: " + location + File.separator + doc.getDocno());
			//System.exit(-1);
		}
		return model;
	}

}
