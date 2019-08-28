package mab;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import database.DatabaseManager;

// many armed bandit strategies
public class ManyArmedBandits {

	// static final String ARTICLE_TABLE = "tbl_article_09";
	// static final String ARTICLE_LINK_TABLE = "tbl_article_link_09";
	static final String ARTICLE_TABLE = "sample_article_1p";
	static final String ARTICLE_LINK_TABLE = "sample_article_link_1p";
	static final int ARTICLE_LINK_SIZE = 120916125; // 1225105;
	static final double DISCOUNT = 0.9;

	private int pageSize;
	private int resultSizeK;

	private int readArticleLinks = 0;
	private int readArticles = 0;
	private int readArticlePages = 0;
	private int readArticleLinkPages = 0;

	private int[] readPagesForK;

	// args[0] can be "mrun" or "nested"
	public static void main(String[] args) throws IOException {
		// int[] kValues = { 10, 100, 1000, 10000 };
		// int[] pageSizeValues = { 64, 256, 1024 };
		int[] kValues = { 1000 };
		int[] pageSizeValues = { 64 };
		int[] randSeed = { 3 }; // , 5, 8, 22, 23, 2, 1000, 66 };
		StringBuilder sb = new StringBuilder();
		sb.append("k, page-size, article-pages, link-pages, total-pages, time, result-size\r\n");
		for (int p : pageSizeValues) {
			for (int k : kValues) {
				System.out.println("==========");
				System.out.println("Experimenting " + args[0] + " with k = " + k + " page-size = " + p);
				double[] results = runExperiment(args[0], k, p, randSeed);
				sb.append(k + ", " + p);
				for (double r : results) {
					sb.append(", " + r);
				}
				sb.append("\r\n");
			}
		}
		try (PrintWriter pw = new PrintWriter(new FileWriter("result_" + args[0] + ".csv"))) {
			pw.write(sb.toString());
		}
	}

	public ManyArmedBandits(int k, int p) {
		this.resultSizeK = k;
		this.pageSize = p;
		readPagesForK = new int[k];
	}

	public static double[] runExperiment(String method, int k, int p, int[] randSeed) throws IOException {
		double[] result = new double[6];
		double[] partial = null;
		for (int i = 0; i < randSeed.length; i++) {
			System.out.println(" Starting experiment #" + i + " at: " + new Date().toString());
			ManyArmedBandits mab = new ManyArmedBandits(k, p);
			if (method.equals("mrun")) {
				partial = mab.mRunPaged(randSeed[i]);
			} else {
				partial = mab.nestedLoop(randSeed[i]);
			}
			for (int j = 0; j < 5; j++) {
				result[j] += partial[j];
			}
			System.out.println(" End of experiment " + new Date().toString());
		}
		for (int i = 0; i < result.length; i++) {
			result[i] /= randSeed.length;
		}
		return result;
	}

	// m-run strategy with pages as arms
	private double[] mRunPaged(int randSeed) {
		List<String> results = new ArrayList<String>();
		long runtime = -1;
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection();
				Connection connection3 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement()) {
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect
						.executeQuery("SELECT id FROM " + ARTICLE_TABLE + " order by rand(" + randSeed + ");");
				ResultSet linkSelectResult = linkSelect.executeQuery(
						"SELECT article_id, link_id FROM " + ARTICLE_LINK_TABLE + " order by rand(" + randSeed + ");");
				double m = Math.sqrt(ARTICLE_LINK_SIZE / pageSize);
				System.out.printf("  estimated n = %d m = %.0f \r\n", (ARTICLE_LINK_SIZE / pageSize), m);
				PriorityQueue<RelationPage> activePageHeap = new PriorityQueue<RelationPage>(pageSize,
						new Comparator<RelationPage>() {
							@Override
							public int compare(RelationPage o1, RelationPage o2) {
								return -1 * o1.value.compareTo(o2.value);
							}
						});
				long start = System.currentTimeMillis();
				int currentSuccessCount = 0;
				boolean successfulPrevJoin = false;
				List<ArticleLinkDAO> articleLinkBufferList = null;
				RelationPage currentPage = readNextArticlePage(articleSelectResult);
				System.out.println("  running first m runs");
				while (activePageHeap.size() < m) {
					if (results.size() >= resultSizeK) {
						System.out.println("found " + resultSizeK + " results in first m run");
						break;
					}
					if (currentSuccessCount >= m) {
						System.out.println("found m consecutive successes");
						currentPage.value = currentSuccessCount;
						activePageHeap.add(currentPage);
						break;
					}
					// read article-link page
					articleLinkBufferList = readNextArticleLinkPage(linkSelectResult);
					// attempt join
					successfulPrevJoin = false;
					for (int i = 0; i < articleLinkBufferList.size(); i++) {
						int articleLinkId = articleLinkBufferList.get(i).articleId;
						if (currentPage.idSet.contains(articleLinkBufferList.get(i).articleId)) {
							results.add(articleLinkId + "-" + articleLinkBufferList.get(i).linkId);
							// discAverage += Math.pow(discFactor, results.size()) * readArticleLinkPages;
							readPagesForK[results.size()] = readArticleLinks + readArticleLinkPages;
							currentSuccessCount++;
							successfulPrevJoin = true;
						}
					}
					// update values and read new article page
					if (!successfulPrevJoin) {
						currentPage.value = currentSuccessCount;
						activePageHeap.add(currentPage);
						currentSuccessCount = 0;
						currentPage = readNextArticlePage(articleSelectResult);
					}
				}
				System.out.println("  phase one done.");
				System.out.println("  articlePages: " + readArticlePages + " articleLinkPages: " + readArticleLinkPages
						+ " size: " + results.size());
				System.out.println("  running phase two");
				int phaseTwoIterations = 0;
				while (results.size() < resultSizeK) {
					phaseTwoIterations++;
					// find the best page
					RelationPage bestPage = activePageHeap.poll();
					System.out.println("    value of best page: " + bestPage.value);
					// join the best page
					// note that best page may have a value of zero
					Set<Integer> articleIds = bestPage.idSet;
					try (Statement wholeLinkSelect = connection3.createStatement()) {
						ResultSet wholeLinkSelectResult = wholeLinkSelect
								.executeQuery("SELECT article_id, link_id FROM " + ARTICLE_LINK_TABLE
										+ " order by rand(" + randSeed + ");");
						int linkCounter = 0;
						while (wholeLinkSelectResult.next() && results.size() < resultSizeK) {
							linkCounter++;
							int linkArticleId = wholeLinkSelectResult.getInt(1);
							if (articleIds.contains(linkArticleId)) {
								results.add(linkArticleId + "-" + wholeLinkSelectResult.getInt(2));
								// discAverage += Math.pow(discFactor, results.size()) * readArticleLinkPages;
								readPagesForK[results.size()] = readArticleLinks + readArticleLinkPages;
							}
						}
						System.out.println("    inner article-link pages: " + (linkCounter / pageSize));
						readArticleLinkPages += (linkCounter / pageSize);
						try {
							wholeLinkSelectResult.close();
						} catch (SQLException e) {
							System.err.println("     couldn't close inner resultset!!!");
							e.printStackTrace();
						}
					}
					if (results.size() > resultSizeK) {
						break;
					}

					// read new article page
					currentPage = readNextArticlePage(articleSelectResult);
					if (currentPage.idSet.size() == 0) {
						System.out.println("    reached end of article!!!");
						break;
					}
					// read new article-link page
					articleLinkBufferList = readNextArticleLinkPage(linkSelectResult);
					if (articleLinkBufferList.size() == 0) {
						System.out.println("    reached end of article-link!!!");
						break;
					}
					// join new page with next article-link page
					for (int i = 0; i < articleLinkBufferList.size(); i++) {
						int articleLinkId = articleLinkBufferList.get(i).articleId;
						if (currentPage.idSet.contains(articleLinkId)) {
							results.add(articleLinkId + "-" + articleLinkBufferList.get(i).linkId);
							// discAverage += Math.pow(discFactor, results.size()) * readArticleLinkPages;
							readPagesForK[results.size()] = readArticleLinks + readArticleLinkPages;
							currentSuccessCount++;
						}
					}
					currentPage.value = currentSuccessCount;
					activePageHeap.add(currentPage);
				}
				runtime = System.currentTimeMillis() - start;
				System.out.println("  phase two iterations: " + phaseTwoIterations);
				System.out.println("  read articles: " + readArticles);
				System.out.println("  read article pages: " + readArticlePages);
				System.out.println("  read article links: " + readArticleLinks);
				System.out.println("  read article-link pages: " + readArticleLinkPages);
				System.out.println("  results size: " + results.size());
				System.out.println("  time(ms) = " + runtime);
				try {
					System.out.println("  started manual closing");
					while (linkSelectResult.next())
						; // do nothing
					linkSelectResult.close();
				} catch (SQLException e) {
					System.err.println("  couldn't close outer resultset!!!");
					e.printStackTrace();
					System.err.println("===");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new double[] { readArticlePages, readArticleLinkPages, (readArticlePages + readArticleLinkPages),
				(int) runtime, results.size(), getDiscountedAverage() };
	}

	private double getDiscountedAverage() {
		double discAverageK = 0;
		for (int j = 0; j < this.resultSizeK; j++) {
			discAverageK += Math.pow(0.9, resultSizeK) * this.readPagesForK[j];
		}
		return discAverageK;
	}

	private List<ArticleLinkDAO> readNextArticleLinkPage(ResultSet linkSelectResult) throws SQLException {
		List<ArticleLinkDAO> articleLinkBufferList = new ArrayList<ArticleLinkDAO>();
		while (articleLinkBufferList.size() < pageSize && linkSelectResult.next()) {
			readArticleLinks++;
			articleLinkBufferList.add(new ArticleLinkDAO(linkSelectResult.getInt(1), linkSelectResult.getInt(2)));
		}
		readArticleLinkPages++;
		return articleLinkBufferList;
	}

	private RelationPage readNextArticlePage(ResultSet articleSelectResult) throws SQLException {
		RelationPage currentPage = new RelationPage();
		while (currentPage.idSet.size() < pageSize && articleSelectResult.next()) {
			readArticles++;
			currentPage.idSet.add(articleSelectResult.getInt(1));
		}
		readArticlePages++;
		return currentPage;
	}

	private double[] nestedLoop(int randSeed) {
		List<String> results = new ArrayList<String>();
		long runtime = 0;
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_READ_ONLY)) {
				System.out.println("Default fetch size for articles: " + articleSelect.getFetchSize());
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect
						.executeQuery("SELECT id FROM " + ARTICLE_TABLE + " order by rand(" + randSeed + ");");
				ResultSet linkSelectResult = linkSelect.executeQuery(
						"SELECT article_id, link_id FROM " + ARTICLE_LINK_TABLE + " order by rand(" + randSeed + ");");
				long start = System.currentTimeMillis();
				while (results.size() < resultSizeK) {
					RelationPage articlePage = readNextArticlePage(articleSelectResult);
					List<ArticleLinkDAO> articleLinkList;
					while (results.size() < resultSizeK) {
						articleLinkList = readNextArticleLinkPage(linkSelectResult);
						if (articleLinkList == null || articleLinkList.size() == 0) {
							break;
						}
						for (ArticleLinkDAO articleLink : articleLinkList) {
							if (articlePage.idSet.contains(articleLink.articleId)) {
								results.add(articleLink.articleId + ", " + articleLink.linkId);
								if (results.size() >= resultSizeK) {
									break;
								}
							}
						}

					} // end inner loop of the join
					linkSelectResult.first();
				} // end outer loop of the join
				runtime = System.currentTimeMillis() - start;
				System.out.println("read article pages: " + readArticlePages);
				linkSelectResult.close();
			} // end try for statements
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("read articles: " + readArticles);
		return new double[] { readArticlePages, readArticleLinkPages, (readArticlePages + readArticleLinkPages),
				(int) runtime, results.size(), getDiscountedAverage() };
	}

	static class RelationPage {
		Set<Integer> idSet;
		Integer value = 0;

		public RelationPage(int value, Set<Integer> idSet) {
			this.value = value;
			this.idSet = idSet;
		}

		public RelationPage() {
			idSet = new HashSet<Integer>();
		}
	}

	static class ArticleLinkDAO {
		int articleId = 0;
		int linkId = 0;

		public ArticleLinkDAO(int articleId, int linkId) {
			this.articleId = articleId;
			this.linkId = linkId;
		}
	}

	static enum ExperimentMode {
		M_RUN, M_LEARNING, NON_REC
	}

}
