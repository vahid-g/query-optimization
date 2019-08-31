package mab;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
	// static final int ARTICLE_LINK_SIZE = 120916125;
	static final String ARTICLE_TABLE = "sample_article_1p";
	static final String ARTICLE_LINK_TABLE = "sample_article_link_1p";
	static final int ARTICLE_LINK_SIZE = 1225105;
	static final double DISCOUNT = 0.9;

	private int pageSize;
	private int resultSizeK;

	private int readArticleLinks = 0;
	private int readArticles = 0;
	private int readArticlePages = 0;
	private int readArticleLinkPages = 0;

	private int[] readPagesForK;

	// args[0] can be "mrun" or "nested"
	// args[1] is k
	// args[2] is the page size
	public static void main(String[] args) throws IOException {
		int[] articleRandSeed = { 5, 6, 20, 666, 69 };
		int[] linkRandSeed = { 91, 17, 7, 68, 59 };
		double[] result = new double[4];
		for (int i = 0; i < articleRandSeed.length; i++) {
			double[] partial;
			if (args[0].equals("mrun")) {
				partial = new ManyArmedBandits(Integer.parseInt(args[1]), Integer.parseInt(args[2]))
						.mRunPaged(articleRandSeed[i], linkRandSeed[i]);
			} else {
				partial = new ManyArmedBandits(Integer.parseInt(args[1]), Integer.parseInt(args[2]))
						.nestedLoop(articleRandSeed[i], linkRandSeed[i]);
			}
			for (int j = 0; j < result.length; j++) {
				result[j] += partial[j];
			}
		}
		System.out.println("===========");
		System.out.println("average article pages, link pages, discounted average and time:");
		for (int i = 0; i < result.length; i++) {
			result[i] /= articleRandSeed.length;
		}
		System.out.println(Arrays.toString(result));
	}

	public static void runExperiment(String method) throws IOException {
		int[] kValues = { 100, 1000 };
		int[] pageSizeValues = { 64, 256 };
		int[] randSeed = { 3, 5, 8, 22, 23 };
		StringBuilder sb = new StringBuilder();
		sb.append("k, page-size, article-pages, link-pages, total-pages, time, result-size\r\n");
		for (int p : pageSizeValues) {
			for (int k : kValues) {
				System.out.println("==========");
				System.out.println("Experimenting " + method + " with k = " + k + " page-size = " + p);
				double[] result = new double[3];
				double[] partial = null;
				for (int i = 0; i < randSeed.length; i++) {
					System.out.println(" Starting experiment #" + i + " at: " + new Date().toString());
					ManyArmedBandits mab = new ManyArmedBandits(k, p);
					if (method.equals("mrun")) {
						partial = mab.mRunPaged(randSeed[i], randSeed[i]);
					} else {
						partial = mab.nestedLoop(randSeed[i], randSeed[i]);
					}
					for (int j = 0; j < 5; j++) {
						result[j] += partial[j];
					}
					System.out.println(" End of experiment " + new Date().toString());
				}
				for (int i = 0; i < result.length; i++) {
					result[i] /= randSeed.length;
				}
				sb.append(k + ", " + p);
				for (double r : result) {
					sb.append(", " + r);
				}
				sb.append("\r\n");
			}
		}
		try (PrintWriter pw = new PrintWriter(new FileWriter("result_" + method + ".csv"))) {
			pw.write(sb.toString());
		}
	}

	public ManyArmedBandits(int k, int p) {
		resultSizeK = k;
		pageSize = p;
		readPagesForK = new int[resultSizeK];
		System.out.println("MAB initialized with k = " + resultSizeK + " page-size = " + p);
	}

	// m-run strategy with pages as arms
	private double[] mRunPaged(int artccileRandSeed, int linkRandSeed) {
		List<String> results = new ArrayList<String>();
		long runtime = -1;
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection();
				Connection connection3 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement()) {
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect
						.executeQuery("SELECT id FROM " + ARTICLE_TABLE + " order by rand(" + artccileRandSeed + ");");
				ResultSet linkSelectResult = linkSelect.executeQuery("SELECT article_id, link_id FROM "
						+ ARTICLE_LINK_TABLE + " order by rand(" + linkRandSeed + ");");
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
							readPagesForK[results.size() - 1] = readArticleLinkPages + readArticleLinkPages;
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
				boolean articleTableExhausted = false;
				boolean linkTableExhausted = false;
				while (results.size() < resultSizeK && !activePageHeap.isEmpty()) {
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
										+ " order by rand(" + linkRandSeed + ");");
						int linkCounter = 0;
						while (wholeLinkSelectResult.next() && results.size() < resultSizeK) {
							linkCounter++;
							int linkArticleId = wholeLinkSelectResult.getInt(1);
							if (articleIds.contains(linkArticleId)) {
								results.add(linkArticleId + "-" + wholeLinkSelectResult.getInt(2));
								readPagesForK[results.size() - 1] = readArticleLinkPages + readArticleLinkPages;
							}
						}
						// System.out.println(" inner article-link pages scanned: " + (linkCounter /
						// pageSize));
						// System.out.println(" result size: " + results.size());
						readArticleLinkPages += (linkCounter / pageSize);
						try {
							wholeLinkSelectResult.close();
						} catch (SQLException e) {
							System.err.println("     couldn't close inner resultset!!!");
							e.printStackTrace();
						}
					}
					if (results.size() >= resultSizeK) {
						break;
					}

					if (!articleTableExhausted && !linkTableExhausted) {
						// read new article page
						currentPage = readNextArticlePage(articleSelectResult);
						if (currentPage.idSet.size() == 0) {
							System.out.println("    reached end of article!!!");
							articleTableExhausted = true;
							continue;
						}
						currentSuccessCount = 0;
						do {
							// read new article-link page
							articleLinkBufferList = readNextArticleLinkPage(linkSelectResult);
							if (articleLinkBufferList.size() > 0) {
								System.out.println("    reached end of article-link!!!");
								linkTableExhausted = true;
								break;
							}
							// join new page with next article-link page
							for (int i = 0; i < articleLinkBufferList.size(); i++) {
								int articleLinkId = articleLinkBufferList.get(i).articleId;
								if (currentPage.idSet.contains(articleLinkId)) {
									results.add(articleLinkId + "-" + articleLinkBufferList.get(i).linkId);
									if (results.size() > resultSizeK) {
										break;
									}
									readPagesForK[results.size() - 1] = readArticleLinkPages + readArticleLinkPages;
									currentSuccessCount++;

								}
							}
						} while (currentSuccessCount > 0);
						currentPage.value = currentSuccessCount;
						activePageHeap.add(currentPage);
					}
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
					linkSelect.close();
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
		return new double[] { readArticlePages, readArticleLinkPages, readArticlePages + readArticleLinkPages,
				getDiscountedAverage(), runtime };
	}

	private double getDiscountedAverage() {
		double discAverageK = 0;
		for (int j = 0; j < this.resultSizeK; j++) {
			discAverageK += Math.pow(0.9, j) * this.readPagesForK[j];
		}
		return Math.round(discAverageK * 100) / 100;
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

	private double[] nestedLoop(int articleRandSeed, int linkRandSeed) {
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
						.executeQuery("SELECT id FROM " + ARTICLE_TABLE + " order by rand(" + articleRandSeed + ");");
				ResultSet linkSelectResult = linkSelect.executeQuery("SELECT article_id, link_id FROM "
						+ ARTICLE_LINK_TABLE + " order by rand(" + linkRandSeed + ");");
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
								readPagesForK[results.size() - 1] = readArticlePages + readArticleLinkPages;
								if (results.size() >= resultSizeK) {
									break;
								}
							}
						}

					} // end inner loop of the join
					linkSelectResult.first();
				} // end outer loop of the join
				runtime = System.currentTimeMillis() - start;
				System.out.println("  read articles: " + readArticles);
				System.out.println("  read article pages: " + readArticlePages);
				System.out.println("  read article links: " + readArticleLinks);
				System.out.println("  read article-link pages: " + readArticleLinkPages);
				System.out.println("  results size: " + results.size());
				linkSelectResult.close();
			} // end try for statements
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("read articles: " + readArticles);
		return new double[] { readArticlePages, readArticleLinkPages, readArticlePages + readArticleLinkPages,
				getDiscountedAverage(), runtime };
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
