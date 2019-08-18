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

	static final String ARTICLE_TABLE = "sample_article_1p"; // "tbl_article_09";
	static final String ARTICLE_LINK_TABLE = "sample_article_link_1p"; // "tbl_article_link_09";
	static final int ARTICLE_LINK_SIZE = 1225105; // 120916125;

	private int pageSize = 1024; // 64, 256, 1024
	private int resultSizeK = 10; // 10, 100, 1000

	private int readArticleLinks = 0;
	private int readArticles = 0;
	private int readArticlePages = 0;
	private int readArticleLinkPages = 0;

	// args[0] can be "mrun" or "nested"
	public static void main(String[] args) throws IOException {
		int[] kValues = { 10, 100, 1000 };
		int[] pageSizeValues = { 64, 256, 1024 };
		StringBuilder sb = new StringBuilder();
		sb.append("k, page-size, article-pages, link-pages, total-pages, time, result-size\r\n");
		for (int k : kValues) {
			for (int p : pageSizeValues) {
				int[] results = new ManyArmedBandits(k, p).runExperiment(args[0]);
				sb.append(k + ", " + p);
				for (int r : results) {
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
	}

	public int[] runExperiment(String method) throws IOException {
		int[] result = new int[5];
		int[] partial = null;
		int loop = 10;
		for (int i = 0; i < loop; i++) {
			System.out.println("starting experiment #" + i + " at: " + new Date().toString());
			if (method.equals("mrun")) {
				partial = mRunPaged();
			} else {
				partial = nestedLoop();
			}
			for (int j = 0; j < 5; j++) {
				result[j] += partial[j];
			}
			System.out.println("end of experiment " + new Date().toString());
		}
		for (int i = 0; i < result.length; i++) {
			result[i] /= loop;
		}
		return result;
	}

	// m-run strategy with pages as arms
	private int[] mRunPaged() {
		List<String> results = new ArrayList<String>();
		long runtime = -1;
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_READ_ONLY)) {
				// articleSelect.setFetchSize(Integer.MIN_VALUE);
				System.out.println("Default fetch size for articles: " + articleSelect.getFetchSize());
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM " + ARTICLE_TABLE + ";");
				ResultSet linkSelectResult = linkSelect
						.executeQuery("SELECT article_id, link_id FROM " + ARTICLE_LINK_TABLE + " order by rand();");
				double m = Math.sqrt(ARTICLE_LINK_SIZE / pageSize);
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
				System.out.println("running first m runs");
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
				System.out.println("running phase two");
				while (results.size() < resultSizeK) {
					// find the best page
					RelationPage bestPage = activePageHeap.poll();
					System.out.println("  value of best page: " + bestPage.value);
					// join the best page
					// TODO note that best page may have a value of zero
					if (bestPage.value == 0) {
						System.out.println("  best page has zero value!!!");
					}
					Set<Integer> articleIds = bestPage.idSet;
					int lastArticleLinkRow = readArticleLinks;
					while (linkSelectResult.next() && results.size() < resultSizeK) {
						int linkArticleId = linkSelectResult.getInt(1);
						if (articleIds.contains(linkArticleId)) {
							results.add(linkArticleId + "-" + linkSelectResult.getInt(2));
						}
					}
					linkSelectResult.absolute(lastArticleLinkRow);
					if (results.size() > resultSizeK) {
						break;
					}

					// read new page
					currentPage = readNextArticlePage(articleSelectResult);
					if (currentPage.idSet.size() < pageSize) {
						System.out.println("  reached end of article!!!");
						break;
					}
					articleLinkBufferList = readNextArticleLinkPage(linkSelectResult);
					if (articleLinkBufferList.size() < pageSize) {
						System.out.println("  reached end of article-link!!!");
						break;
					}
					// join new page with next article-link page
					for (int i = 0; i < articleLinkBufferList.size(); i++) {
						int articleLinkId = articleLinkBufferList.get(i).articleId;
						if (currentPage.idSet.contains(articleLinkBufferList.get(i).articleId)) {
							results.add(articleLinkId + "-" + articleLinkBufferList.get(i).linkId);
							currentSuccessCount++;
						}
					}
					currentPage.value = currentSuccessCount;
					activePageHeap.add(currentPage);
				}
				runtime = System.currentTimeMillis() - start;
				System.out.println("read articles: " + readArticles);
				System.out.println("read article pages: " + readArticlePages);
				System.out.println("results size: " + results.size());
				System.out.println("time(ms) = " + runtime);
				linkSelectResult.close();
			}
			return new int[] { readArticlePages, readArticleLinkPages, (readArticlePages + readArticleLinkPages),
					(int) runtime, results.size() };
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private List<ArticleLinkDAO> readNextArticleLinkPage(ResultSet linkSelectResult) throws SQLException {
		List<ArticleLinkDAO> articleLinkBufferList = new ArrayList<ArticleLinkDAO>();
		int articleLinkCounter = 0;
		while (articleLinkCounter++ < pageSize && linkSelectResult.next()) {
			readArticleLinks++;
			articleLinkCounter++;
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

	private int[] nestedLoop() {
		List<String> results = new ArrayList<String>();
		long runtime = 0;
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_READ_ONLY)) {
				System.out.println("Default fetch size for articles: " + articleSelect.getFetchSize());
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM " + ARTICLE_TABLE + ";");
				ResultSet linkSelectResult = linkSelect
						.executeQuery("SELECT article_id, link_id FROM " + ARTICLE_LINK_TABLE + " order by rand();");
				long start = System.currentTimeMillis();
				while (results.size() < resultSizeK) {
					RelationPage articlePage = readNextArticlePage(articleSelectResult);
					List<ArticleLinkDAO> articleLinkList = readNextArticleLinkPage(linkSelectResult);
					while (articleLinkList.size() > 0 && results.size() < resultSizeK) {
						for (ArticleLinkDAO articleLink : articleLinkList) {
							if (articlePage.idSet.contains(articleLink.articleId)) {
								results.add(articleLink.articleId + ", " + articleLink.linkId);
								// TODO should we break if K results are found?
							}
						}
						articleLinkList = readNextArticleLinkPage(linkSelectResult);
					} // end inner loop of the join
					linkSelectResult.first();
				} // end outer loop of the join
				runtime = System.currentTimeMillis() - start;
				linkSelectResult.close();
			} // end try for statements
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("read articles: " + readArticles);
		return new int[] { readArticlePages, readArticleLinkPages, (readArticlePages + readArticleLinkPages),
				(int) runtime, results.size() };
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
