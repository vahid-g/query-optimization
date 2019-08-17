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

	static final int SAMPLE_ARTICLE_LINK_SIZE = 1225105;
	static final int ARTICLE_LINK_SIZE = 120916125;
	static final int PAGE_SIZE = 1024;
	static final int RESULT_SIZE_K = 3;

	private int readArticleLinks = 0;
	private int readArticles = 0;
	private int readArticlePages = 0;
	private int readArticleLinkPages = 0;

	public static void main(String[] args) throws IOException {
		if (args[0].equals("mrun")) {
			new ManyArmedBandits().runMRunPaged();
		} else if (args[1].equals("nested")) {
			new ManyArmedBandits().runNestedLoop();
		} else {
			System.out.println("command not found");
		}
	}

	public void runMRunPaged() throws IOException {
		List<String> results = new ArrayList<String>();
		for (int i = 0; i < 20; i++) {
			System.out.println("starting experiment #" + i + " at: " + new Date().toString());
			results.add(new ManyArmedBandits().mRunPaged());
			System.out.println("end of experiment " + new Date().toString());
		}
		try (PrintWriter pw = new PrintWriter(new FileWriter("join.csv"))) {
			pw.println("article-pages, link-pages, total-pages, time (ms), result-size");
			for (String s : results) {
				pw.println(s);
			}
		}
	}

	public void runNestedLoop() throws IOException {
		List<String> results = new ArrayList<String>();
		for (int i = 0; i < 20; i++) {
			System.out.println("starting experiment #" + i + " at: " + new Date().toString());
			results.add(new ManyArmedBandits().nestedLoop());
			System.out.println("end of experiment " + new Date().toString());
		}
		try (PrintWriter pw = new PrintWriter(new FileWriter("join.csv"))) {
			pw.println("article-pages, link-pages, total-pages, time (ms), result-size");
			for (String s : results) {
				pw.println(s);
			}
		}
	}

	// m-run strategy with pages as arms
	public String mRunPaged() {
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
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM tbl_article_09;");
				ResultSet linkSelectResult = linkSelect
						.executeQuery("SELECT article_id, link_id FROM tbl_article_link_09 order by rand();");
				double m = Math.sqrt(ARTICLE_LINK_SIZE / PAGE_SIZE);
				PriorityQueue<RelationPage> activePageHeap = new PriorityQueue<RelationPage>(PAGE_SIZE,
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
					if (results.size() >= RESULT_SIZE_K) {
						System.out.println("found " + RESULT_SIZE_K + " results in first m run");
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
				while (results.size() < RESULT_SIZE_K) {
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
					while (linkSelectResult.next() && results.size() < RESULT_SIZE_K) {
						int linkArticleId = linkSelectResult.getInt(1);
						if (articleIds.contains(linkArticleId)) {
							results.add(linkArticleId + "-" + linkSelectResult.getInt(2));
						}
					}
					linkSelectResult.absolute(lastArticleLinkRow);
					if (results.size() > RESULT_SIZE_K) {
						break;
					}

					// read new page
					currentPage = readNextArticlePage(articleSelectResult);
					if (currentPage.idSet.size() < PAGE_SIZE) {
						System.out.println("  reached end of article!!!");
						break;
					}
					articleLinkBufferList = readNextArticleLinkPage(linkSelectResult);
					if (articleLinkBufferList.size() < PAGE_SIZE) {
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
			return readArticlePages + "," + readArticleLinkPages + "," + (readArticlePages + readArticleLinkPages) + ","
					+ runtime + "," + results.size();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}

	private List<ArticleLinkDAO> readNextArticleLinkPage(ResultSet linkSelectResult) throws SQLException {
		List<ArticleLinkDAO> articleLinkBufferList = new ArrayList<ArticleLinkDAO>();
		int articleLinkCounter = 0;
		while (articleLinkCounter++ < PAGE_SIZE && linkSelectResult.next()) {
			readArticleLinks++;
			articleLinkCounter++;
			articleLinkBufferList.add(new ArticleLinkDAO(linkSelectResult.getInt(1), linkSelectResult.getInt(2)));
		}
		readArticleLinkPages++;
		return articleLinkBufferList;
	}

	private RelationPage readNextArticlePage(ResultSet articleSelectResult) throws SQLException {
		RelationPage currentPage = new RelationPage();
		while (currentPage.idSet.size() < PAGE_SIZE && articleSelectResult.next()) {
			readArticles++;
			currentPage.idSet.add(articleSelectResult.getInt(1));
		}
		readArticlePages++;
		return currentPage;
	}

	private String nestedLoop() {
		List<String> results = new ArrayList<String>();
		long time = 0;
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_READ_ONLY)) {
				System.out.println("Default fetch size for articles: " + articleSelect.getFetchSize());
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM tbl_article_09;");
				ResultSet linkSelectResult = linkSelect
						.executeQuery("SELECT article_id, link_id FROM tbl_article_link_09 order by rand();");
				int articleId = -1;
				long start = System.currentTimeMillis();
				while (articleSelectResult.next() && results.size() < 3) {
					readArticles++;
					articleId = articleSelectResult.getInt(1);
					while (linkSelectResult.next() && results.size() < 3) {
						readArticleLinks++;
						int linkArticleId = linkSelectResult.getInt(1);
						if (articleId == linkArticleId) {
							results.add(linkArticleId + ", " + linkSelectResult.getInt(2));
						}
					} // end inner loop of the join
					linkSelectResult.first();
				} // end outer loop of the join
				time = System.currentTimeMillis() - start;
				linkSelectResult.close();
			} // end try for statements
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (readArticles / PAGE_SIZE) + "," + (readArticleLinks / PAGE_SIZE) + ","
				+ ((readArticles + readArticleLinks) / PAGE_SIZE) + "," + time + "," + results.size();
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
