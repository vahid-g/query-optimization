package bandits;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

// many armed bandit strategies
public class MAB {

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

	static final int SAMPLE_ARTICLE_LINK_SIZE = 1225105;
	static final int ARTICLE_LINK_SIZE = 120916125;
	static final int PAGE_SIZE = 1024;
	static final int RESULT_SIZE_K = 3;

	public static void main(String[] args) throws IOException {
		List<String> results = new ArrayList<String>();
		results.add("link_pages, article_pages, time, result_size");
		for (int i = 0; i < 1; i++) {
			System.out.println("starting experiment #" + i + " at: " + new Date().toString());
			results.add(new MAB().mRunPaged());
			System.out.println("end of experiment " + new Date().toString());
		}
		try (PrintWriter pw = new PrintWriter(new FileWriter("join.csv"))) {
			for (String s : results) {
				pw.println(s);
			}
		}
	}

	int readArticleLinks = 0;
	int readArticles = 0;
	int readArticlePages = 0;
	int readArticleLinkPages = 0;

	// m-run strategy with pages as arms
	public String mRunPaged() {
		List<String> results = new ArrayList<String>();
		long runtime = -1;
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_READ_ONLY)) {
				// articleSelect.setFetchSize(Integer.MIN_VALUE);
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
				System.out.println("read article pages: " + readArticlePages);
				System.out.println("results size: " + results.size());
				System.out.println(results);
				System.out.println("time(ms) = " + runtime);
				linkSelectResult.close();
			}			
			return readArticlePages + "," + readArticleLinkPages + "," + runtime + "," + results.size();
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

	public static void mabJoinExperiment(String[] args) {
		if (args.length == 0 || args[0].equals("mrun")) {
			mabJoin(ExperimentMode.M_RUN);
		} else if (args[0].equals("mlearning")) {
			mabJoin(ExperimentMode.M_LEARNING);
		} else if (args[0].equals("nonrec")) {
			mabJoin(ExperimentMode.NON_REC);
		} else if (args[0].equals("nested")) {
			nestedLoop();
		} else {
			System.out.println("method not found");
		}
	}

	// different MAB strategies with tuples as arms
	public static void mabJoin(ExperimentMode mode) {
		List<String> results = new ArrayList<String>();
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement()) {
				articleSelect.setFetchSize(Integer.MIN_VALUE);
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				// ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM
				// sample_article_1p;");
				ResultSet articleSelectResult = articleSelect
						.executeQuery("SELECT id FROM tbl_article_09 order by rand();");
				ResultSet linkSelectResult = linkSelect
						.executeQuery("SELECT article_id, link_id FROM tbl_article_link_09 order by rand(1);");
				// .executeQuery("SELECT article_id, link_id FROM sample_article_link_1p order
				// by rand();");
				int currentSuccessCount = 0;
				// double b = 0.0000445;
				double b = 1;
				double n = ARTICLE_LINK_SIZE;
				double m = 0;
				if (mode == ExperimentMode.M_RUN || mode == ExperimentMode.NON_REC) {
					m = Math.sqrt(n * b); // assuming a ~= 0
				} else if (mode == ExperimentMode.M_LEARNING) {
					m = Math.sqrt((n * b)) * Math.log(n * b);
				}
				int readArticleLinks = 0;
				int readArticles = 0;
				int articleTableScans = 1;
				int articleId = 0;
				Map<Integer, Integer> seenArmVals = new HashMap<Integer, Integer>();
				System.out.println("phase one");
				while (linkSelectResult.next()) {
					readArticleLinks++;
					if (articleTableScans >= 10) {
						System.out.println("max articleTableScans reached");
						break;
					} else if (results.size() >= 3) {
						System.out.println("found k results");
						break;
					} else if (mode == ExperimentMode.M_LEARNING && readArticleLinks >= m) {
						System.out.println("m-learning finished phase one");
						break;
					} else if (mode == ExperimentMode.NON_REC && currentSuccessCount > m) {
						System.out.println("non-recalling m-run finished phase ");
						break;
					} else if (mode == ExperimentMode.M_RUN && (seenArmVals.size() >= m || currentSuccessCount > m)) {
						System.out.println("m-run finished phase one with");
						System.out.println("  m = " + m);
						System.out.println("  tried arms = " + seenArmVals.size());
						System.out.println("  current suuccess count = " + currentSuccessCount);
						break;
					}
					int linkArticleId = linkSelectResult.getInt(1);
					if (articleId == linkArticleId) {
						System.out.println("success at article: " + articleId);
						seenArmVals.put(articleId, seenArmVals.get(articleId) + 1);
						results.add(linkArticleId + "-" + linkSelectResult.getInt(2));
						currentSuccessCount++;
					} else { // attempt reading a new articleId
						if (articleSelectResult.next()) {
							readArticles++;
							articleId = articleSelectResult.getInt(1);
							currentSuccessCount = 0;
							if (!seenArmVals.containsKey(articleId)) {
								seenArmVals.put(articleId, 0);
							}
						} else { // tbl_article_09 reached its end
							System.out.println("reached end of articles!");
							System.out.println("  read links: " + readArticleLinks);
							System.out.println("  read articles: " + readArticles);
							System.out.println("  article table scans: " + articleTableScans);
							articleSelectResult.close();
							break;
							// articleTableScans++;
							// articleSelectResult = articleSelect.executeQuery("SELECT id FROM
							// sample_article_1p;");
							// articleId = 0;
							// readArticles = 0;
						}
					}
				}
				System.out.println("===============");
				System.out.println("read links: " + readArticleLinks);
				System.out.println("read articles: " + readArticles);
				System.out.println("article table scans: " + articleTableScans);
				if (results.size() < 3) {
					// find best arm
					int bestArm = -1;
					int bestVal = 0;
					System.out.println("finding best arm");
					for (Integer key : seenArmVals.keySet()) {
						if (seenArmVals.get(key) > bestVal) {
							bestArm = key;
							bestVal = seenArmVals.get(key);
						}
					}
					System.out.println("best arm: " + bestArm);
					// join best arm
					if (bestArm != -1) {
						System.out.println("joining best arm");
						while (linkSelectResult.next() && results.size() < 3) {
							readArticleLinks++;
							int linkArticleId = linkSelectResult.getInt(1);
							if (linkArticleId == bestArm) {
								results.add(linkArticleId + "-" + linkSelectResult.getInt(2));
							}
						}
					}
				}
				System.out.println("read links: " + readArticleLinks);
				System.out.println("results size: " + results.size());
				System.out.println(results);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void nestedLoop() {
		List<String> results = new ArrayList<String>();
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement linkSelect = connection2.createStatement()) {
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet linkSelectResult = linkSelect
						// .executeQuery("SELECT article_id, link_id FROM sample_article_link_1p;");
						.executeQuery("SELECT article_id, link_id FROM tbl_article_link_09 order by rand(1);");
				int readArticleLinks = 0;
				int articleId = -1;
				while (linkSelectResult.next() && results.size() < 3) {
					if (readArticleLinks % 10000 == 0) {
						System.out.println("  read article-links: " + readArticleLinks);
					}
					readArticleLinks++;
					int linkArticleId = linkSelectResult.getInt(1);
					System.out.println("  joining link-article-id: " + linkArticleId);
					try (Statement articleSelect = connection1.createStatement();) {
						articleSelect.setFetchSize(Integer.MIN_VALUE);
						// ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM
						// sample_article_1p;");
						ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM tbl_article_09;");
						while (articleSelectResult.next() && results.size() < 3) {
							articleId = articleSelectResult.getInt(1);
							if (articleId == linkArticleId) {
								System.out.println("success");
								results.add(linkArticleId + ", " + linkSelectResult.getInt(2));
							}
						}
					}
				}
				System.out.println("read links: " + readArticleLinks);
				System.out.println("results: " + results.size());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
