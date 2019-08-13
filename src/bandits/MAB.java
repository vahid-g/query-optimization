package bandits;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MAB {

	static enum ExperimentMode {
		M_RUN, M_LEARNING, NON_REC
	}

	static int SAMPLE_ARTICLE_LINK_SIZE = 1225105;
	static int ARTICLE_LINK_SIZE = 120916125;

	public static void main(String[] args) {
		System.out.println("starting experiment " + new Date().toString());
		if (args.length == 0 || args[0].equals("mrun")) {
			mRun(ExperimentMode.M_RUN);
		} else if (args[0].equals("mlearning")) {
			mRun(ExperimentMode.M_LEARNING);
		} else if (args[0].equals("nonrec")) {
			mRun(ExperimentMode.NON_REC);
		} else if (args[0].equals("nested")) {
			nestedLoop();
		} else {
			System.out.println("method not found");
		}
		System.out.println("end of experiment " + new Date().toString());
	}

	public static void mRun(ExperimentMode mode) {
		List<String> results = new ArrayList<String>();
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement()) {
				articleSelect.setFetchSize(Integer.MIN_VALUE);
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				// ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM
				// sample_article_1p;");
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM tbl_article_09;");
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
				while (linkSelectResult.next() || results.size() < 3) {
					if (readArticleLinks % 10000 == 0) {
						System.out.println("  read article-links: " + readArticleLinks);
					}
					readArticleLinks++;
					int linkArticleId = linkSelectResult.getInt(1);
					try (Statement articleSelect = connection1.createStatement();) {
						int readArticles = 0;
						articleSelect.setFetchSize(Integer.MIN_VALUE);
						// ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM
						// sample_article_1p;");
						ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM tbl_article_09;");
						while (articleSelectResult.next() && results.size() < 3) {
							if (readArticles % 10000 == 0) {
								System.out.println("    read articles: " + readArticles);
							}
							readArticles++;
							readArticles = articleSelectResult.getInt(1);
							if (articleId == linkArticleId) {
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
