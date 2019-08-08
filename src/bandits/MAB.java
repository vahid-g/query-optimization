package bandits;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MAB {
	
	static int SAMPLE_ARTICLE_LINK_SIZE = 1225105;
	
	public static void main(String[] args) {
		mLearn();
	}

	public static void mLearn() {
		List<String> results = new ArrayList<String>();
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement()) {
				articleSelect.setFetchSize(Integer.MIN_VALUE);
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM sample_article_1p;");
				ResultSet linkSelectResult = linkSelect
						.executeQuery("SELECT article_id, link_id FROM sample_article_link_1p;");
				
				double m = Math.sqrt(SAMPLE_ARTICLE_LINK_SIZE);
				double readArticleLinks = 0;
				int articleId = -1;
				Map<Integer, Integer> seenArmVals = new HashMap<Integer, Integer>();
				System.out.println("phase one");
				while (readArticleLinks < m) {
					if (results.size() >= 3) {
						break;
					}
					linkSelectResult.next();
					readArticleLinks++;
					int linkArticleId = linkSelectResult.getInt(1);
					if (articleId == linkArticleId) {
						System.out.println("success");
						if (seenArmVals.containsKey(articleId)) {
							seenArmVals.put(articleId, seenArmVals.get(articleId) + 1);
						} else {
							seenArmVals.put(articleId, 0);
						}
						results.add(linkArticleId + ", " + linkSelectResult.getInt(2));
					} else {
						if (articleSelectResult.next()) {
							articleId = articleSelectResult.getInt(1);
						} else {
							System.out.println("reached end of articles :(");
							System.out.println("seen arms: " + seenArmVals.keySet().size());
							System.out.println("read article-links: " + readArticleLinks);
							break;
						}
					}
				}
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
					System.out.println("joining best arm");
					while (linkSelectResult.next()) {
						int linkArticleId = linkSelectResult.getInt(1);
						if (linkArticleId == bestArm) {
							results.add(linkArticleId + ", " + linkSelectResult.getInt(2));
						}
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void mRun() {
		List<String> results = new ArrayList<String>();
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement()) {
				articleSelect.setFetchSize(Integer.MIN_VALUE);
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM sample_article_1p;");
				ResultSet linkSelectResult = linkSelect
						.executeQuery("SELECT article_id, link_id FROM sample_article_link_1p;");
				int currentSuccessCount = 0;
				double m = Math.sqrt(SAMPLE_ARTICLE_LINK_SIZE);
				double readArticleLinks = 0;
				int articleId = -1;
				Map<Integer, Integer> seenArmVals = new HashMap<Integer, Integer>();
				System.out.println("phase one");
				while (seenArmVals.size() < m || currentSuccessCount < m) {
					if (results.size() >= 3) {
						break;
					}
					linkSelectResult.next();
					readArticleLinks++;
					int linkArticleId = linkSelectResult.getInt(1);
					if (articleId == linkArticleId) {
						if (seenArmVals.containsKey(articleId)) {
							seenArmVals.put(articleId, seenArmVals.get(articleId) + 1);
						} else {
							seenArmVals.put(articleId, 0);
						}
						results.add(linkArticleId + ", " + linkSelectResult.getInt(2));
						currentSuccessCount++;
					} else {
						if (articleSelectResult.next()) {
							articleId = articleSelectResult.getInt(1);
							currentSuccessCount = 0;
						} else {
							System.out.println("reached end of articles :(");
							System.out.println("seen arms: " + seenArmVals.keySet().size());
							System.out.println("read article-links: " + readArticleLinks);
							break;
						}
					}
				}
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
					System.out.println("joining best arm");
					while (linkSelectResult.next()) {
						int linkArticleId = linkSelectResult.getInt(1);
						if (linkArticleId == bestArm) {
							results.add(linkArticleId + ", " + linkSelectResult.getInt(2));
						}
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
