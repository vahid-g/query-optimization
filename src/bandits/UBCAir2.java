package bandits;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// reads two tables R, S consisting of repeated article IDs with different orderings and joins them using a UBC approach
// at each round, one tuple is read from S sequentially and the algorithm decides to read a new tuple 
// from R or do explore exploitationg on the already seen tuples from R
public class UBCAir2 {

	static int scannedLinksNumber = 0;
	static int linkTableScansNumber = 0;
	static int scannedArticleNumber = 0;
	final static int LINK_SCAN_THRESHOLD = 1;

	static class UCBValue {
		double totalReward = 0;
		double totalSquaredReward = 0;
		double selectedTimes = 0;
		double value = 0;

		void increaseReward(int reward) {
			totalReward += reward;
			totalSquaredReward += Math.pow(reward, 2);
		}

		void updateValue(int n) {
			double meanReward = (totalReward / selectedTimes);
			double meanSquredReward = (totalSquaredReward / selectedTimes);
			double V = (Math.pow(meanReward, 2) - meanSquredReward) / selectedTimes;
			value = meanReward + Math.sqrt(2 * V * Math.log(n) / selectedTimes) + (3 * Math.log(n) / selectedTimes);
		}

	}

	static class JoinResult {
		int articleId;
		int linkId;

		public JoinResult(int articleId, int linkId) {
			this.articleId = articleId;
			this.linkId = linkId;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (!(o instanceof JoinResult)) {
				return false;
			}
			JoinResult jr = (JoinResult) o;
			return jr.articleId == this.articleId && jr.linkId == this.linkId;
		}

		@Override
		public String toString() {
			return articleId + " - " + linkId;
		}
	}

	public static void main(String[] args) throws IOException, SQLException {
		try (Connection connection1 = DatabaseManager.createConnection();
				Connection connection2 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement()) {
				articleSelect.setFetchSize(Integer.MIN_VALUE);
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT article_id FROM article_ids;");
				// ResultSet linkSelectResult = linkSelect.executeQuery("SELECT link_id FROM
				// link_ids;");
				ResultSet linkSelectResult = linkSelect.executeQuery("SELECT article_id FROM article_ids_2;");
				// topk join over R and S
				int k = 0; // parameter k from UCB-AIR algorithm
				int n = 0; // parameter n from UCB-AIR algorithm
				Map<Integer, UCBValue> idValue = new HashMap<Integer, UCBValue>();
				Set<JoinResult> results = new HashSet<JoinResult>();
				while (results.size() < 100) {
					if (scannedLinksNumber % 100000 == 0) {
						System.out.println("scanned links: " + scannedLinksNumber);
						System.out.println("scanned articles: " + scannedArticleNumber);
						System.out.println("index size: " + idValue.size());
					}
					n++;
					// get next unseen tuple r from R and update value of k
					int articleId = -1;
					if (k <= Math.sqrt(n)) {
						while (articleSelectResult.next()) {
							scannedArticleNumber++;
							articleId = articleSelectResult.getInt(1);
							if (!idValue.containsKey(articleId)) {
								k++;
								break;
							}
						}
						if (articleId == -1) {
							System.err.println("Article table reached its end!");
							break;
						}
						// join r with random tuple s from S
					} else {
						articleId = findBestArm(idValue);
					}
					// jr = attemptJoinAndUpdate(linkSelectResult, n, idValue, articleId);
					if (linkSelectResult.next()) {
						scannedLinksNumber++;
						int linkId = linkSelectResult.getInt(1);
						if (articleId == linkId) {
							results.add(new JoinResult(articleId, linkId));
						}
						updateUCBValues(idValue, linkId, +1, n);
					} else {
						System.out.println("links does not return next tuple");
						break;
					}
				}
				System.out.println(results);
				System.out.println("scanned links: " + scannedLinksNumber);
				System.out.println("scanned articles: " + scannedArticleNumber);
			}
		}
	}

	static void updateUCBValues(Map<Integer, UCBValue> idValue, int selectedId, int reward, int n) {
		if (!idValue.containsKey(selectedId)) {
			UCBValue newValue = new UCBValue();
			idValue.put(selectedId, newValue);
		}
		for (int id : idValue.keySet()) {
			UCBValue value = idValue.get(id);
			if (id == selectedId) {
				value.selectedTimes++;
				value.increaseReward(reward);
			}
			value.updateValue(n);
		}
	}

	static int findBestArm(Map<Integer, UCBValue> idValue) {
		double maxValue = -1;
		int bestId = -1;
		for (int id : idValue.keySet()) {
			double value = idValue.get(id).value;
			if (maxValue < value) {
				maxValue = value;
				bestId = id;
			}
		}
		return bestId;
	}

}
