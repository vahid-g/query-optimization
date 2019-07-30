package bandits;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UCBAir {

	static int scannedLinksNumber = 0;
	static int linkTableScansNumber = 0;
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
		try (DatabaseConnection dc = new DatabaseConnection();
				DatabaseConnection dc2 = new DatabaseConnection();
				DatabaseConnection dc3 = new DatabaseConnection();) {
			dc.getConnection().setAutoCommit(false);
			try (Statement articleSelect = dc.getConnection().createStatement(ResultSet.CONCUR_READ_ONLY,
					ResultSet.TYPE_FORWARD_ONLY);
					Statement linkSelect = dc2.getConnection().createStatement(ResultSet.CONCUR_READ_ONLY,
							ResultSet.TYPE_FORWARD_ONLY);
					Statement joinStatement = dc3.getConnection().createStatement()) {
				articleSelect.setFetchSize(Integer.MIN_VALUE);
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT article_id FROM article_ids;");
				ResultSet linkSelectResult = linkSelect.executeQuery("SELECT link_id FROM link_ids;");
				// topk join over R and S
				int k = 0; // parameter k from UCB-AIR algorithm
				int n = 0; // parameter n from UCB-AIR algorithm
				Map<Integer, UCBValue> idValue = new HashMap<Integer, UCBValue>();
				Set<JoinResult> results = new HashSet<JoinResult>();
				System.out.println("entering the loop");
				while (results.size() < 5) {
					if (scannedLinksNumber % 10000 == 0) {
						System.out.println("scanner " + scannedLinksNumber + "links");
					}
					n++;
					JoinResult jr = null;
					// get next unseen tuple r from R and update value of k
					int articleId = -1;
					if (k <= Math.sqrt(n)) {
						while (articleSelectResult.next()) {
							articleId = articleSelectResult.getInt("id");
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
						System.out.println("find best arm");
						articleId = findBestArm(idValue);
					}
					jr = attemptJoinAndUpdate(joinStatement, linkSelectResult, n, idValue, articleId);
					if (jr != null) {
						System.out.println("successful join!");
						results.add(jr);
					} else if (linkTableScansNumber >= LINK_SCAN_THRESHOLD) {
						break;
					}
				}
				System.out.println(results);
				System.out.println("read articles and links: " + k + ", " + scannedLinksNumber);
			}
		}
	}

	static JoinResult attemptJoinAndUpdate(Statement joinStatement, ResultSet linkSelectResult, int n,
			Map<Integer, UCBValue> idValue, int articleId) throws SQLException {
		if (linkSelectResult.next()) {
			scannedLinksNumber++;
			int linkId = linkSelectResult.getInt("id");
			String joinSql = "select article_id, link_id from tbl_article_wiki13 a, tbl_article_link_09 al, "
					+ "tbl_link_09 l where a.id = al.article_id and al.link_id = l.id and a.id = " + articleId
					+ " and l.id = " + linkId + ";";
			ResultSet joinResult = joinStatement.executeQuery(joinSql);
			// TODO should we drop the indexes on tables?
			int joinResultSize = joinResult.getFetchSize();
			updateUCBValues(idValue, linkId, +1, n);
			if (joinResultSize > 0) {
				return new JoinResult(articleId, linkId);
			}
		} else {
			System.err.println("Link table reached its end!");
			System.out.println("link scans: " + linkTableScansNumber);
			linkTableScansNumber++;
			if (linkTableScansNumber >= LINK_SCAN_THRESHOLD) {
				return null;
			}
			// linkSelectResult.first(); // this may skip the very first tuple
		}
		return null;
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
