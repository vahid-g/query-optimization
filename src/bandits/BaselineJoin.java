package bandits;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class BaselineJoin {

	static int scannedLinksNumber = 0;
	static int linkTableScansNumber = 0;
	final static int LINK_SCAN_THRESHOLD = 1;

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
				Connection connection2 = DatabaseManager.createConnection();
				Connection connection3 = DatabaseManager.createConnection()) {
			try (Statement articleSelect = connection1.createStatement();
					Statement linkSelect = connection2.createStatement();
					Statement joinStatement = connection3.createStatement()) {
				articleSelect.setFetchSize(Integer.MIN_VALUE);
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT article_id FROM article_ids;");
				ResultSet linkSelectResult = linkSelect.executeQuery("SELECT link_id FROM article_ids_2;");
				int scannedArticleNumber = 0;
				Set<Integer> seenTuplesSet = new HashSet<Integer>();
				Set<JoinResult> results = new HashSet<JoinResult>();
				while (results.size() < 1000) {
					if (scannedLinksNumber % 100000 == 0) {
						System.out.println("Scanned " + scannedLinksNumber + " links");
					}
					articleSelectResult.next();
					scannedArticleNumber++;
					int articleId = articleSelectResult.getInt(1);
					seenTuplesSet.add(articleId);
					linkSelectResult.next();
					scannedLinksNumber++;
					int linkId = linkSelectResult.getInt(1);
					if (seenTuplesSet.contains(linkId)) {
						results.add(new JoinResult(linkId, linkId));
					}
				}
				System.out.println(results);
				System.out.println("read articles and links: " + scannedArticleNumber + ", " + scannedLinksNumber);
			}
		}
	}

}
