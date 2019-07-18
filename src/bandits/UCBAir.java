package bandits;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import bandits.DatabaseConnection;

public class UCBAir {

	public static void main(String[] args) throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection()) {
			try (Statement articleSelect = dc.getConnection().createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
					Statement linkSelect = dc.getConnection().createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
							java.sql.ResultSet.CONCUR_READ_ONLY);
					Statement joinStatement = dc.getConnection().createStatement()) {
				articleSelect.setFetchSize(Integer.MIN_VALUE);
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet articleSelectResult = articleSelect.executeQuery("SELECT id FROM tbl_article_wiki13;");
				ResultSet linkSelectResult = linkSelect.executeQuery("SELECT id FROM tbl_link_09;");
				// topk join over R and S
				int joinCount = 0;
				int k = 0; // parameter k from UCB-AIR algorithm
				int n = 0; // parameter n from UCB-AIR algorithm
				double beta = 1; // parameter beta from UCB-AIR algorithm
				int reward = 0;
				Map<Integer, Double> idValue = new HashMap<Integer, Double>();
				while (joinCount < 10) {
					n++;
					// get next unseen tuple r from R and update value of k
					if (k < Math.pow(n, (beta / beta + 1.0))) {
						int articleId = -1;
						while (articleSelectResult.next()) {
							articleId = articleSelectResult.getInt("id");
							if (!idValue.containsKey(articleId)) {
								k++;
								break;
							}
						}
						if (articleId == -1) {
							System.err.println("Article table reached its end!");
							return;
						}
						// join r with random tuple s from S
						if (linkSelectResult.next()) {
							int linkId = linkSelectResult.getInt("id");
							String joinSql = "select article_id, link_id from tbl_article_wiki13 a, tbl_article_link_09 al, "
									+ "tbl_link_09 l where a.id = al.article_id and al.link_id = l.id and a.id = "
									+ articleId + "l.id = " + linkId + ";";
							ResultSet joinResult = joinStatement.executeQuery(joinSql);
							// TODO should we drop the indexes on tables?
							reward = joinResult.getFetchSize();
							updateUCBValues(idValue, articleId, reward, n);
						} else {
							System.err.println("Link table reached its end!");
							return;
						}
					} else {
						// do UCB-V
					}
				}
			}

		}
	}

	static void updateUCBValues(Map<Integer, Double> idValue, int id, int reward, int n) {

	}

}
