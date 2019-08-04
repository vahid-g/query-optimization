package bandits;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class UBC {

	static int ARTICLE_NUMBERS = 2666491;
	public static void main(String[] args) {

		try (Connection conn = DatabaseManager.createConnection()) {
			try (Statement articleSelect = conn.createStatement();
					Statement linkSelect = conn.createStatement()) {
				String articleSelectSql = "select id from tbl_article_09;";
				String linkSelectSql = "select article_id from tbl_article_link_09";
				int[] ubcValues = new int[ARTICLE_NUMBERS];
				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
