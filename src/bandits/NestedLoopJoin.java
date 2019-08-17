package bandits;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import database.DatabaseManager;

public class NestedLoopJoin {

	public static void main(String[] args) {
		List<Integer> joinResults = new ArrayList<Integer>();
		int totalScannedArticleLinkTuples = 0;
		try (Scanner sc = new Scanner(new FileInputStream("/data/wikipedia_article_ids.csv"));
				Connection conn = DatabaseManager.createConnection()) {
			String articleSelectSql = "select id from tbl_article_09";
			while (sc.hasNext() && joinResults.size() < 3) {
				if (totalScannedArticleLinkTuples % 10000 == 0) {
					System.out.println("processed " + totalScannedArticleLinkTuples + " links");
				}
				int articleIdFromArticleLinks = sc.nextInt();
				try (Statement articleSelect = conn.createStatement()) {
					articleSelect.setFetchSize(Integer.MIN_VALUE);
					ResultSet articleResultSet = articleSelect.executeQuery(articleSelectSql);
					int scannedArticles = 0;
					while (articleResultSet.next() && joinResults.size() < 3) {
						scannedArticles++;
						int articleId = articleResultSet.getInt(1);
						if (articleId == articleIdFromArticleLinks) {
							joinResults.add(articleId);
						}
					}
					System.out.println("scanned articles: " + scannedArticles);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("joins size: " + joinResults.size());
		System.out.println("scanned links: " + totalScannedArticleLinkTuples);
	}
}
