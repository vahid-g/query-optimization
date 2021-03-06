package bandits;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import database.DatabaseManager;

public class UCB {

	static class ArmInfo {
		int armPullCount = 0;
		int reward = 0;

		double getValue(int totalArmPulls) {
			if (armPullCount == 0)
				return Integer.MAX_VALUE;
			else
				return reward + Math.sqrt(2 * Math.log(totalArmPulls) / armPullCount);
		}
	}

	static int TBL_ARTICLE_SIZE = 2666491;
	static int TBL_ARTICLE_LINK_SIZE = 120916125;

	public static void main(String[] args) {
		experimentOverFile();
	}

	public static void experimentOverFile() {
		List<Integer> joinResults = new ArrayList<Integer>();
		try (Scanner sc = new Scanner(new FileInputStream("/data/wikipedia_article_ids_rand.csv"))) {
			ArmInfo[] arms = new ArmInfo[TBL_ARTICLE_SIZE];
			for (int i = 0; i < arms.length; i++) {
				arms[i] = new ArmInfo();
			}
			System.out.println("init completed");
			int totalScannedArticleLinkTuples = 0;
			while (sc.hasNext() && joinResults.size() < 3) {
				if (totalScannedArticleLinkTuples % 10000 == 0) {
					System.out.println("processed " + totalScannedArticleLinkTuples + " links");
				}
				totalScannedArticleLinkTuples++;
				int joiningArticleId = Integer.parseInt(sc.next());
				// arms[joiningArticleId].reward++;
				// This can not be done because we do not the position (arm index) of the
				// corresponding
				// joining article, we could do this if the arms were associated with the
				// attribute values
				int bestArmIndex = pickBestArmIndex(arms, totalScannedArticleLinkTuples);
				arms[bestArmIndex].armPullCount++;
				if (bestArmIndex == joiningArticleId) {
					joinResults.add(bestArmIndex);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void experimentOverDatabase() {
		List<Integer> joinResults = new ArrayList<Integer>();
		try (Connection conn = DatabaseManager.createConnection()) {
			try (Statement linkSelect = conn.createStatement()) {
				String linkSelectSql = "select article_id from tbl_article_link_09";
				ArmInfo[] arms = new ArmInfo[TBL_ARTICLE_SIZE];
				for (int i = 0; i < arms.length; i++) {
					arms[i] = new ArmInfo();
				}
				linkSelect.setFetchSize(Integer.MIN_VALUE);
				ResultSet linkResultSet = linkSelect.executeQuery(linkSelectSql);
				System.out.println("init completed");
				int totalScannedArticleLinkTuples = 0;
				while (linkResultSet.next() && joinResults.size() < 3) {
					if (totalScannedArticleLinkTuples % 10000 == 0) {
						System.out.println("processed " + totalScannedArticleLinkTuples + " links");
					}
					totalScannedArticleLinkTuples++;
					int joiningArticleId = linkResultSet.getInt(1);
					// arms[joiningArticleId].reward++;
					// This can not be done because we do not the position (arm index) of the
					// corresponding
					// joining article, we could do this if the arms were associated with the
					// attribute values
					int bestArmIndex = pickBestArmIndex(arms, totalScannedArticleLinkTuples);
					arms[bestArmIndex].armPullCount++;
					if (bestArmIndex == joiningArticleId) {
						joinResults.add(bestArmIndex);
						// TODO actual join
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("joins size: " + joinResults.size());
	}

	static int pickBestArmIndex(ArmInfo[] arms, int totalArmPulls) {
		int bestValue = -1;
		int bestArm = -1;
		for (int i = 0; i < arms.length; i++) {
			ArmInfo armInfo = arms[i];
			if (armInfo.getValue(totalArmPulls) > bestValue) {
				bestArm = i;
			}
		}
		return bestArm;
	}

	static int pickBestArmIndexGreedy(ArmInfo[] arms) {
		double epsilon = 0.1;
		if (new Random().nextDouble() < epsilon) {
			return new Random().nextInt(TBL_ARTICLE_SIZE);
		} else {
			int bestValue = -1;
			int bestArm = -1;
			for (int i = 0; i < arms.length; i++) {
				ArmInfo armInfo = arms[i];
				if (armInfo.reward > bestValue) {
					bestArm = i;
				}
			}
			return bestArm;
		}

	}

}
