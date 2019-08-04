package bandits;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class UBC {

	static class ArmInfo {
		int armPullCount = 0;
		int reward = 0;

		double getValue(int totalArmPulls) {
			return reward + Math.sqrt(2 * Math.log(totalArmPulls) / armPullCount);
		}
	}

	static int totalArmPulls = 0;

	static int ARTICLE_NUMBERS = 2666491;

	public static void main(String[] args) {
		// TODO check index size really
		List<Integer> joinResults = new ArrayList<Integer>();
		try (Connection conn = DatabaseManager.createConnection()) {
			try (Statement linkSelect = conn.createStatement()) {
				String linkSelectSql = "select article_id from tbl_article_link_09";
				ArmInfo[] arms = new ArmInfo[ARTICLE_NUMBERS];
				for (int i = 0; i < arms.length; i++) {
					arms[i] = new ArmInfo();
				}
				ResultSet linkResultSet = linkSelect.executeQuery(linkSelectSql);
				while (linkResultSet.next()) {
					if (totalArmPulls % 10000 == 0) {
						System.out.println("processed " + totalArmPulls + " arms");
					}
					totalArmPulls++;
					int joiningArticleId = linkResultSet.getInt(1);
					arms[joiningArticleId].reward++;
					int bestArmIndex = pickBestArmIndexGreedy(arms);
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
	}

	static int pickBestArmIndex(ArmInfo[] arms) {
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
			return new Random().nextInt(ARTICLE_NUMBERS);
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
