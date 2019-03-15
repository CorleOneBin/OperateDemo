import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.spi.DirStateFactory.Result;

public class Test {
	private static String dirverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) {
		try {
			Class.forName(dirverName);
			Connection con = DriverManager.getConnection("jdbc:hive2://node3:10000/default",
					"root", "");
			Statement stm = con.createStatement();
			String sql = "select * from psn1";
			ResultSet rs = stm.executeQuery(sql);
			while(rs.next()) {
				System.out.println(rs.getInt(1)+"-"+rs.getString(2));
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
