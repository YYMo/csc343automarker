import java.sql.*;

public class Assignment2 {
  
  private static final String CSC343DBNAME = "a2";
  
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
	try { 
		Class.forName("org.postgresql.Driver");
	} catch (Exception e) {
		//Fatal error: could not load postgres driver
	}
	connection = null;
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	boolean success;

	try {
		connection = DriverManager.getConnection(URL, username, password);
		success = true;
	} catch (SQLException e) {
		success = false;
	}

	return success;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	boolean success;

	if (connection == null) {
		success = false;
	} else {
		try {
			connection.close();
			success = true;
		} catch (SQLException e) {
			success = false;
		}
	}

	return success;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	boolean success;
	PreparedStatement ps;
	ResultSet rs;
	int res;
	String s1 = "select cid from " + CSC343DBNAME + ".country where cid = ?";
	String s2 = "insert into " + CSC343DBNAME +".country (cid, cname, height, population) "
			+ "values (?, ?, ?, ?)";

	if (connection == null) {
		success = false;
	} else {
		try {
			ps = connection.prepareStatement(s1);
			ps.setInt(1, cid);

			rs = ps.executeQuery();

			if (rs.next()) {
				//country with given cid already exists
				success = false;
				rs.close();
				ps.close();
			} else {
				rs.close();
				ps.close();

				ps = connection.prepareStatement(s2);
				ps.setInt(1, cid);
				ps.setString(2, name);
				ps.setInt(3, height);
				ps.setInt(4, population);

				res = ps.executeUpdate();

				if (res == 1) {
					success = true;
				} else {
					success = false;
				}

				ps.close();
			}

			success = true;
		} catch (SQLException e) {
			success = false;
		}
	}

	return success;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	int num;
	PreparedStatement ps;
	ResultSet rs;
	String s1 = "select cid from "+CSC343DBNAME+".oceanAccess where oid = ?";

	if (connection == null) {
		num = -1;
	} else {
		try {
			num = 0;

			ps = connection.prepareStatement(s1);
			ps.setInt(1, oid);

			rs = ps.executeQuery();

			while(rs.next()) {
				num++;
			}

			rs.close();
			ps.close();

		} catch (SQLException e) {
			num = -1;
		}
	}

	return num;
  }
   
  public String getOceanInfo(int oid){
	String oceanInfo;
	PreparedStatement ps;
	ResultSet rs;
	String s1 = "select * from "+CSC343DBNAME+".ocean where oid = ?";

	if(connection == null) {
		oceanInfo = "";
	} else {
		try {
		
			oceanInfo = "";

			ps = connection.prepareStatement(s1);
			ps.setInt(1, oid);

			rs = ps.executeQuery();

			if(rs.next()) {
				oceanInfo = rs.getInt("oid")
					+ ":"
					+ rs.getString("oname")
					+ ":"
					+ rs.getInt("depth");
			} else {
				oceanInfo = "";
			}

			rs.close();
			ps.close();
		} catch (SQLException e) {
			oceanInfo = "";
		}
	}

	return oceanInfo;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	boolean success;
	int res;
	PreparedStatement ps;
	String s1 = "update "+CSC343DBNAME+".hdi set hdi_score = ? where cid = ? and year = ?";

	if (connection == null) {
		success = false;
	} else {
		try {
			ps = connection.prepareStatement(s1);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);

			res = ps.executeUpdate();
			
			if (res == 1) {
				success = true;
			} else {
				success = false;
			}

			ps.close();

		} catch (SQLException e) {
			success = false;
		}
	}

	return success;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	boolean success;
	PreparedStatement ps;
	String s1 = "delete from "+CSC343DBNAME+".neighbour where country = ? and neighbor = ?";
	int res;

	if (connection == null) {
		success = false;
	} else {
		try {
			res = 0;
			connection.setAutoCommit(false);

			ps = connection.prepareStatement(s1);
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);

			res += ps.executeUpdate();
			ps.close();

			ps = connection.prepareStatement(s1);
			ps.setInt(1, c2id);
			ps.setInt(2, c1id);

			res += ps.executeUpdate();
			ps.close();


			if (res == 2 ) {
				connection.commit();
				success = true;
			} else {
				connection.rollback();
				success = false;
			}

		} catch (SQLException e) {
			success = false;
			try {
				connection.rollback();
			} catch (SQLException e2) {
				//can't really do anything about this
			}
		} finally {
			try {
				connection.setAutoCommit(true);
			} catch (SQLException e) {
				//can't really do anything
			}
		}
	}

	return success;
  }
  
  public String listCountryLanguages(int cid){
	String lcl;
	PreparedStatement ps;
	ResultSet rs;
	String s1 = "select l.lid lid, c.cname lname, CAST((c.population * l.lpercentage) as int) population "
			+ " from "+CSC343DBNAME+".language l "
			+ " inner join "+CSC343DBNAME+".country c on c.cid = l.cid "
			+ " where c.cid = ?";

	if (connection == null) {
		lcl = "";
	} else {
		try {
			ps = connection.prepareStatement(s1);
			ps.setInt(1, cid);

			rs = ps.executeQuery();

			if (rs.next()) {
				lcl = rs.getInt("lid")
					+ ":"
					+ rs.getString("lname").trim()
					+ ":"
					+ rs.getInt("population");

				while(rs.next()) {
					lcl += "#"
						+ rs.getInt("lid")
						+ ":"
						+ rs.getString("lname").trim()
						+ ":"
						+ rs.getInt("population");
				}

			} else {
				lcl = "";
			}

			rs.close();
			ps.close();

		} catch (SQLException e) {
			lcl = "";
		} 
	}
	return lcl;
  }
  
  public boolean updateHeight(int cid, int decrH){
	boolean success = false;
	PreparedStatement ps;
	String s1 = "update "+CSC343DBNAME+".country set height = (height - ?) where cid = ?";
	int res;

	if(connection == null || decrH < 0) {
		success = false;
	} else {
		try {
			ps = connection.prepareStatement(s1);
			ps.setInt(1, decrH);
			ps.setInt(2, cid);

			res = ps.executeUpdate();

			if(res == 1) {
				success = true;
			} else {
				success = false;
			}

			ps.close();

		} catch (SQLException e) {
			success = false;
		}
	}

	return success;
  }
  
  public boolean updateDB(){
	boolean success;
	PreparedStatement ps;
	String s1 = "drop table if exists "+CSC343DBNAME+".mostPopulousCountries";
	String s2 = "create table "+CSC343DBNAME+".mostPopulousCountries ( "
			+ " cid INTEGER, cname VARCHAR(20) )";
	String s3 = "insert into "+CSC343DBNAME+".mostPopulousCountries "
			+ " (select cid, cname from "+CSC343DBNAME+".country "
			+ " where population > 100000000) ";

	if (connection == null) {
		success = false;
	} else {
		try {
			connection.setAutoCommit(false);

			ps = connection.prepareStatement(s1);
			ps.executeUpdate();
			ps.close();

			ps = connection.prepareStatement(s2);
			ps.executeUpdate();
			ps.close();

			ps = connection.prepareStatement(s3);
			ps.executeUpdate();
			ps.close();

			connection.commit();

			success = true;

		} catch (SQLException e) {
			success = false;
			try {
				connection.rollback();
			} catch (SQLException e2) {
				//can't do anything about this
			}
		} finally {
			try {
				connection.setAutoCommit(true);
			} catch (SQLException e) {
				//can't do anything about this
			}
		}
	}

	return success;
  }
  
}
