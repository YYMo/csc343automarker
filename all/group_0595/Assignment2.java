import java.sql.*;

public class Assignment2 {

    // A connection to the database
    Connection connection;

    // Statement to run queries
    Statement sql;

    // Prepared Statement
    PreparedStatement ps;

    // Resultset for the query
    ResultSet rs;

    // CONSTRUCTOR
    Assignment2() {
	try {
	    Class.forName("org.postgresql.Driver");
	} catch (ClassNotFoundException e) {
	    // do nothing \
	}
    }

    // Using the input parameters, establish a connection to be used for this
    // session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password) {
	try {
	    connection = DriverManager
		    .getConnection("jdbc:postgresql://localhos"
			    + "t:5432/csc343h-" + username, username, password);
	} catch (SQLException e) {
	    return false;
	}
	if (connection != null) {
	    return true;
	} else {
	    return false;
	}
    }

    // Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB() {
	try {
	    connection.close();
	    return true;
	} catch (Exception e) {
	    return false;
	}
	return false;
    }

    public boolean insertCountry(int cid, String name, int height,
	    int population) {
	try {
	    String queryString = "select * from country where cid = " + cid;
	    ps = connection.prepareStatement(queryString);
	    rs = ps.executeQuery();
	    rs.next();
	    if (rs != null) {
		return false;
	    }
	    rs.close();
	    ps.close();
	    sql = connection.createStatement();
	    String sqlText = "INSERT INTO country VALUES (" + cid + ", '" + name
		    + "', " + height + ", " + population + ")";
	    sql.executeUpdate(sqlText);
	    sql.close();
	    return true;
	} catch {
	    return false;
	}
	return false;
    }

    public int getCountriesNextToOceanCount(int oid) {
	try {
	    String queryString = "select count(oid) as result from oceanAccess group by oid having oid = "
		    + oid;
	    ps = connection.prepareStatement(queryString);
	    rs = ps.executeQuery();
	    rs.next();
	    int resultInt = rs.getInt("result");
	    rs.close();
	    ps.close();
	    if (resultInt != null) {
		return resultInt;
	    } else {
		return -1;
	    }
	} catch (Exception e) {
	    return -1;
	}
	return -1;
    }

    public String getOceanInfo(int oid) {
	try {
	    String queryString = "select * from ocean where oid = " + oid;
	    ps = connection.prepareStatement(queryString);
	    rs = ps.executeQuery();
	    rs.next();
	    String resultString = "";
	    resultString = rs.getInt("oid") + ":";
	    resultString = resultString + rs.getString("oname") + ":";
	    resultString = resultString + rs.getInt("depth");
	    rs.close();
	    ps.close();
	    return resultString;
	} catch {
	    return "";
	}
	return "";
    }

    public boolean chgHDI(int cid, int year, float newHDI) {
	try {
	    //sql = connection.createStatement(); 
	    String queryString = "update hdi set hdi_score = " + newHDI + ", year = " + year + " where cid = " + cid;
	    Statement stmt = connection.createStatement();
	    stmt.executeUpdate(queryString);
	    stmt.close();
	    return true;
	} catch {
	    return false;
	}
	return false;
    }

    public boolean deleteNeighbour(int c1id, int c2id) {
	try {
	    String queryString = "delete from neighbour where country = "
		    + c1id + " and neighbor = " + c2id;
	    String queryString2 = "delete from neighbour where country = "
		    + c2id + " and neighbor = " + c1id;
	    Statement stmt = connection.createStatement();
	    stmt.executeUpdate(queryString);
	    stmt.close();
	    Statement stmt2 = connection.createStatement();
	    stmt2.executeUpdate(queryString2);
	    stmt2.close();
	    return true;
	} catch (Exception e) {
	    return false;
	}
	return false;
    }

    public String listCountryLanguages(int cid) {
	try {
	    String queryString = "select * from country where cid = " + cid;
	    ps = connection.prepareStatement(queryString);
	    rs = ps.executeQuery();
	    rs.next();
	    int pop = rs.getInt("population");
	    rs.close();
	    String queryString2 = " select * from language where cid = " + cid;
	    PreparedStatement pStatement = connection
		    .prepareStatement(queryString2);
	    rs = pStatement.executeQuery();
	    String output = "";
	    float actualpop;
	    if (rs != null) {
		while (rs.next()) {
		    output = output + rs.getInt("lid") + ":";
		    output = output + rs.getString("lname") + ":";
		    actualpop = pop * .01 * rs.getFloat("lpercentage");
		    output = output + actualpop + "#";
		}
	    }
	    rs.close();
	    return output;
	} catch (Exception e) {
	    return "";
	}
	return "";
    }

    public boolean updateHeight(int cid, int decrH) {
	try {
	    String queryString = "select height from country where cid = "
		    + cid;
	    PreparedStatement pStatement = connection
		    .prepareStatement(queryString);
	    rs = pStatement.executeQuery();
	    rs.next();
	    int height = rs.getInt("height");
	    height = height - decrH;
	    rs.close();
	    pStatement.close();
	    String queryString2 = "update country set height = " + height
		    + " where cid = " + cid;
	    Statement stmt = connection.createStatement();
	    stmt.executeUpdate(queryString2);
	    stmt.close();
	    return true;
	} catch (Exception e) {
	    return false;
	}
	return false;
    }

    public boolean updateDB() {
	try {
	    String queryString1 = "drop table mostPopulousCountries";
	    Statement stmt1 = connection.createStatement();
	    stmt.executeUpdate(queryString);
	    stmt1.close();    
	} catch {
	}

	try {
	    String queryString = "select cid, cname into mostPopulousCountries from (select * from country where population >= 100000000)a order by cid asc";
	    Statement stmt = connection.createStatement();
	    stmt.executeUpdate(queryString);
	    stmt.close();
	    return true;
	} catch (Exception e) {
	    return false;
	}
	return false;
    }
}
