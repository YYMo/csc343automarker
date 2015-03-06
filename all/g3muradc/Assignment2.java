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

  //CONSTRUCTOR
  Assignment2(){
        try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
		}
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if conne$
  public boolean connectDB(String URL, String username, String password){
        try {
                connection = DriverManager.getConnection(URL, username, password);
                return true;
        } catch (SQLException e) {
                return false;
        }
  }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
        try {
                connection.close();
                return true;
        } catch (SQLException e) {
                return false;
        }
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
      try { 
              ps = connection.prepareStatement("INSERT INTO a2.country VALUES (?, ?, ?, ?)");

              ps.setInt(1, cid);
              ps.setString(2, name);
              ps.setInt(3, height);
              ps.setInt(4, population);
              int updated = ps.executeUpdate();

              ps.close();
              if (updated == 0) {
                      return false;
              }
              return true;
      } catch (SQLException e) {
	      return false;
      }
}
  
  public int getCountriesNextToOceanCount(int oid) {
      try {
              ps = connection.prepareStatement("SELECT COUNT(cid) from a2.oceanAccess WHERE ? = oid");

              ps.setInt(1, oid);
              rs = ps.executeQuery();
              int count = 0;

              while (rs.next()) {
                      count =  rs.getInt(1);
              }

              rs.close();
              ps.close();
              return count;
      } catch (SQLException e) {
              return -1;
      }
}

public String getOceanInfo(int oid){
      try {
              ps = connection.prepareStatement("SELECT * from a2.ocean WHERE ? = oid");
              ps.setInt(1, oid);
              rs = ps.executeQuery();
	      rs.next();
              String result = Integer.toString(rs.getInt(1)) + ":" + rs.getString(2) + ":" + Integer.toString(rs.getInt(3));
              rs.close();
              ps.close();
              return result;

      } catch (SQLException e) {
              return "";
      }
}

public boolean chgHDI(int cid, int year, float newHDI){
    try {
            ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?");
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            int updated = ps.executeUpdate();
            if (updated == 0) {
                    return false;
            }
            ps.close();
            return true;

    } catch (SQLException e) {
            return false;
    }
}
    
    public boolean deleteNeighbour(int c1id, int c2id){
        try {
                // remove the first neighbouring tuple
                ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE ? = country AND ? = neighbor");
                ps.setInt(1, c1id);
                ps.setInt(2, c2id);
                int updated = ps.executeUpdate();
                if (updated == 0) {
                        return false;
                }
                ps.close();

                // remove the reverse neighbouring tuple
                ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE ? = country AND ? = neighbor");
                ps.setInt(1, c2id);
                ps.setInt(2, c1id); 
                updated = ps.executeUpdate();
                if (updated == 0) {
                        return false;
                }
                ps.close(); 
                return true;

        } catch (SQLException e) {
                return false;
        }
  }

    public String listCountryLanguages(int cid){
        try {
                ps = connection.prepareStatement("SELECT lid, lname, lpercentage * population AS amountspeaking FROM a2.language, a2.country WHERE a2.language.cid = ? AND a2.language.cid = a2.country.cid ORDER BY amountspeaking");
                ps.setInt(1, cid);
                rs = ps.executeQuery();
                String result = "";
		rs.next();
		result = result + rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3);
                while (rs.next()) {
                        result = result + "#" + rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3);
                }
                rs.close();
                ps.close();
                return result;

        } catch (SQLException e) {
                return "";
        }
  }
    
    public boolean updateHeight(int cid, int decrH){
        try {
                ps = connection.prepareStatement("UPDATE a2.country SET height = height - ? WHERE cid = ?");
                ps.setInt(1, decrH);
                ps.setInt(2, cid);
                int updated = ps.executeUpdate();
                if (updated == 0) {
                        ps.close();
                        return false;
                }
                ps.close();
                return true;
        } catch (SQLException e) {
                return false;
        }
  }

  public boolean updateDB(){
        try {
                ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries( cid INTEGER, cname VARCHAR(20))");
                int updated = ps.executeUpdate();

                ps.close();
                ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population > 1000000 ORDER BY cid ASC)");
                updated = ps.executeUpdate();

                return true;
        } catch (SQLException e) {
		e.printStackTrace();
                return false;
        }
  }

}







