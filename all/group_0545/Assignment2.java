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
  		e.printStackTrace();
  	}
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try{
      connection = DriverManager.getConnection(URL, username, password);
      sql = connection.createStatement();
    }
    catch(SQLException e){
      return false;
    }
    if(connection !=null){
      return true;
    }
    else{
      return false;
    }
  }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try{
      connection.close();
    }
    catch(SQLException e){
      return false;
    }
    return true;
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
    try {
      String query = "INSERT INTO country VALUES(" + new Integer(cid).toString() + ", '" + name + "', " +  new Integer(height).toString() +", " + new Integer(population).toString() +")";
      sql.executeUpdate(query);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }

  public int getCountriesNextToOceanCount(int oid) {
	try {
    String query = "SELECT COUNT(cid) FROM oceanAccess WHERE oceanAccess.oid=" + new Integer(oid).toString();
		rs = sql.executeQuery(query);
		if(rs != null){
			rs.next();
			return rs.getInt(1);
		}
	} catch (SQLException e) {
		return -1;
	}
	return -1;
  }

  public String getOceanInfo(int oid){
		try {
      String query = "SELECT oname, depth FROM ocean WHERE ocean.oid=" + new Integer(oid).toString();
			rs = sql.executeQuery(query);
			if(rs != null){
				rs.next();
				return new Integer(oid).toString()+":" +rs.getString(1)+ ":" + rs.getString(2);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return "";
		}
		return "";
	  }
  

  public boolean chgHDI(int cid, int year, float newHDI){
		try {
			String query = "UPDATE hdi SET hdi_score =" + new Double(newHDI).toString() +"WHERE hdi.cid=" + new Integer(cid).toString()
					  +"AND hdi.year="+ new Integer(year).toString();
			sql.executeUpdate(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}
   return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
			String query = "DELETE FROM neighbour WHERE country=" + new Integer(c1id).toString() + "AND neighbor=" + new Integer(c2id).toString();
			sql.executeUpdate(query);
			query = "DELETE FROM neighbour WHERE country=" + new Integer(c2id).toString() + "AND neighbor=" + new Integer(c1id).toString();
			sql.executeUpdate(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}
	  return true;
  }

  public String listCountryLanguages(int cid){
    try {
      String query = "SELECT L.lid, L.lname, L.lpercentage*C.population FROM language L, country C WHERE L.cid=C.cid AND L.cid=" + new Integer(cid).toString();
      rs = sql.executeQuery(query);

      String result = "";
      boolean first = true;

      if (rs != null) {
        while (rs.next()) {
          if (first) {
            first = false;
          } else {
            result += "#";
          }

          result += rs.getString(1) + ":" + rs.getString(2) + ":" + new Integer(rs.getInt(3)).toString();
        }

        return result;
      }      
    } catch (SQLException e) {

    }

    return "";
  }

  public boolean updateHeight(int cid, int decrH){
    try {
      String query = "SELECT height FROM country WHERE cid=" + new Integer(cid).toString();
      rs = sql.executeQuery(query);

      if (rs != null) {
        rs.next();
        int height = rs.getInt(1);

        query = "UPDATE country SET height=" + new Integer(height - decrH).toString() + "WHERE cid=" + new Integer(cid).toString();
        sql.executeUpdate(query);

        return true;
      }
    } catch (SQLException e) {
      
    }

    return false;
  }

  public boolean updateDB(){
	 try {
    String query = "CREATE TABLE mostPopulousCountries (cid INTEGER, cname VARCHAR(20))";
    sql.executeUpdate(query);

    query = "INSERT INTO mostPopulousCountries SELECT cid, cname FROM country WHERE population>100000000 ORDER BY cid";
    sql.executeUpdate(query);

    return true;
   } catch (SQLException e) {

   }

   return false;
  }

}
