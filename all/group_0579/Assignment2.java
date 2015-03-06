import java.sql.*;

//TODO COUNT THE NUM OF MODIFIED ROWS

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
      //Load JDBC driver
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      //System.out.println("Driver not found");
      return;
    }
  }

  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){

      try {
        connection = DriverManager.getConnection(URL, username, password);
        sql = connection.createStatement();
      }
      catch (SQLException e) {
        return false;
      }
      //System.out.println("Connected!");
      return (connection != null);
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        if (sql != null && !sql.isClosed()) sql.close();
        if (rs != null && !rs.isClosed()) rs.close();
        if (ps != null && !ps.isClosed()) ps.close();
        return (connection == null || connection.isClosed());
    }
    catch (SQLException e) {
        //e.printStackTrace();
        return false;
    }
  }

    
  public boolean insertCountry (int cid, String name, int height, int population) { 
    try {
        //System.out.println("in insertCountry");
        String sqlQuery = "SELECT cid FROM a2.country WHERE cid = " +
                    cid + ";";
        rs = sql.executeQuery(sqlQuery);

        if (!rs.next()) {
            String sqlText = "INSERT INTO a2.country VALUES (" + 
                       cid + ", '" +
                       name + "', " +
                       height + ", " +
                       population + ")";
		
            ps = connection.prepareStatement(sqlText);
            //System.out.println(sqlText);
            int numRows = ps.executeUpdate();
	    //numRows = sql.executeUpdate(sqlText);
            //System.out.println(numRows);
            ps.close();
	    rs.close();
            if(numRows == 1) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
    catch (SQLException e) {
      //e.printStackTrace();
      return false;
    }
  
  }

  
  public int getCountriesNextToOceanCount(int oid) {
    try {

      String sqlText = "SELECT count(cid) as c " + 
                "FROM a2.oceanAccess " +
                "WHERE oid = " + oid + 
                " GROUP BY oid;";
      rs = sql.executeQuery(sqlText);

      if (!rs.next()) {
        rs.close();
        return 0;
      }
      else {  //else there are countries by ocean so count them
	int count = rs.getInt("c");        
	rs.close();
	return count;
      }
    }
    catch (SQLException e) {
      //e.printStackTrace();
      return -1;
    }
  }
   

  public String getOceanInfo(int oid) {
    try {
      String sqlText = "SELECT * " + 
                "FROM a2.ocean " +
                "WHERE oid = " + oid + ";";
      rs = sql.executeQuery(sqlText);

      if (rs.next()) {
          int oceanoid = rs.getInt("oid");
          String oceanname = rs.getString("oname");
          int oceandepth = rs.getInt("depth");
	  rs.close();
          return Integer.toString(oceanoid) + ":" + oceanname + ":" + Integer.toString(oceandepth);
      }
      else {
        rs.close();
        return "";
      }
    }
    catch (SQLException e) {
      //e.printStackTrace();
      return "";
    }
  }


  public boolean chgHDI(int cid, int year, float newHDI){
    try {
      String sqlText = "UPDATE a2.hdi " + 
                "SET hdi_score = " + newHDI + 
                " WHERE cid = " + cid + " AND year = " + year + ";";
      int numRows = sql.executeUpdate(sqlText);
      if(numRows == 1) {
	return true;
      } else {
	return false;
      }
    }
    catch (SQLException e) {
      //e.printStackTrace();
      return false;
    }
  }


  public boolean deleteNeighbour(int c1id, int c2id){
    try {
      
      String sqlText = "DELETE FROM a2.neighbour " +
                "WHERE country= " + c1id + " AND neighbor= " + c2id;
      int numRows = sql.executeUpdate(sqlText);
      if(numRows >= 1) {
         String sqlText2 = "DELETE FROM a2.neighbour " +
                "WHERE country= " + c2id + " AND neighbor= " + c1id;
         int scndNumRows = sql.executeUpdate(sqlText2);
	    if(scndNumRows >= 1) {
		return true;
	    }   
      }
      return false;   
    }
    catch (SQLException e) {
       //e.printStackTrace();
       return false;
    }
    
  }
  

  public String listCountryLanguages(int cid) {
    try {
      String sqlText = 
              "SELECT lid, lname, (population * lpercentage)/100 AS population " +
              "FROM a2.language natural join a2.country " +
              "WHERE country.cid = " + cid + 
              " ORDER BY population;";
      rs = sql.executeQuery(sqlText);

      String answer = "";
      int i = 1;

      if (rs != null) {
          while (rs.next()) {
            answer += Integer.toString(rs.getInt("lid")) + ":" +
                      rs.getString("lname") + ":" +
                      Integer.toString(rs.getInt("population")) + "#"; 
          }
      }
      else {
          answer = "";
      }
      rs.close();
      return answer;       
    }

    catch (SQLException e) {
      //e.printStackTrace();
      return "";
    }
    
  }
  
  public boolean updateHeight(int cid, int decrH) {
    try {
      String sqlText = "UPDATE a2.country " +
                "SET height = " + "height - " + Integer.toString(decrH) + 
                " WHERE cid = " + Integer.toString(cid);
      int numRows = sql.executeUpdate(sqlText);
      if(numRows >= 1) {
         return true;
      } else {
         return false;
      }
    }
    catch (SQLException e) {
      //e.printStackTrace();
      return false;
    }
  }
    

  public boolean updateDB(){
    try {
      String sqlText = "CREATE TABLE a2.mostPopulousCountries (" +
                "cid INTEGER ," +
                " cname VARCHAR(20) , PRIMARY KEY (cid))";
      sql.executeUpdate(sqlText);  
      String sqlQuery;
      sqlQuery = "INSERT INTO a2.mostPopulousCountries (" +
                  "SELECT cid, cname FROM a2.country WHERE population > 100000000) " +
		  " ORDER BY population";
      int numRows = sql.executeUpdate(sqlQuery);
      if(numRows >= 1) {
         return true;
      } else {
         return false;
      }
    }
    catch (SQLException e) {
      //e.printStackTrace();
      return false;  
    }

  }
}


