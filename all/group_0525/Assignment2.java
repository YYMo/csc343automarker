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
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if c$
  public boolean connectDB(String URL, String username, String password){
      try {
          connection = DriverManager.getConnection(URL, username, password);
      } catch (Exception e) {
          return false;
      }
     System.out.println("connected");
      if (connection != null) {
          return true;
      } else {
          return false;
      }
  }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
        connection.close();
        return true;
      } catch (Exception e) {
        return false;
      }
  }
 

  public boolean insertCountry (int cid, String name, int height, int population) {
     try {
         sql = connection.createStatement();

         String checkDup = "select count(cid) from a2.country where cid = " + String.valueOf(cid);

         rs  = sql.executeQuery(checkDup);
         rs.next();
         int numDup = rs.getInt(1);

         if (numDup == 0) { // No duplicate
             String queryString = "INSERT INTO a2.country " +
                                "VALUES (?, ?, ?, ?)";
             ps = connection.prepareStatement(queryString);
             ps.setInt(1, cid);
             ps.setString(2, name);
             ps.setInt(3, height);
             ps.setInt(4, population);

             ps.executeUpdate();
             ps.close();
             rs.close();
             return true;
         }
         rs.close();
         return false;
     } catch (SQLException e) {
         System.out.println("Error");
         return false;
     }
  }

  public int getCountriesNextToOceanCount(int oid) {
    try {
        sql = connection.createStatement();
        String sqlText;
        sqlText = "select count(cid) from a2.oceanAccess where oid = " + String.valueOf(oid);

        rs = sql.executeQuery(sqlText);
        rs.next();
        int result = rs.getInt(1);
        rs.close();
        return result;
    } catch (SQLException e) {
        return -1;
    }
  }

  public String getOceanInfo(int oid){
    try{
        sql = connection.createStatement();

        String sqlText;
        sqlText = "select oid, oname, depth from a2.ocean where oid = " + String.valueOf(oid);

        rs = sql.executeQuery(sqlText);
        if (rs.next()) {
            String strOid = String.valueOf(rs.getInt("oid"));
            String strOname = rs.getString("oname");
            String strDepth = rs.getString("depth");
            rs.close();
            return strOid + ":" + strOname + ":" + strDepth;
        } else {
            rs.close();
            return "";
        }
    } catch (SQLException e) {
        return "";
    }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try{
        sql = connection.createStatement();

        String HDIText;
        HDIText = "update a2.hdi set hdi_score = " + String.valueOf(newHDI) + " where cid = " + String.valueOf(cid)
                + " and year = " + String.valueOf(year);

        sql.executeUpdate(HDIText);
        return true;
    } catch (SQLException e){
        return false;
    }
  }


  public boolean deleteNeighbour(int c1id, int c2id){
    try{
        sql = connection.createStatement();
        String DLText;
        DLText = "delete from a2.neighbour where (country = " + String.valueOf(c1id) + 
                "and neighbor = " + String.valueOf(c2id) + ") or (country = " +
                String.valueOf(c2id) + "and neighbor = " + String.valueOf(c1id) + ")";

        sql.executeUpdate(DLText);
        return true;
    } catch (SQLException e){
        return false;
    }
  }
  public String listCountryLanguages(int cid){
     try{
        sql = connection.createStatement();

        String allLangs = "select lid, lname, (lpercentage  *  population) as lpop " +  
                        "from a2.language natural join a2.country where cid = "
                         + String.valueOf(cid) + "order by lpop";

        rs = sql.executeQuery(allLangs);

        String cidLangs =  "";
        if (rs != null){

            while (rs.next()){
                cidLangs += "l" + rs.getString("lid") + ":l" +
                            rs.getString("lname") + ":l" + rs.getString("lpop") + "#";
            }
        }
         rs.close();
        return cidLangs.substring(0, cidLangs.length()-1);
     } catch (SQLException e){
        return "";
     }
  }

  public boolean updateHeight(int cid, int decrH){
      try{
          sql = connection.createStatement();

          String getHeight;
          getHeight = "select height from a2.country where " + 
                        " cid = " + String.valueOf(cid);
          rs = sql.executeQuery(getHeight);
          if (rs.next()) {
              int height = rs.getInt(1); 
              String heightText;

              heightText = "update a2.country set height = " + 
                        String.valueOf(height - decrH)
                  + " where cid = " + String.valueOf(cid);

              sql.executeUpdate(heightText);
              rs.close();
              return true;
            } else {
                rs.close();
                return false;
            }
       } catch (SQLException e){
          return false;
       }
  }
 
  public boolean updateDB(){
        try{
            sql = connection.createStatement();

            String updateText;

            updateText = "CREATE TABLE a2.mostPopulousCountries " +
                        "(cid INTEGER, " +
                        "cname VARCHAR(20))";

            sql.executeUpdate(updateText);

            System.out.println("Error??");
            updateText = "INSERT INTO a2.mostPopulousCountries " +
                        "SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC";

            sql.executeUpdate(updateText);
            return true;
        } catch (SQLException e) {
            return false;
        }
  }

}

