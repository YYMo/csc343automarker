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
      Class.forName("org.postgresql.Driver") ; 
    } 
    catch (ClassNotFoundException ex ){
      return ;
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try{
      connection = DriverManager.getConnection( "jdbc:postgresql://" + URL,
                                                 username, password);
      return true;
    }
    catch (SQLException ex ){
      return false;
    }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
      connection.close();
      return true;
    }  
    catch (SQLException ex){
      return false;
    }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {

    if (connection == null ) {return false;}

    try {
      String update = "INSERT INTO a2.country " +
                      " VALUES (? , ? , ? , ?) " ;

      ps = connection.prepareStatement(update);
      ps.setInt (1, cid);
      ps.setString (2, name);
      ps.setInt(3, height) ;
      ps.setInt(4, population) ;

      ps.executeUpdate();

      if (ps.getUpdateCount() != 1){
        ps.close() ;
        return false;
      }
      ps.close() ;
      return true;
    }
    catch (SQLException ex) {
      return false;
    }
  }

  public int getCountriesNextToOceanCount(int oid) {

    if (connection == null ) {return -1; }

    try{
      String query = "SELECT count(*) " +
                     "FROM a2.oceanAccess " +
                     "WHERE oid = ? " ;

      ps = connection.prepareStatement(query);

      ps.setInt(1, oid);

      rs = ps.executeQuery();
      

      if (rs != null){
        while (rs.next()){
          return rs.getInt(1) ; 
        }
      } 

      // rs is null
      return -1;
    }
    catch (SQLException ex) {
      return -1;
    }
  }
   
  public String getOceanInfo(int oid){

    if (connection == null ) {return "" ;}

    try{
      String query = "SELECT oid, oname, depth " +
                     "FROM a2.ocean " +
                     "WHERE oid = ? " ;

      ps = connection.prepareStatement(query);
      ps.setInt(1, oid);

      rs = ps.executeQuery();

      String answer = "" ;
 
      if (rs != null){
        while (rs.next()){
          answer = answer + rs.getInt("oid") + ":" + rs.getString("oname") +
                  ":" + rs.getInt("depth");
        }
        ps.close() ;
        rs.close() ;
      }

      return answer;
    }
    catch (SQLException ex){
      return "";
    }
  }

  public boolean chgHDI(int cid, int year, float newHDI){

    if (connection == null ) {return false;}

    try{
      String update = "UPDATE a2.hdi " +
                      "SET hdi_score = ? " +
                      "WHERE (cid = ?) AND (year = ?) " ;

      ps = connection.prepareStatement (update) ;

      ps.setDouble(1, newHDI);
      ps.setInt(2, cid);
      ps.setInt(3, year);

      ps.executeUpdate() ;

      if (ps.getUpdateCount() == 0){
        ps.close() ;
        return false;
      }

      ps.close() ;
      return true;
    }
    catch (SQLException ex ) {
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    if (connection == null ) {return false;}

    try {
      String update = "DELETE FROM a2.neighbour " +
                      "WHERE (country = ? AND neighbor = ?) " +
                      "      OR       " +
                      "      (country = ? AND neighbor = ?) " ;

      ps = connection.prepareStatement(update);

      ps.setInt(1, c1id);
      ps.setInt(2, c2id);
      ps.setInt(3, c2id);
      ps.setInt(4, c1id);

      ps.executeUpdate();

      if (ps.getUpdateCount() < 2){
        ps.close() ;
        return false;
      }
      ps.close();
      return true;
    }
    catch (SQLException ex) {
      return false ;
   }    
  }
  
  public String listCountryLanguages(int cid){
    if (connection == null ) {return "";}

    try{
      String query = 
        "SELECT lid, lname, (population * lpercentage) AS lpopulation " +
        "FROM a2.country c, a2.language l " +
        "WHERE (c.cid = l.cid) AND (c.cid = ?) " +
        "ORDER BY (population * lpercentage) " ; 

      ps = connection.prepareStatement(query);

      ps.setInt(1, cid);

      rs = ps.executeQuery();

      String answer = "" ;

      boolean first = true;
      if (rs != null){
        while (rs.next()){
          if (! first) { 
            //append '#' to siganl the end of a row
             answer = answer + "#" ;
          } 

          answer = answer + Integer.toString(rs.getInt("lid")) + ":" +
                   rs.getString("lname") + ":" + 
                   Double.toString(rs.getDouble("lpopulation")) ;
          first = false;

        }
      }

      ps.close();
      rs.close();
      return answer;
    } 
    catch (SQLException ex ){
      return "" ;
    }
  }
  
  public boolean updateHeight(int cid, int decrH){
    if (connection == null ) {return false;}

    int oldHeight = -1;
    try {
      // 1. get the original height IF it exists
      String getHeight = "SELECT height " +
                         "FROM a2.country " +
                         "WHERE cid = ? " ;

      ps = connection.prepareStatement(getHeight);
      ps.setInt(1, cid);

      rs = ps.executeQuery() ;

      if (rs != null){
        while (rs.next()){
          // the cid exists in the database
          oldHeight = rs.getInt("height");
        }
      }

      if (oldHeight < decrH){
        // if user entered an invalid decrH or if there is no such cid 
        ps.close();
        rs.close();
        return false;
      }

      ps.close();
      rs.close();

      int newHeight = oldHeight - decrH ;

      String update = "UPDATE a2.country " +
                      "SET height = ? " +
                      "WHERE cid = ? " ;

      ps = connection.prepareStatement (update);

      ps.setInt(1, newHeight);
      ps.setInt(2, cid);

      ps.executeUpdate() ;

      if (ps.getUpdateCount() < 1) {
        ps.close();
        return false;
      } 
      ps.close();
      return true;
    }
    catch (SQLException ex ){
      return false;
    }
  }
    
  public boolean updateDB() {
    if (connection == null ) {return false;}

    try {

      sql = connection.createStatement();

      String update = "CREATE TABLE a2.mostPopulousCountries ( " +
                      " cid INTEGER , " +
                      " cname VARCHAR(20) );  " ;
      sql.executeUpdate(update);


      String update2 = "INSERT INTO a2.mostPopulousCountries ( "  +
                       " SELECT cid, cname " +
                       " FROM a2.country " +
                       " WHERE population > 100000000 " +
                       " ORDER BY cid ASC )  ; " ;
      
      sql.executeUpdate(update2);

      if (sql.getUpdateCount() == 0) {
        // the database is NOT update
        sql.close();
        return false ;
      }
      // something is updated
      sql.close();
      return true;
    }
    catch (SQLException ex){
      return false;
    }

  }

}
