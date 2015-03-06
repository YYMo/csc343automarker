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
            this.debugPrint("constructor","PostgreSQL JDBC Driver Registered!");
        } catch ( ClassNotFoundException e ) {
            this.debugPrint("constructor", "Cannot locate PostgreSQL JDBC Driver!");
            return;
        }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    this.debugPrint("connectDB","Attempting an connection");
        try {
            connection = DriverManager.getConnection(URL, username, password);
            return true; 
        } catch ( SQLException e ) {
            this.debugPrint("connectDB","Connection Failed! Check output console");
            return false;
        }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
            connection.close();
            return true;
        } catch ( SQLException e ) {
            this.debugPrint("disconnectDB","Disconnection failed! Check output console");
            return false;
        }   
  }
  
  // Embedded SQL Queries
    
 public boolean insertCountry (int cid, String name, int height, int population) {
   boolean ret = false;

        try {
            sql = connection.createStatement(); 
            
            String sqlString = String.format("SELECT * FROM country WHERE cid = %d", cid);
            
            this.debugPrint("insertCountry", sqlString);
            
            rs = sql.executeQuery(sqlString);
            
            if ( rs != null ) {
                ret = rs.next();
            }

            sql = connection.createStatement();
            
            String sqlString2 = String.format("INSERT INTO country VALUES (%d, '%s', %d, %d)", cid, name, height, population);
            
            this.debugPrint("insertCountry", sqlString2);
            
            sql.executeUpdate(sqlString2);
            
            return true;
        } catch (SQLException e) {
            this.debugPrint("insertCountry", "Exception caught!");
            ret = false;
        }

        return ret;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    int ret = -1;
    
        String count = "count";
        
        try {
            sql = connection.createStatement(); 
            String sqlString = String.format ("SELECT COUNT(*) AS %s FROM oceanAccess WHERE oid = %d", count, oid);

            this.debugPrint("getCountriesNextToOceanCount", sqlString);
            rs = sql.executeQuery(sqlString);
            if ( rs != null ) {
                while ( rs.next() ) {
                    ret = rs.getInt(count);
                }
            }
            rs.close();
        } catch (SQLException e) {
            this.debugPrint("getCountriesNextToOceanCount", "Exception caught!");
            ret = -1;
        }

        return ret;
  }
   
  public String getOceanInfo(int oid){
   String ret = "";
        String field1 = "oname";
        String field2 = "depth";

        try {
            sql = connection.createStatement(); 
            String sqlString = String.format ("SELECT %s, %s FROM ocean WHERE oid = '%s'", field1, field2, oid);

            this.debugPrint("getOceanInfo", sqlString);
            rs = sql.executeQuery(sqlString);
            if ( rs != null ) {
                while ( rs.next() ) {
                    ret = String.format ("%s:%s:%s", oid, rs.getString(field1),rs.getString(field2));
                }
            }
            rs.close();
        } catch (SQLException e) {
            this.debugPrint("getOceanInfo", "Exception caught!");
            ret = "";
        }
        return ret;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
           try {
            sql = connection.createStatement(); 
            String sqlString = String.format ("UPDATE hdi SET hdi_score = %f WHERE cid = %d AND year = %d", newHDI, cid, year);

            this.debugPrint("chgHDI", sqlString);
            sql.executeUpdate(sqlString);
            return true;
        } catch (SQLException e) {
            this.debugPrint("chgHDI", "Exception caught!");
            return false;
        }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try {
            sql = connection.createStatement(); 
            // Select Everything
            
            String sqlString = String.format("DELETE FROM neighbour WHERE country = %d AND neighbor = %d",c1id, c2id);
            this.debugPrint("deleteNeighbour", sqlString);
            sql.executeUpdate(sqlString);

            String sqlString2 = String.format("DELETE FROM neighbour WHERE country = %d AND neighbor = %d", c2id, c1id);
            this.debugPrint("deleteNeighbour", sqlString2);
            sql.executeUpdate(sqlString2);

            return true;
        } catch (SQLException e) {
            this.debugPrint("deleteNeighbour", "Exception caught!");
            return false;
        }     
  }
  
  public String listCountryLanguages(int cid){
    String ret = "";
        try {
            sql = connection.createStatement(); 
            String sqlString = String.format ("SELECT lid, cname, (CL.lpercentage*CO.population/100) AS numofpeople FROM country CO JOIN language CL ON CO.cid = CL.cid WHERE CO.name='%d'", cid);
            this.debugPrint("listWines", sqlString);
            rs = sql.executeQuery(sqlString);
            
            if ( rs != null ) {
                while ( rs.next() ) {
                    ret = String.format ("%d:%s:%d", rs.getInt("lid"), rs.getString("cname"), rs.getInt("numberofpeople"));
                }
            }

            if (ret.length() != 0) {
                ret= ret.substring(0, ret.length()-1);
            }
            rs.close();
        } catch (SQLException e) {
            this.debugPrint("listCountryLanguages", "Exception caught!");
            ret = "";
        }

        return ret;
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
            sql = connection.createStatement();
            
            String sqlString = String.format ("UPDATE country SET height = height - %d WHERE cid = %d", decrH, cid);

            this.debugPrint("updateHeight", sqlString);
            sql.executeUpdate(sqlString);
            return true;

        } catch (SQLException e) {
            this.debugPrint("updateHeight", "Exception caught!");
            return false;
        }
  }
    
  public boolean updateDB(){
	try {
            sql = connection.createStatement();

            String sqlString =    "CREATE TABLE mostPopulousCountries ( " +
                                "    cid INTEGER REFERENCES country(cid) ON DELETE RESTRICT, " +
                                "    cname VARCHAR(20) NOT NULL)";

            this.debugPrint("updateDB", sqlString);
            sql.executeUpdate(sqlString);


            String sqlString2 =   "INSERT INTO mostPopulousCountries ( SELECT cid, cname FROM country WHERE population > 100000000)";

            this.debugPrint("updateDB", sqlString2);
            sql.executeUpdate(sqlString2);
            return true;
            
        } catch (SQLException e) {
            this.debugPrint("updateDB", "Exception caught!");
            return false;    
        }
  }
  /*
  public static void main(String []args){
        System.out.println("Delete Me PLs");
  }*/
  
  public void debugPrint(String funcName, String line)
    {
        String out = String.format ("%s:: Now executing the command: \n%s", funcName, line);
        //System.out.println(out);
    }
  
}
