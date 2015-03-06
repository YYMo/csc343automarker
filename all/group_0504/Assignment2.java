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

    /**
     * CONSTRUCTOR
     */
    Assignment2(){
        try {
            // Load JDBC driver
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
    }
    
    /**
     * Helper function to initialize the SQL statement
     * @return 
     */
    private boolean initSQLStatement() {
        try {
            sql = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Using the input parameters, establish a connection to be used for this session
     * @param URL
     * @param username
     * @param password
     * @return true if connection is successful
     */
    public boolean connectDB(String URL, String username, String password){
        try {
            // Make connection to database
            connection = DriverManager.getConnection(URL, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        if(connection != null) {
            if (initSQLStatement())
                return true;
        }
        
        return false;        
    }

    /**
     * Closes the connection
     * @return true if closure was successful
     */
    public boolean disconnectDB(){
        // DROP tables if necessary
        try {
	    sql.executeUpdate("DROP TABLE IF EXISTS a2.mostPopulousCountries");
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        return false;
    }
    
    /**
     * Inserts a row into the country table.
     * @param cid is the id of the country
     * @param name is the name of the country
     * @param height is the highest elevation point
     * @param population is the population of the country
     * @return whether insertion was successful or not
     */
    public boolean insertCountry (int cid, String name, int height, int population) {
        try {
            // Add new country to database
            String sqlText = "INSERT INTO a2.country VALUES (" + String.valueOf(cid)
                                                    + ", '" + name + "', " 
                                                    + String.valueOf(height) + ", "
                                                    + String.valueOf(population) + ")";
            
            int rows = sql.executeUpdate(sqlText);           
            
            // Check that one row was updated
            if(rows!=1)
                return false;
            
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * This function finds the number of countries in table "oceanAccess" that 
     * are located next to the ocean with id oid. Returns -1 if an error occurs.
     * @param oid is the id of the ocean
     * @return number of oceans or -1
     */
    public int getCountriesNextToOceanCount(int oid) {
        int countries = -1;
        
        try {
            String sqlText = "SELECT COUNT(cid) as countries FROM a2.oceanAccess WHERE oid=" + String.valueOf(oid);
            
            rs = sql.executeQuery(sqlText);
            
            if(rs.next()) {
                countries = rs.getInt("countries");
            }
            
            rs.close();
            
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;  
        }
        
        return countries;
    }

    /**
     * Gets the information of an ocean with id oid. The output is "oid:oname:depth". 
     * @param oid
     * @return String of contain ocean info or an empty string "" if the ocean does not exist.
     */
    public String getOceanInfo(int oid){
        String oceanInfo = "";
        
        try {
            String sqlText = "SELECT * FROM a2.ocean WHERE oid=" + String.valueOf(oid);
            rs = sql.executeQuery(sqlText);
            
	    // Parse result set
            if(rs.next()) {
                    String s_oid = String.valueOf(rs.getInt(1)).trim();
                    String oname = rs.getString(2).trim();
                    String depth = String.valueOf(rs.getInt(3)).trim();
                    
                    oceanInfo = s_oid + ":" + oname + ":" + depth;
            }            
            rs.close();
            
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        }
        
        return oceanInfo;
    }

    /**
     * Changes the HDI value of the country (cid) for the year (year) to the HDI value supplied (newHDI). 
     * @param cid
     * @param year
     * @param newHDI
     * @return true if the change was successful, false otherwise.
     */
    public boolean chgHDI(int cid, int year, float newHDI){
        try {
            String sqlText = "UPDATE a2.hdi SET hdi_score = " + String.valueOf(newHDI)
                    + " WHERE cid=" + String.valueOf(cid) + " AND year=" + String.valueOf(year);
            
            int rows = sql.executeUpdate(sqlText);      
            
            if(rows!=1)
                return false;
            
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        
        return true;
    }
    
    /**
     * Deletes the neighboring relation between two countries.  
     * @param c1id
     * @param c2id
     * @return true if the deletion was successful, false otherwise.
     */
    public boolean deleteNeighbour(int c1id, int c2id){
        try {
            // Delete c2id from the neighbours of c1id 
            String sqlText = "DELETE FROM a2.neighbour WHERE country =" + String.valueOf(c1id)
                                + " AND neighbor=" + String.valueOf(c2id);
            int result = sql.executeUpdate(sqlText);
            
            if(result != 1)
                return false;
            
            // Delete c1id from the neighbours of c2id 
            sqlText = "DELETE FROM a2.neighbour WHERE country =" + String.valueOf(c2id) 
                        + " AND neighbor=" + String.valueOf(c1id);
            result = sql.executeUpdate(sqlText);
            
            if(result != 1)
                return false;
        } catch (SQLException e){
            e.printStackTrace();
            return false;
        }

        return true; 
    }

    /**
     * Finds all the languages that are spoken in the country with id cid. 
     * @param cid
     * @return string all the languages that are spoken in the country or empty string "" if the country does not exist.
     */
    public String listCountryLanguages(int cid){
        
        String langs = ""; //String of languages 
      
        try {
            String sqlText = "SELECT L.lid, L.lname,"
                    + " (L.lpercentage*C.population) AS numpeople"
                    + " FROM a2.language L JOIN a2.country C ON L.cid = C.cid"
                    + " WHERE L.cid=" + String.valueOf(cid)
                    + " ORDER BY numpeople";

            // rs will retrive the values from the query
            rs = sql.executeQuery(sqlText);

            // Loop through each row in rs
            if (rs != null){
                while (rs.next()){
                    langs = langs + rs.getInt(1) +":"
                            + rs.getString(2).trim() +":"
                            + rs.getLong(3) +"#";
                }
            }
            int n = langs.length();
            langs = langs.substring(0, n-1);
            rs.close();
            
        } catch (SQLException e){
            e.printStackTrace();
            return "";
        }

        return langs;
    }

    /**
     * Decreases the height of the country with id cid.
     * @param cid
     * @param decrH
     * @return true if the update was successful, false otherwise.
     */
    public boolean updateHeight(int cid, int decrH){
        try {
            String sqlText = "UPDATE a2.country SET height= (height-"
                            +String.valueOf(decrH)+") WHERE cid="
                            +String.valueOf(cid);

            sql.executeUpdate(sqlText);
            int updates = sql.getUpdateCount();

            if (updates != -1){
                return true;
            }

        } catch (SQLException e){
            e.printStackTrace();
            return false;
        }  

        return false;
    }

    /**
     * Create a table containing all the countries which have a population over 100 million. The name of the table should be mostPopulousCountries and the attributes should be:
     * cid INTEGER (country id)
     * cname VARCHAR(20) (country name)
     * @return true if the database was successfully updated, false otherwise
     */
    public boolean updateDB(){
        String sqlText = "CREATE TABLE a2.mostPopulousCountries"
                        + " (cid INTEGER, cname VARCHAR(20))";

        String sqlTextInsert = "INSERT INTO a2.mostPopulousCountries VALUES (?,?)";

        String columns = "SELECT cid, cname "
                        + "FROM a2.country "
                        + "WHERE population > 100000000 ORDER BY cid ASC";

      try {
          //If table already exists drop it
          sql.execute("DROP TABLE IF EXISTS a2.mostPopulousCountries");

          //Create table
          sql.execute(sqlText);
          
          //Set values from country into rs
          rs = sql.executeQuery(columns);
          
          //Set ps: will be used to insert values into new table 
          ps = connection.prepareStatement(sqlTextInsert);

          //Loop through rs rows and insert values into new table 
          if (rs != null){
              while (rs.next()){
                  ps.setInt(1, rs.getInt(1));
                  ps.setString(2, rs.getString(2));
                  ps.executeUpdate();
              }
              
          } else {
              return false;
          }
          ps.close();
          rs.close();
          return true;  
          
      } catch (SQLException e){
          e.printStackTrace();
          return false;
      
      }     
    }
    
}


