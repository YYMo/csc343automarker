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
      try{
        // Load JDBC driver
        Class.forName("org.postgresql.Driver");
      }
      catch(ClassNotFoundException e){
        
      }
    }
    
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
      try 
      {
        connection = DriverManager.getConnection(URL, username, password);
        String path = "SET search_path TO A2";
        sql = connection.createStatement();
        sql.executeUpdate(path);           
        sql.close();

      }
      catch (Exception e) {
      }
      
      return (connection != null);
    
    }
    
    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
      
      boolean isConnClosed = false;
      boolean isStatementClosed = false;
      boolean isPsClosed = false;
      boolean isResultSetClosed = false;
      
      try{
        if(rs != null) {
          rs.close();
          isResultSetClosed = rs.isClosed();
        }
        
        if(ps != null){
          ps.close();
          isPsClosed = ps.isClosed();
        }
        
        if(sql != null){
          sql.close();
          isStatementClosed = sql.isClosed();
        }
        
        if(connection != null){
          connection.close();
          isConnClosed = connection.isClosed();
        }
        

      }
      catch(SQLException e){
        return false;  
      }
      
        return (isResultSetClosed && isPsClosed && isStatementClosed && isConnClosed);
      
    }
      
    public boolean insertCountry (int cid, String name, int height, int population) { 
      
      if(connection!=null){
        boolean isPsClosed = false;
        int psReturn = 0;
        
        try{
          String query = "INSERT INTO country(cid, cname, height, population) VALUES (?, ?, ?, ?)";
          ps = connection.prepareStatement(query);
          ps.setInt(1, cid);
          ps.setString(2, name);
          ps.setInt(3, height);
          ps.setInt(4, population);
          psReturn = ps.executeUpdate();
          ps.close();
          isPsClosed = ps.isClosed();
              
          
        } catch(SQLException e) {
          return false;
        }
        
        return (isPsClosed && (psReturn==1));
      }
      return false;
      
    }
    
    
    public int getCountriesNextToOceanCount(int oid) {
      
      if(connection!=null) {
        boolean isPsClosed = false;
        boolean rsClosed = false;
        int count = 0;
        
        try{
          String statement = "SELECT COUNT(cid) AS number FROM oceanAccess WHERE oid=?";
          ps = connection.prepareStatement(statement);
          ps.setInt(1, oid);
          rs = ps.executeQuery();     
          if(rs.next()) {
            count = rs.getInt(1);
          }
          if(rs!=null) {
            rs.close();
          }
          rsClosed = rs.isClosed();
          
          ps.close();
          isPsClosed = ps.isClosed();           
          
        }
        catch(SQLException e) {
        }
        
        if(rsClosed && isPsClosed){
          return count;
        } else{
          return -1;
        }
        
      }
      return -1;
      
    }
     
    public String getOceanInfo(int oid){
      
      if( connection!=null ) 
      {
        boolean isPsClosed = false;
        boolean rsClosed = false;
        String oceanInfo;
        
        try
        {
          String query = "SELECT * FROM ocean WHERE oid=?";
          ps = connection.prepareStatement(query);
          ps.setInt(1, oid);
          rs = ps.executeQuery();     
          if(rs.next()){
            oceanInfo = rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3);
          }
          else 
          {
            oceanInfo = "";
          }
          
          if(rs != null){
            rs.close();
          }
          rsClosed = rs.isClosed();
          
          ps.close();
          isPsClosed = ps.isClosed();
          
          return oceanInfo;
        }
        catch(SQLException e){
          
        }
      }
      return "";
    }

    public boolean chgHDI(int cid, int year, float newHDI){
      
      if(connection!=null){
        boolean isPsClosed = false;
        int returnVal = 0;
        
        try{
          String query = "UPDATE hdi SET hdi_score=? WHERE cid=? AND year=?";
          ps = connection.prepareStatement(query);
          ps.setFloat(1, newHDI);
          ps.setInt(2, cid);
          ps.setInt(3, year);
          returnVal = ps.executeUpdate();            
          ps.close();     
          isPsClosed = ps.isClosed();
          
        } catch(SQLException e) {
          return false;
        }
        
        return (isPsClosed && (returnVal==1));
      }
      return false;
   
    }

    public boolean deleteNeighbour(int c1id, int c2id){
      
      if(connection != null) {
        boolean isPsClosed = false;
        int psReturn = 0;
        
        try 
        {
          String query = "DELETE FROM neighbour WHERE country=? AND neighbor=?";
          ps = connection.prepareStatement(query);
          ps.setInt(1, c1id);
          ps.setInt(2, c2id);     
          psReturn += ps.executeUpdate();
          
          ps.setInt(1, c2id);
          ps.setInt(2, c1id);     
          psReturn += ps.executeUpdate();     
          
          ps.close();     
          isPsClosed = ps.isClosed();
         
        }
        catch(SQLException e){
          return false;  
        }
        
        return (isPsClosed && (psReturn==2)); 
      }
      return false;
      
    }
    
    public String listCountryLanguages(int cid){
      
      if(connection != null){
        boolean isPsClosed = false;
        boolean rsClosed = false;
        String queryString = "";
        
        try{
          String query = "SELECT * FROM language WHERE cid=? ORDER BY lpercentage DESC";
          ps = connection.prepareStatement(query);
          ps.setInt(1, cid);
          rs = ps.executeQuery();     
          int i=1;
          while(rs.next()) {
            queryString += "|" + i + rs.getInt(2) + ":|" + i + rs.getString(3) + ":|" + i + rs.getDouble(4) + "#";
          }
          
          if(rs!=null){
            rs.close();
          }
          rsClosed = rs.isClosed();
          
          ps.close();
          isPsClosed = ps.isClosed();
          
          return queryString;
          
        }catch(SQLException e){
          
        }
      }
      return "";

    }
    
    public boolean updateHeight(int cid, int decrH)
    {
      if(connection!=null) {
        boolean isPsClosed = false;
        int psReturn = 0;
        
        try{
          String query = "UPDATE country SET height=? WHERE cid=?";
          ps = connection.prepareStatement(query);
          ps.setInt(1, decrH);
          ps.setInt(2, cid);      
          psReturn = ps.executeUpdate();            
          ps.close();     
          isPsClosed = ps.isClosed();
         
        }
        catch(SQLException e) {
          return false;
        }
        
        return (isPsClosed && (psReturn==1));
      }
      return false;
      
    }
      
    public boolean updateDB()
    {
      
      if(connection != null) {
        boolean isSqlClosed = false;
        int sqlReturn = 0;
        
        try
        {
          String query = "CREATE TABLE mostPopulousCountries AS (SELECT cid,cname FROM country WHERE population>100000000 ORDER BY cid ASC)";
          sql = connection.createStatement();
          sqlReturn = sql.executeUpdate(query);           
          
          sql.close();      
          isSqlClosed = sql.isClosed();        
          
        }
        catch(SQLException e){
          return false;
        }
        
        return (isSqlClosed && (sqlReturn==0)); 
      }
      return false;
      
    }
  
}