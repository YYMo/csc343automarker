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
      }
      catch (ClassNotFoundException e) {
          //System.out.println("Failed to find the JDBC driver");
          //e.printStackTrace();
          return;
      }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
          connection = DriverManager.getConnection(URL,username,password);
          if(connection != null){
              return true;
          }else{
              return false;
          }
      }
      catch (SQLException e){
          return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
          connection.close();
          return true;
      }catch(Exception e){
          return false;
      }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      try{
          sql=connection.createStatement();
          String sqlText = "INSERT INTO country (cid, cname, height, population) VALUES (?,?,?,?)";
          ps = connection.prepareStatement(sqlText);
          ps.setInt(1, cid);
          ps.setString(2, name);
          ps.setInt(3, height);
          ps.setInt(4, population);
          
          ps.executeUpdate();

          return true;
      }catch(Exception e){
          return false;
      }

  }
  
  public int getCountriesNextToOceanCount(int oid) {
      //SELECT COUNT(DISTINCT cid) FROM oceanAccess WHERE oceanAccess.oid=oid;
      try{
          sql=connection.createStatement();
          String sqlText = "SELECT COUNT(DISTINCT cid) AS num FROM oceanAccess WHERE oceanAccess.oid="+String.valueOf(oid)+";";
          rs = sql.executeQuery(sqlText);
          if (rs != null){
              while(rs.next()){
                  return rs.getInt("num");
              }
          }
          return -1;
      }catch(Exception e){
          return -1;
      }
  }
   
  public String getOceanInfo(int oid){
      //SELECT * FROM ocean WHERE ocean.oid=oid;
      try{
          sql=connection.createStatement();
          String sqlText = "SELECT * FROM ocean WHERE ocean.oid="+String.valueOf(oid)+";";
          rs = sql.executeQuery(sqlText);
          if (rs != null){
              while(rs.next()){
                  return rs.getInt("oid")+":"+rs.getString("oname")+":"+rs.getInt("depth");
              }
          }
          return "";
      }catch(Exception e){
          return "";
      }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      //SELECT * FROM hdi WHERE hdi.cid=6 AND hdi.year=2011;
      //UPDATE hdi SET hdi_score=0.697 WHERE cid=6 AND year=2011;
      try{
          sql=connection.createStatement();
          String sqlText = "UPDATE hdi SET hdi_score="+String.valueOf(newHDI)+" WHERE cid="+String.valueOf(cid)+" AND year="+String.valueOf(year)+";";
          sql.executeUpdate(sqlText);
          return true;
      }catch(Exception e){
          return false;
      }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      //DELETE FROM neighbour WHERE country=c1id AND neighbor=c2id;
      try{
          sql=connection.createStatement();
          String sqlText1 = "DELETE FROM neighbour WHERE country="+String.valueOf(c1id)+" AND neighbor="+String.valueOf(c2id)+";";
          String sqlText2 = "DELETE FROM neighbour WHERE country="+String.valueOf(c2id)+" AND neighbor="+String.valueOf(c1id)+";";
          sql.executeUpdate(sqlText1);
          sql.executeUpdate(sqlText2);
          return true;
      }catch(Exception e){
          return false;
      }
  }
  
  public String listCountryLanguages(int cid){
      //SELECT l.lid AS lid, c.cname AS lname, c.population*l.lpercentage AS population FROM country c,language l WHERE c.cid=l.cid ORDER BY c.cid;
      try{
          sql=connection.createStatement();
          String sqlText = "SELECT l.lid AS lid, c.cname AS lname, c.population*l.lpercentage AS population FROM country c,language l WHERE c.cid="+String.valueOf(cid)+" AND l.cid="+String.valueOf(cid)+" ORDER BY c.cid;";
          rs = sql.executeQuery(sqlText);
          String resultText="";
          if (rs != null){
              while(rs.next()){
                  resultText=resultText+rs.getInt("lid")+":"+rs.getString("lname")+":"+rs.getInt("population")+"#";
              }
              //remove the last "#" from resultText
              if (resultText.length() > 0) {
                  resultText = resultText.substring(0, resultText.length()-1);
              }
          }
          return resultText;
      }catch(Exception e){
          return "";
      }
  }
  
  public boolean updateHeight(int cid, int decrH){
      //SELECT * FROM country WHERE country.cid=cid;
      try{
          sql=connection.createStatement();
          String sqlText1 = "SELECT * FROM country WHERE country.cid="+String.valueOf(cid)+";";
          rs = sql.executeQuery(sqlText1);
          int oldHeight=0;
          if (rs != null){
              while(rs.next()){
                  oldHeight=rs.getInt("height");
              }
          }
          String sqlText2 = "UPDATE country SET height="+String.valueOf(oldHeight)+"-"+String.valueOf(decrH)+" WHERE cid="+String.valueOf(cid)+";";
          sql.executeUpdate(sqlText2);
          return true;
      }catch(Exception e){
          return false;
      }
  }
    
  public boolean updateDB(){
      //SELECT cid, cname FROM country WHERE population>100 ORDER BY cid ASC;
      //CREATE TABLE mostPopulousCountries(cid INTEGER,cname VARCHAR(20));
      //INSERT INTO mostPopulousCountries VALUES(?,?);
      try{
          String sqlText1 = "CREATE TABLE IF NOT EXISTS mostPopulousCountries(cid INTEGER,cname VARCHAR(20));";
          String sqlText2 = "SELECT cid, cname FROM country WHERE population>100 ORDER BY cid ASC;";
          String sqlText3 = "INSERT INTO mostPopulousCountries VALUES(?,?);";
          ps = connection.prepareStatement(sqlText1);
          ps.executeUpdate();
          ps = connection.prepareStatement(sqlText3);
          
          rs = sql.executeQuery(sqlText2);
          if (rs != null){
              while(rs.next()){
                  ps.setInt(1,rs.getInt("cid"));
                  ps.setString(2,rs.getString("cname"));
              }
          }
          ps.executeUpdate();
          ps.close();
          
          return true;
      }catch(Exception e){
          return false;
      }
  }
  
}
