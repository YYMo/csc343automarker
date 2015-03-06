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
           Class.forName("org.postgresql.Driver");
          } catch (ClassNotFoundException e) {
                  System.out.println("Where is your PostgreSQL JDBC Driver?");
          }
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){               try
      {
              connection = DriverManager.getConnection(URL, username, password);
      }
      catch (SQLException se)
      {
              return false;
      }
      return true;
  }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
          try {

                connection.close();
          }
          catch (SQLException se) {
                  return false;
          }
          return true;
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
          try {
                  sql = connection.createStatement();
                  sql.executeUpdate("SET search_path to a2");
                  ps  = connection.prepareStatement("INSERT INTO country "+
                          "VALUES ("+cid+", '"+name+"', "+height+", "+population+")");
                  ps.executeUpdate();
                  ps.close();
          } catch (SQLException se) {
                  return false;
          }
          return true;
  }

  public int getCountriesNextToOceanCount(int oid) {
          int answer = 0;
          try{
                  sql = connection.createStatement();
                  sql.executeUpdate("SET search_path to a2");
                  ps = connection.prepareStatement("SELECT count(cid) " +
                          "FROM oceanAccess "+
                          "WHERE oid="+oid);
                  rs = ps.executeQuery();
                  while (rs.next()){
                          answer = rs.getInt(1);
                  }
                  ps.close();
                  rs.close();
          } catch (SQLException se) {
                  return -1;
          }
          return answer;
  }

  public String getOceanInfo(int oid){
          String answer="";
          try {
                  StringBuilder sb = new StringBuilder();
                  sql = connection.createStatement();
                  sql.executeUpdate("SET search_path to a2");
                  ps = connection.prepareStatement("SELECT * " +
                          "FROM ocean "+
                          "WHERE oid=" + oid);
                  rs = ps.executeQuery();
                  while (rs.next()){
                          sb.append(rs.getInt(1));
                          sb.append(":");
                          sb.append(rs.getString(2));
                          sb.append(":");
                          sb.append(rs.getInt(3));
                  }
                  answer = sb.toString();

                  ps.close();
                  rs.close();

          } catch (SQLException se) {

          }
          return answer;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
          try{
                  sql = connection.createStatement();
                  sql.executeUpdate("SET search_path to a2");
                  ps = connection.prepareStatement("UPDATE hdi "+
                                  "SET hdi_score = " + newHDI +
                                  " WHERE cid = "+cid+" AND year = "+year);
                  if (ps.executeUpdate()==0)
                          return false;
                  ps.close();
          } catch (SQLException se) {
                  return false;
          }
          return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
          try{
                  sql = connection.createStatement();
                  sql.executeUpdate("SET search_path to a2");
                  ps = connection.prepareStatement(
                                  "DELETE FROM neighbour "+
                                  "WHERE (country = " + c1id + " AND "+
                                  "neighbor = " + c2id + ") OR (country = "
                                  + c2id + " AND neighbor = " + c1id + ")" );
                  if(ps.executeUpdate() == 0)
                          return false;
                  ps.close();
          } catch (SQLException se) {
                  return false;
          }
          return true;
  }

  public String listCountryLanguages(int cid){
          String answer = "";
          try {
                  int countryPop = 0;
                  StringBuilder sb = new StringBuilder();
                  sql = connection.createStatement();
                  sql.executeUpdate("SET search_path to a2");
                  ps = connection.prepareStatement(
                                  "SELECT population "+
                                  "FROM country "+
                                  "WHERE cid = " + cid);
                  rs = ps.executeQuery();
                  while(rs.next()){
                          countryPop = rs.getInt(1);
                 }

                  ps = connection.prepareStatement(
                                  "SELECT lid, lname, " + countryPop + " * lpercentage as population "+
                                  "FROM language " +
                                  "WHERE cid = " + cid +
                                  " ORDER BY population"
                                  );
                  rs = ps.executeQuery();
                  while (rs.next()){
                          sb.append(rs.getInt(1));
                          sb.append(":");
                          sb.append(rs.getString(2));
                          sb.append(":");
                          sb.append(rs.getFloat(3));
                          sb.append("#");
                  }

                  answer = sb.toString();
                  if (!answer.isEmpty()){
                          answer = answer.substring(0, answer.length()-1);
                  }
                  ps.close();
                  rs.close();

          } catch (SQLException se) {
          }
          return answer;
  }

  public boolean updateHeight(int cid, int decrH){
          try {
                  sql = connection.createStatement();
                  sql.executeUpdate("SET search_path to a2");
                  ps = connection.prepareStatement(
                                  "UPDATE country " +
                                  "SET height = height - " + decrH +
                                  " WHERE cid = "+cid);
                  if (ps.executeUpdate()==0)
                          return false;
                  ps.close();
          } catch (SQLException se) {
                  return false;
          }
          return true;
  }

  public boolean updateDB(){
          try {
                  sql = connection.createStatement();
                  sql.executeUpdate("SET search_path to a2");
                  ps = connection.prepareStatement(
                                  "DROP TABLE IF EXISTS mostPopulousCountries; " +
                                  "CREATE TABLE mostPopulousCountries(" +
                                  "cid     INTEGER     PRIMARY KEY, " +
                                  "cname     VARCHAR(20)); " +
                                  "INSERT INTO mostPopulousCountries (" +
                                  "SELECT cid, cname "+
                                  "FROM country "+
                                  "WHERE population > 100000000 "+
                                  "ORDER BY cid ASC)"
                                  );
                  ps.executeUpdate();
                  ps.close();
          } catch (SQLException se) {
                  return false;
          }
          return true;
  }
  
}
