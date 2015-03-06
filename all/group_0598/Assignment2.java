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
        catch (ClassNotFoundException e) {}
    }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
          connection = DriverManager.getConnection(URL, username, password);
      } catch (Exception e) {
          return false;
      }

      if (connection != null){
          return true;
      } else {
          return false;
      }
  }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
          if(connection != null){
              connection.close();
              return true;
          }
      } catch (Exception e) {}
      return false;
  }

  public boolean insertCountry(int cid, String name, int height, int population){
      String sqlText;

      try{
          sql = connection.createStatement();
          rs = null;

          sqlText = "SELECT * FROM a2.country where cid = ?";
          ps = connection.prepareStatement(sqlText);
          ps.setInt(1, cid);

          rs = ps.executeQuery();
          //check if cid already exist
          if(rs.next()) {
              return false;
          }
          rs.close();

          sqlText = "INSERT INTO a2.country "+
                    "VALUES (?, ?, ?, ?)";
          ps = connection.prepareStatement(sqlText);
          ps.setInt(1, cid);
          ps.setString(2, name);
          ps.setInt(3, height);
          ps.setInt(4, population);
          ps.executeUpdate();
          ps.close();
          return true;
      } catch (SQLException e){}
      return false;

  }

  public int getCountriesNextToOceanCount(int oid) {
      String sqlText;
      int number;

      try{
          sql = connection.createStatement();
          rs = null;

          //check oid existence
          sqlText = "SELECT * " +
                    "FROM a2.oceanaccess " +
                    "WHERE oid = ?";
          ps = connection.prepareStatement(sqlText);
          ps.setInt(1, oid);
          rs = ps.executeQuery();
          if(rs.next() == false){
              return -1;
          }
          rs.close();

          sqlText = "SELECT count(*) as count " +
                    "FROM a2.oceanaccess " +
                    "WHERE oid = ?";
          ps = connection.prepareStatement(sqlText);
          ps.setInt(1, oid);
          rs = ps.executeQuery();
          rs.next();
          number = rs.getInt("count");
          rs.close();
          ps.close();
          return number;
      } catch(SQLException e) {}
      return -1;
  }

  public String getOceanInfo(int oid){
      String sqlText = "SELECT * " +
                       "FROM a2.ocean " + 
                       "WHERE oid = ?";
      String oname;
      String result;

      try{
          sql = connection.createStatement();
          rs = null;

          ps = connection.prepareStatement(sqlText);
          ps.setInt(1, oid);
          rs = ps.executeQuery();
          //handle when oid not exist
          if (rs.next() == false){
              return "";
          }
          result = String.format("%d:%s:%d",
                                rs.getInt("oid"),
                                rs.getString("oname"),
                                rs.getInt("depth"));
          rs.close();
          ps.close();
          return result;
      } catch(SQLException e) {}
      return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      String sqlText = "UPDATE a2.hdi " +
                      "SET hdi_score = ? " +
                      "WHERE cid = ? and year =?";

      try{
          sql = connection.createStatement();
          ps = connection.prepareStatement(sqlText);
          ps.setFloat(1, newHDI);
          ps.setInt(2, cid);
          ps.setInt(3, year);
          if (ps.executeUpdate() == 0){
              ps.close();
              return false;
          } else {
              ps.close();
              return true;
          }
      } catch(SQLException e) {}
      return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      String sqlText = "DELETE FROM a2.neighbour " + 
                      "WHERE(country = ? and neighbor = ?) " +
                      "or (country = ? and neighbor = ?) ";

      try{
          sql = connection.createStatement();
          ps = connection.prepareStatement(sqlText);
          ps.setInt(1, c1id);
          ps.setInt(2, c2id);
          ps.setInt(3, c2id);
          ps.setInt(4, c1id);
          if(ps.executeUpdate() == 0 ){
              ps.close();
              return false;
          } else {
              ps.close();
              return true;
          }
      } catch(SQLException e) {}
      return false;
  }

  public String listCountryLanguages(int cid){
      String sqlText = "SELECT lid, lname, lpercentage*population AS population " +
                       "FROM a2.country c, a2.language l "                              +
                       "WHERE c.cid=l.cid AND c.cid=? "                           +
                       "ORDER BY population ";
      String result = "";
      int count = 1;

      try{
          sql = connection.createStatement();
          rs = null;
          ps = connection.prepareStatement(sqlText);
          ps.setInt(1,cid);
          rs = ps.executeQuery();
          if(rs.next() == false){
            ps.close();
            rs.close();
            return "";
          } else{
            do{
                if (count == 1){
                    result += String.format("%d:%s:%d",
                                          rs.getInt("lid"),
                                          rs.getString("lname"),
                                          rs.getInt("population"));
                  count++;
                } else {
                    result += String.format("#%d:%s:%d",
                                           rs.getInt("lid"),
                                           rs.getString("lname"),
                                           rs.getInt("population"));
                }
                }while(rs.next());
                rs.close();
                ps.close();
                return result;
            }
      }catch(SQLException e){}
      return "";
  }

  public boolean updateHeight(int cid, int decrH){
      String sqlText = "UPDATE a2.country " +
                       "SET height = ? " +
                       "WHERE cid = ? ";
      String sqlText1 = "SELECT height FROM a2.country " +
                        "WHERE cid = ?";
      int height;

      try{
        sql = connection.createStatement();
        rs = null;

        ps = connection.prepareStatement(sqlText1);
        ps.setInt(1,cid);
        rs = ps.executeQuery();
        if (rs.next() == false){
          rs.close();
          ps.close();
          return false;
        }

        height = rs.getInt("height");
        height -= decrH;
        ps=connection.prepareStatement(sqlText);
        ps.setInt(1,height);
        ps.setInt(2,cid);
        if (ps.executeUpdate() == 0){
          ps.close();
          return false;
        }else{
          ps.close();
          return true;
        }
      }catch(SQLException e){}
      return false;
  }

  public boolean updateDB(){
      String sqlText = "CREATE TABLE IF NOT EXISTs a2.mostPopulousCountries( " +
                       "cid INTEGER, " +
                       "cname VARCHAR(20));" +
                       "INSERT INTO a2.mostPopulousCountries( "+
                       "SELECT cid,cname "+
                       "FROM a2.country " +
                       "WHERE population > 100000000" +
                       "ORDER BY cid ASC);";
      try{
          sql = connection.createStatement();
          ps=connection.prepareStatement(sqlText);
          ps.executeUpdate();
          return true;
      }catch(SQLException e){}
      return false;
  }
}
