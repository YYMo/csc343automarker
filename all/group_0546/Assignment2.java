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
  Assignment2()  {
     try {
        Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
        return;
    }
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) {
      boolean result = false;

      try {
            if (URL != null && username != null && password != null)
                connection = DriverManager.getConnection(URL, username, password);
                if (connection !=null) {
                     result = true;
                }
      } catch (SQLException e) {
          result = false;
      }
     return result;
  }

  public boolean disconnectDB()  {
      boolean result = false;

      try {
          if (connection != null) {
              connection.close();
              result = true;
          }
       } catch (SQLException e) {
              result = false;
       }

      return result;
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
      boolean is_existing = false;
      boolean result = false;
      String queryString;

      // Check if cid exist in the table, if not continue inserting the data
      try {
          sql = connection.createStatement();
          ResultSet rs = sql.executeQuery("SELECT cid FROM a2.country");
          while (rs.next()) {
              if (cid == rs.getInt("cid")) {
                  is_existing = true;
                  break;
              }
          }
          if (!is_existing) {
              sql.close();
              rs.close();
          }
          sql.close();
          rs.close();

      } catch (SQLException se) {
          result = false;
      }

      // Inserts new country into the table
      try {
          queryString = "INSERT INTO a2.country(cid, cname, height, population) VALUES (?,?,?,?);";
          ps = connection.prepareStatement(queryString);
          ps.setInt(1,cid);
          ps.setString(2,name);
          ps.setInt(3,height);
          ps.setInt(4,population);
          int is_succesful = ps.executeUpdate();

          if (is_succesful == 1) {
              result = true;
          }

      } catch (SQLException e) {
          result = false;
      }

      finally {
          try {
              if (ps!=null) {
                  ps.close();
              }
          } catch (SQLException se) {
              result = false;
          }
      }
      return result;
  }

  public int getCountriesNextToOceanCount (int oid) {
      int count = 0;

      try {
          ps = connection.prepareStatement("SELECT cid FROM a2.oceanAccess WHERE oid = ?;");
          ps.setInt(1, oid);
          rs = ps.executeQuery();

          if (!rs.isBeforeFirst()){
              count = -1;
          }
          while (rs.next()) {
              count++;
          }
         return count;

      } catch (SQLException se) {
           count = -1;
      }

      finally {
          try {
              if (rs!=null) {
                  rs.close();
              }
          } catch (SQLException se) {
              count = -1;
          }
          try {
              if (ps!=null) {
                  ps.close();
              }
         } catch (SQLException se) {
             count = -1;
         }
      }
      return count;
 }

 public String getOceanInfo(int oid) {
     String oceanInfo = "";
     String oceanName;
     int oceanDepth;

     try {
         ps = connection.prepareStatement("SELECT oid, oname, depth FROM a2.ocean WHERE oid = ?;");
         ps.setInt(1,oid);
         rs = ps.executeQuery();

         while (rs.next()) {
             oceanName = rs.getString("oname").trim();
             oceanDepth = rs.getInt("depth");
             oceanInfo = oid+":"+oceanName+":"+oceanDepth;
         }

      } catch (SQLException se) {
          oceanInfo = "";
      }

      finally {
          try {
             if (rs != null) {
                 rs.close();
             }
           } catch (SQLException se) {
           }
           try {
               if (ps != null) {
                   ps.close();
               }
           } catch (SQLException se) {
           }
      }
     return oceanInfo;
  }

  public boolean chgHDI(int cid, int year, float newHDI) {
      boolean is_successful = false;

      try {
          ps = connection.prepareStatement("SELECT cid, year, hdi_score  FROM a2.hdi WHERE cid = ? AND year = ?;",
          ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
          ps.setInt(1, cid);
          ps.setInt(2, year);
          rs = ps.executeQuery();

          while (rs.next()) {
              rs.updateFloat("hdi_score", newHDI);
              rs.updateRow();
              is_successful = true;
          }

       } catch (SQLException se) {
           is_successful = false;
       }

       finally {
          try {
             if (rs != null) {
                 rs.close();
             }
           } catch (SQLException se) {
           }
           try {
               if (ps != null) {
                   ps.close();
               }
           } catch (SQLException se) {
           }
      }
     return is_successful;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      boolean is_deleted = false;

      try {
          ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country in (?,?) AND neighbor in (?,?);");
          ps.setInt(1, c1id);
          ps.setInt(2, c2id);
          ps.setInt(3, c1id);
          ps.setInt(4, c2id);
          int is_succ_executed = ps.executeUpdate();

          if (is_succ_executed > 0) {
              is_deleted = true;
          }

      } catch (SQLException se) {
           is_deleted = false;
      }

      finally {
          try {
              if (ps != null) {
                 ps.close();
              }
           } catch (SQLException se) {
               is_deleted = false;
           }
     }
     return is_deleted;
  }

  public String listCountryLanguages(int cid){
      String languagesSpoken="";

      try{     //need to order result by population
          ps = connection.prepareStatement("SELECT lid, lname, (lpercentage * country.population) as population " +
                                           "FROM a2.country join a2.language on (language.cid = country.cid) " +
                                           "WHERE language.cid = ? ORDER BY population;");
           ps.setInt(1, cid);
           rs = ps.executeQuery();

           while (rs.next()) {
               int lid = rs.getInt("lid");
               String lname = rs.getString("lname").trim();
               String population = rs.getString("population");
               languagesSpoken+= lid + ":" + lname + ":" + population + "#";
           }
           if (languagesSpoken != "") {
               return languagesSpoken.substring(0, languagesSpoken.length()-1);
           }
           return languagesSpoken;

       } catch (SQLException se) {
           languagesSpoken = "";
       }

       finally {
           try {
               if (rs != null) {
                    rs.close();
               }
           } catch (SQLException se) {
               languagesSpoken = "";
           }
           try {
               if (ps != null) {
                   ps.close();
               }
           } catch (SQLException se) {
               languagesSpoken = "";
           }
       }
     return languagesSpoken;
  }

  public boolean updateHeight(int cid, int decrH){
      boolean is_updated = false;

      try {
          ps = connection.prepareStatement("UPDATE a2.country SET height = ? WHERE cid = ?;");
          ps.setInt(1,decrH);
          ps.setInt(2,cid);

          int is_update_successful = ps.executeUpdate();

          if (is_update_successful > 0) {
              is_updated = true;
          }

       } catch (SQLException se) {
           is_updated = false;
       }

       finally {
           try {
               if (ps != null) {
                    ps.close();
               }
           } catch (SQLException se) {
               is_updated = false;
           }
       }
    return is_updated;
  }

  public boolean updateDB(){
      boolean is_updated_db = false;
      String query;
      // drop a2.mostPop.. before creating
      try {
          sql = connection.createStatement();
          int dropTable = sql.executeUpdate("DROP TABLE a2.mostPopulousCountries;");
      } catch (SQLException se) {
          is_updated_db = false;
      }

      try {
          query = "CREATE TABLE a2.mostPopulousCountries AS (SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC);";
          sql = connection.createStatement();
          int updated_passed  = sql.executeUpdate(query);

      } catch (SQLException se) {
          is_updated_db = false;
      }
      // check whether creation and insertion are successful
      try {
          sql = connection.createStatement();
          rs = sql.executeQuery("SELECT count(*) as countCID from a2.mostPopulousCountries;");

          while (rs.next()){
              if (rs.getInt("countCID") >= 0) {
                  is_updated_db = true;
               }
          }
      } catch (SQLException se) {
          is_updated_db = false;
      }

      finally {
           try {
               if (rs != null) {
                   rs.close();
               }
           } catch (SQLException se) {
               is_updated_db = false;
           }
           try {
               if (sql != null) {
                    sql.close();
               }
           } catch (SQLException se) {
               is_updated_db = false;
           }
      }
      return is_updated_db;
  }
}