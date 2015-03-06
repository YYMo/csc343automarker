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
	}catch(ClassNotFoundException e){
		return ;
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      	try{
		connection = DriverManager.getConnection(URL, username, password);
		//sql = connection.createStatement();
	} catch (SQLException e){
		return false;
	}
	if (connection != null){
		return true;
	}else{
		return false;
	}
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	try{
		connection.close();
		sql.close();
	} catch (SQLException e){
		return false;
	}
	return true;
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
	try{
		sql = connection.createStatement();
		String query = String.format("SELECT * FROM a2.country WHERE cid=%d;", cid);
		rs = sql.executeQuery(query);
		if (rs.next()){
			return false;
		}
		rs.close();

		sql =connection.createStatement();

		query = String.format("INSERT INTO a2.country VALUES (%d, '%s', %d, %d);",
					cid, name, height, population);
		sql.executeUpdate(query);
		return true;
	} catch (SQLException e){
		return false;
	}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	int oceancount = 0;
	try{
		String query = String.format("SELECT * FROM a2.ocean oc WHERE oc.oid = %d;", oid);
		rs = sql.executeQuery(query);
		if (!(rs.next())){
			rs.close();
			return -1;
		}

		query = String.format("SELECT count(*) FROM a2.country co JOIN a2.oceanAccess oa ON co.cid=oa.oid WHERE "+
					"oa.oid = %d GROUP BY co.cid;", oid);
		rs = sql.executeQuery(query);
		if (rs.next()){
			oceancount = rs.getInt("count");
		}
		rs.close();
	}catch (SQLException e){
		return -1;
	}
	return oceancount;
  }
   
  public String getOceanInfo(int oid){
	String result = "";
	try{
		String query = String.format("SELECT * FROM a2.ocean oc WHERE oc.oid= %d;", oid);
		rs = sql.executeQuery(query);
		if (rs.next()){
			result = (result+rs.getString(1)+":"+rs.getString(2)+":"+rs.getString(3));
		}
		rs.close();
	}catch (SQLException e){
		return "";
	}
	return result;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	try{
		String query = String.format("SELECT * FROM a2.hdi hd WHERE hd.cid=%d "+
						"AND hd.year=%d;", cid, year);
		rs = sql.executeQuery(query);
		if (!rs.next()){
			return true;
		}
		rs.close();
		query = String.format("UPDATE a2.hdi SET hdi_score=%f WHERE "+
					"hdi.cid=%d AND hdi.year=%d;", newHDI, cid, year);
		sql.executeUpdate(query);
	}catch (SQLException e){
		return false;
	}return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	String query;
	try{
		query = String.format("SELECT * FROM a2.country co WHERE co.cid=%d;", c1id);
		rs = sql.executeQuery(query);
		if (!rs.next())
			return true;
		rs.close();

		query = String.format("SELECT * FROM a2.country co WHERE co.cid=%d;", c2id);
		ResultSet rs = sql.executeQuery(query);
		if (!rs.next())
			return true;
		rs.close();
	}catch (SQLException e){
		return false;
	}
	try{
		query = String.format("DELETE FROM a2.neighbour WHERE country=%d "+
					"AND neighbor=%d;", c1id, c2id);
		sql.executeUpdate(query);
	}catch (SQLException e){
		return false;
	}
	try{
		query = String.format("DELETE FROM a2.neighbour ne WHERE ne.country=%d "+
					"AND ne.neighbor=%d;", c2id, c1id);
		sql.executeUpdate(query);
	}catch (SQLException e){
		return false;
	}
	return true;
  }
  
  public String listCountryLanguages(int cid){
	String result = "";
	try{
		String query = String.format("SELECT la.lid, la.lname, (la.lpercentage*c1.population) as population FROM "+
						"a2.language la JOIN a2.country c1 ON la.cid=c1.cid WHERE "+
						"c1.cid=%d;", cid);
		rs = sql.executeQuery(query);
		if (rs!=null){
			while (rs.next()){
				result = result + String.format("%d:%s:%f", rs.getInt("lid"), rs.getString("lname"),
							rs.getDouble("population"));
			}
		}
		rs.close();
		return result;
	}catch (SQLException e){
		return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH){
	try{
                String query = String.format("SELECT oc.oid FROM a2.country co JOIN a2.oceanAccess oa ON "+
						"co.cid=oa.cid JOIN a2.ocean oc ON oa.oid=oc.oid WHERE "+
                                                "co.cid=%d;", cid);
                rs = sql.executeQuery(query);
                if (!rs.next()){
                        return true;
                }
                rs.close();
                query = String.format("UPDATE a2.ocean SET depth=%d WHERE "+
                                        "oid IN (SELECT oc.oid FROM a2.country co JOIN a2.oceanAccess oa ON "+
                                                "co.cid=oa.cid JOIN a2.ocean oc ON oa.oid=oc.oid WHERE "+
                                                "co.cid=%d);", decrH, cid);//decrH, rs.getInt("oid"));
                sql.executeUpdate(query);

	}catch(SQLException e){
		return false;
	}
	return true;
  }
    
  public boolean updateDB(){
	String query, insert;
	String table = "CREATE TABLE a2.mostPopulousCountries (cid INTEGER REFERENCES a2.country(cid) ON " +
			"DELETE RESTRICT, cname VARCHAR(20) NOT NULL);";
	try {
		sql.executeUpdate(table);
		//query = "SELECT co.cid, co.cname FROM a2.country co WHERE co.population>100000000;";
		//rs = sql.executeQuery(query);
		//if (rs.next()){
		insert = "INSERT INTO a2.mostPopulousCountries ( "+
			"SELECT co.cid, co.cname FROM a2.country co WHERE co.population>100000000);";
			//insert = String.format("INSERT INTO a2.mostPopulousCountries VALUES(%d, '%s');",
					//rs.getInt("cid"), rs.getString("cname"));
		sql.executeUpdate(insert);
		//}
	} catch (SQLException e){
		return false;
	}
	return true;

  }
  
}
