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
  	public Assignment2(){
		try{
		// load JDBC driver
		Class.forName("org.postgresql.Driver");
		}catch (ClassNotFoundException e){
		//System.out.println("Include in your library path.");
		//e.printStackTrace();
		}
  	}

  	//Using the input parameters, establish a connection to be used for this session.
	//Returns true if connection is sucessful
  	public boolean connectDB(String URL, String username, String password){
		try{
			// make connection with the database
			connection = DriverManager.getConnection(URL, username, password);
		}catch (SQLException e){
			//System.out.println("Connection failed");
			//e.printStackTrace();
			return false;
		}
		if (connection == null)
			return false;
		return true;
  	}

  	//Closes the connection. Returns true if closure was sucessful
  	public boolean disconnectDB(){
		try{
			connection.close();
			return true;
		}catch (SQLException e){
			//e.printStackTrace();
			//System.out.println("Disconnect failed");
			return false;
		}
  	}

	//Insert a row into the country table with cid cid, cname name, height hegith,
	//population population
	//Returns true if the insertion was successful, false otherwise.
	public boolean insertCountry (int cid, String name, int height, int population) {
		try{
			// check if the country is in
			String check = "SELECT * " +
				       "FROM a2.country " +
				       "WHERE cid = " + cid;
			sql = connection.createStatement();
			rs = sql.executeQuery(check);
			if (rs.next() == false){
				String statement = String.format(
							"INSERT INTO a2.country " +
						 	"VALUES(%d, %s, %d, %d)",
							cid, "\'"+name+"\'", height, population);
                                System.out.println(statement);
				sql.executeUpdate(statement);
				sql.close();
				rs.close();
				return true;
			}
		}catch(SQLException e){
			//System.out.println("Insertion into table country failed");
			//e.printStackTrace();
			return false;
		}
		return false;
	}

	//Returns the number of countries that located next to the ocean with id oid.
	//Return -1 if an error occur.
	public int getCountriesNextToOceanCount(int oid) {
		try{
			int count = 0;
			String check = "SELECT count(DISTINCT cid) " +
				       "FROM a2.oceanAccess " +
				       "WHERE oid = " + oid;
			sql = connection.createStatement();
			rs = sql.executeQuery(check);
			if (rs.next())
				count = rs.getInt("count");
			sql.close();
			rs.close();
			return count;
		}catch (SQLException e){
			//System.out.println("Failed counting");
			//e.printStackTrace();
			return -1;
		}
  	}

	public String getOceanInfo(int oid){
		try{
			String result = "";
			String check = "SELECT * "+
			       	       "FROM a2.ocean "+
			       	       "WHERE oid = " + oid;
			sql = connection.createStatement();
			rs = sql.executeQuery(check);
			if (rs.next())
				result = oid + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
			sql.close();
			rs.close();
			return result;
		}catch (SQLException e){
			//System.out.println("Failed getting ocean info");
			//e.printStackTrace();
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		try{
			boolean temp = false;
			String check = "SELECT * " +
				       "FROM a2.hdi " +
				       "WHERE cid = " + cid + " " +
				       "AND year = " + year;
			sql = connection.createStatement();
			rs = sql.executeQuery(check);
			if (rs.next()){
				temp = true;
				String update = "UPDATE a2.hdi " +
					       "SET hdi_score = " + newHDI + " " +
				    	       "WHERE cid = " + cid + " " +
				     	       "AND year = " + year;
				sql.executeUpdate(update);}
			sql.close();
			rs.close();
			return temp;
		}catch (SQLException e){
			//System.out.println("Failed changing hdi");
			//e.printStackTrace();
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		try{
			boolean temp = false;
			String check1 = "SELECT * "+
					"FROM a2.neighbour " +
					"WHERE country = " + c1id + " " +
					"AND neighbor = " + c2id;
			String check2 = "SELECT * "+
                                        "FROM a2.neighbour " +
                                        "WHERE country = " + c2id + " " +
                                        "AND neighbor = " + c1id;
			sql = connection.createStatement();
			rs = sql.executeQuery(check1);
			if (rs.next()){
				rs = sql.executeQuery(check2);
				if (rs.next()){
					String deleteCountry1 = "DELETE FROM a2.neighbour " +
							        "WHERE country = " + c1id + " " +
							        "AND neighbor = " + c2id;
					String deleteCountry2 = "DELETE FROM a2.neighbour " +
                                                                "WHERE country = " + c2id + " " +
                                                                "AND neighbor = " + c1id;
					sql.executeUpdate(deleteCountry1);
					sql.executeUpdate(deleteCountry2);
					temp = true;
				}
			}/*else{
				System.out.println("Error: The two countries are not neighbouring coutries");
			}*/
			sql.close();
			rs.close();
			return temp;
		}catch (SQLException e){
			//System.out.println("Failed deleting neighbour");
			//e.printStackTrace();
			return false;
		}
  	}

      public String listCountryLanguages(int cid){
		try{
			String result = "";
            		String statement = String.format(
						"SELECT L.lid as lid, L.lname as lname,round(C.population*L.lpercentage) as lpop  FROM a2.country C JOIN a2.language L on C.cid = L.cid where L.cid = " +cid);
            		String check = "SELECT * "+
                        	"FROM a2.country "+
                              	"WHERE cid = " + cid;
            		sql = connection.createStatement();
            		rs = sql.executeQuery(check);
            		if (rs.next()){
                		rs= sql.executeQuery(statement);
                	while (rs.next()){
                    		result = result + rs.getInt("lid")+ ":" + rs.getString("lname") + ":" + rs.getInt("lpop") + "#";}
            		}
            		sql.close();
            		rs.close();
            		return result;
		}catch (SQLException e){
		    	System.out.println("Failed getting ocean info");
		    	e.printStackTrace();
			return "";
		}
	}

  	public boolean updateHeight(int cid, int decrH){
    		try{
			boolean temp = false;
			String check = "SELECT * " +
				       "FROM a2.country " +
				       "WHERE cid = " + cid;
			sql = connection.createStatement();
			rs = sql.executeQuery(check);
			if (rs.next()){
				int newHeight = rs.getInt("height") - decrH;
				if (newHeight >= 0){
					temp = true;
					String update = "UPDATE a2.country " +
							"SET height = " + newHeight + " " +
							"WHERE cid = " + cid;
					sql.executeUpdate(update);
				}/*else{ System.out.println("Error: Negative height");}*/
			}
			sql.close();
			rs.close();
			return temp;
		}catch (SQLException e){
			//System.out.println("Failed updating height");
			//e.printStackTrace();
			return false;
		}
  	}

	public boolean updateDB(){
		try{
			String drop = "DROP TABLE IF EXISTS mostPopulousCountries CASCADE";
			sql = connection.createStatement();
			String search_path = "SET search_path TO A2";
			sql.executeUpdate(search_path);
			sql.executeUpdate(drop);
			String create = "CREATE TABLE mostPopulousCountries(" +
					"cid         INTEGER         REFERENCES country(cid) ON DELETE RESTRICT," +
					"cname       VARCHAR(20)     NOT NULL)";
			sql.executeUpdate(create);
			String check = "SELECT * " +
				       "FROM country " +
				       "WHERE population > 10E6";
			rs = sql.executeQuery(check);
			String statement = String.format("INSERT INTO mostPopulousCountries "+
                                                         "VALUES(?, ?)");
			ps = connection.prepareStatement(statement);
			while (rs.next()){
				int cid = rs.getInt("cid");
				String cname = rs.getString("cname");
				ps.setInt(1, cid);
				ps.setString(2, cname);
				ps.executeUpdate();
				//System.out.println(statement);
			}
			rs.close();
			sql.close();
			ps.close();
			return true;
		}catch (SQLException e){
			//System.out.println("Failed updating DB");
			//e.printStackTrace();
			return false;
		}
  	}
	/*
	public static void main(String[] args){
		Assignment2 test = new Assignment2();
		test.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3olga", "g3olga", "");
		test.insertCountry(100001, "Olga", 12, 53252);
		System.out.println("getCountriesNextToOceanCount(9001) => " + test.getCountriesNextToOceanCount(9001));
		System.out.println("getOceanInfo(9002) => " + test.getOceanInfo(9002));
		System.out.println("getOceanInfo(9008) => " + test.getOceanInfo(9008));
		System.out.println("chgHDI(10006, 2012, (float)0.84) => " + test.chgHDI(10006, 2012, (float)0.84));
		System.out.println("chgHDI(10034, 2014, (float)0.43) => " + test.chgHDI(10034, 2014, (float)0.43));
		System.out.println("deleteNeighbour(10001, 10002) => " + test.deleteNeighbour(10001, 10002));
		System.out.println("deleteNeighbour(10008, 10002) => " + test.deleteNeighbour(10008, 10002));
		System.out.println("updateHeight(10001, 10) => " + test.updateHeight(10001, 10));
		System.out.println("updateHeight(10002,3200) => " + test.updateHeight(10002,3200));
		System.out.println("listCountryLanguages(10001) => " + test.listCountryLanguages(10001));
		test.updateDB();
	}*/
}
