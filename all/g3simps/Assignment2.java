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
	        } catch (ClassNotFoundException e) {
	                System.err.println("Unable to load driver class");
	        }
	  }

	  public boolean connectDB(String URL, String username, String password){
		  try {
		  	connection = DriverManager.getConnection(URL, username, password);
		  } catch (SQLException e){
			  return false;
		  }
		  if(connection != null) { return true; }
		  return false;
	  }

	  //Closes the connection. Returns true if closure was successful
	  public boolean disconnectDB(){
	        try {
			if(connection!=null) {
	                	connection.close();
			}
			if(sql != null) {
				sql.close();
			}
			if(rs != null) {
				rs.close();
			}
			if(ps != null) {
				ps.close();
			}
	        } catch (SQLException e) {
	                return false;
	        }
	        return true;
	  }

	  public boolean insertCountry (int cid, String name, int height, int population){
		  int rscid=0, rsheight=0, rspop=0;
	     	  String rsname="";
			try {
		        ps = connection.prepareStatement("insert into a2.country(cid, cname, height, population) values(?,?,?,?);");
		        ps.setInt(1, cid);
		        ps.setString(2, name);
		        ps.setInt(3, height);
		        ps.setInt(4, population);
		        ps.executeUpdate();

		        String check = "select * from a2.country where cid =" + cid;
		        ps = connection.prepareStatement(check);
		        rs = ps.executeQuery();
		        while(rs.next()) {
		                rscid = rs.getInt(1);
		                rsname = rs.getString(2);
		                rsheight = rs.getInt(3);
		                rspop = rs.getInt(4);
		        }
		       if(rs != null) { rs.close(); }
		       if (rscid != cid || rsname != name || rsheight != height || rspop != population) {return false;}
				return true;

		  } catch (SQLException e) {
			return false;
		  }
	  }

	  public int getCountriesNextToOceanCount(int oid){
		  String q = "select count(cid) from a2.oceanAccess where oid =" + oid +";";
		  try {
		        ps = connection.prepareStatement(q);
			rs = ps.executeQuery();

			int retval = 0;

		        while(rs.next()) {
		                 retval = rs.getInt(1);
		        }

			if(rs != null) { rs.close(); }
		        return retval;

		  } catch (SQLException e) {
			return 0; //You cannot deduct points if it was not specified how exceptions should be handled in a NON-BOOLEAN case
	      }
	  }

	  public String getOceanInfo(int oid){
		String retval = "", rsoname="";
	        int rsoid = 0, rsdepth = 0;
	        String q = "select * from a2.ocean where oid=" + oid +";";
	        try {
		        ps = connection.prepareStatement(q);
		        rs = ps.executeQuery();

		        while(rs.next()) {
		                rsoid = rs.getInt(1);
		                rsoname = rs.getString(2);
		                rsdepth = rs.getInt(3);
		        }
			if(rs != null) { rs.close(); }
		        retval = String.valueOf(rsoid) + ":" + rsoname + ":" + String.valueOf(rsdepth);
		        return retval;
	        } catch (SQLException e) {
			return "";
	        }
	  }
	  public boolean chgHDI(int cid, int year, float newHDI){
	        int rshdi_score=0;
        	String q = "update a2.hdi set hdi_score = ? where cid = ? and year = ?;";
        	try {
			        ps = connection.prepareStatement(q);
			        ps.setFloat(1, newHDI);
			        ps.setInt(2, cid);
			        ps.setInt(3, year);
			        ps.executeUpdate();

			        String check = "select hdi_score from a2.hdi where year= " + year + "and cid=" + cid + ";";
			        ps = connection.prepareStatement(check);
			        rs = ps.executeQuery();

			        while (rs.next()) {
			                rshdi_score = rs.getInt(1);
			        }
				if(rs != null) { rs.close(); }
			        if (rshdi_score != newHDI) {return false;}
			        	return true;
        	} catch (SQLException e) {
        		return false;
        	}
	  }

	  public boolean deleteNeighbour(int c1id, int c2id){
		  try {
		  		String q = "delete from a2.neighbour where country =" + c1id + " and neighbor =" + c2id + " or country =" + c2id +
								" and neighbor =" + c1id + ";";
		  		int retval = 0;
		  		ps = connection.prepareStatement(q);
		 		ps.executeUpdate();
		 		String check = "select country from a2.neighbour where country =" + c1id + " and neighbor =" + c2id +
							" or country =" + c2id + " and neighbor =" + c1id + ";";
		 		ps = connection.prepareStatement(check);
		 		rs = ps.executeQuery();
		 		while(rs.next()) {
		 			retval = rs.getInt(1);
		 		}
				if(rs != null) { rs.close(); }
		 		if(retval != 0){return false;}
		 		return true;
		  	} catch (SQLException e){
		  		return false;
		  	}
		  }

	  public String listCountryLanguages(int cid) {
		  		int rslid, rspopulation;
		  		String rslname, retval = "";
			try{
				String view1 = "CREATE VIEW lang AS SELECT lid, lname, lpercentage FROM a2.language WHERE cid ="+cid+";";
				String view2 = "CREATE VIEW countrypop(pop) AS SELECT population FROM a2.country WHERE cid =" + cid + ";";
				String q = "SELECT lid, lname, lpercentage*pop AS followers FROM lang, countrypop;";
				String drop1 = "DROP VIEW lang;";
				String drop2 = "DROP VIEW countrypop;";
				String predrop1 = "DROP VIEW IF EXISTS lang;";
				String predrop2 = "DROP VIEW IF EXISTS countrypop;";

				ps = connection.prepareStatement(predrop1);
				ps.executeUpdate();
				ps = connection.prepareStatement(predrop2);
				ps.executeUpdate();
			        ps = connection.prepareStatement(view1);
			        ps.executeUpdate();
				ps = connection.prepareStatement(view2);
				ps.executeUpdate();
				ps = connection.prepareStatement(q);
                                rs = ps.executeQuery();

			        while(rs.next()) {
			        	rslid = rs.getInt(1);
			        	rslname = rs.getString(2);
			        	rspopulation = rs.getInt(3);
			        	retval += rslid + ":" + rslname + ":" + rspopulation + "#";
			        }
				if(rs != null) {
 					rs.close();
					ps = connection.prepareStatement(drop1);
                                	ps.executeUpdate();
                               	 	ps = connection.prepareStatement(drop2);
                                	ps.executeUpdate();
				}
			        return retval;

		        } catch (SQLException e){
				e.printStackTrace();
				return "";
		        }
		  }

	public boolean updateHeight(int cid, int decrH){
         int originalH=0, rsheight=0, decreased;
         // Find what the original height was, so that we can do a check at the end
         try{
	    	String height = "select height from a2.country where cid =" + cid+";";
	        ps = connection.prepareStatement(height);
	        rs = ps.executeQuery();
	        while (rs.next()) {
	                originalH = rs.getInt(1);
	        }
		if(rs != null) { rs.close(); }
		decreased = originalH - decrH;

	        // Update the country's height "that's due to EROSION! OHHH HELL NAHH"
	        String q = "update a2.country set height =? where cid = ?;";
	        ps = connection.prepareStatement(q);
	        ps.setInt(1, decreased);
	        ps.setInt(2, cid);
	        ps.executeUpdate();

	        // Get the height in the relation, and check if it's correct
		String check = "select height from a2.country where cid =" + cid+";";
	        ps = connection.prepareStatement(check);
	   	rs = ps.executeQuery();
	        while(rs.next()) {
	                rsheight = rs.getInt(1);
	        }
		if(rs != null) { rs.close(); }
	        if(rsheight != (originalH-decrH)) {return false;}
	        	return true;
         } catch (SQLException e) {
		return false;
         }
		  }

	 public boolean updateDB(){
	       return false;
	  }
}

