import java.sql.*;

public class Assignment2 {
	Connection connection; // A connection to the database
	Statement sql;         // Statement to run queries
	PreparedStatement ps;  // Prepared Statement
	ResultSet rs;          // Resultset for the query
	boolean loaded;        // Checks if the Driver is loaded.

	/** Constructor */
	Assignment2() {
		connection = null; sql = null; ps = null; rs = null;

		try {
			Class.forName("org.postgresql.Driver");
			loaded = true;
		} catch(ClassNotFoundException e) { loaded = false; }
	}


	/**
	 * Using the input parameters, establish a connection to be used for this
	 *  session. Returns true if connection is successful
	 * @param  URL      [description]
	 * @param  username [description]
	 * @param  password [description]
	 * @return          [description]
	 */
	public boolean connectDB(String URL, String username, String password){
		if(loaded == false) return false;

		try {
			String url = "jdbc:postgresql://" + URL;
			connection = DriverManager.getConnection(url, username, password);
			return true;
		} catch(SQLException e) { connection = null; }

		return false;
	}


	/**
	 * [sqlExec description]
	 * @param query [description]
	 */
	private void sqlExec(String query) {
		try {
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
		} catch(SQLException e1) { }

		try{ ps.close(); }
		catch(SQLException e2) { ps = null; }
	}


	/**
	 * [sqlClean description]
	 */
	private void sqlClean() {
		try { ps.close(); }
		catch(SQLException e2) { ps = null; }
	}


	/**
	 * [sqlUpdate description]
	 * @param  query [description]
	 * @return       [description]
	 */
	private boolean sqlUpdate(String query) {
		int update = 0;
		try {
			ps = connection.prepareStatement(query);
			update = ps.executeUpdate();
		} catch(SQLException e1) { update = -1; }

		try { ps.close(); }
		catch(SQLException e2) { ps = null; }

		if(update == 1) return true;
		return false;
	}


	/**
	 * Closes the connection. Returns true if closure was successful
	 *
	 * @return [description]
	 */
	public boolean disconnectDB(){
		if(connection != null) return true;

		try {
			connection.close();
			connection = null;
		} catch(SQLException e) { return false; }

		return true;
	}


	/**
	 * Inserts a tuple into the country relation, if the tuple is not
	 * already in the relation.
	 *
	 * @param  cid        [description]
	 * @param  name       [description]
	 * @param  height     [description]
	 * @param  population [description]
	 * @return            [description]
	 */
	public boolean insertCountry(int cid, String name, int height, int population) {
		String query = String.format(
			"INSERT INTO a2.country " +
				"(cid, cname, height, population) " +
				"VALUES(%d, '%s', %d, %d)",
			cid, name, height, population
		);

		return this.sqlUpdate(query);
	}


	/**
	 * Returns the number of counties with direct access to ocean oid.
	 *
	 * @param  oid [description]
	 * @return     [description]
	 */
	public int getCountriesNextToOceanCount(int oid) {
		String query = String.format(
			"SELECT count(*)" +
				"FROM a2.oceanAccess " +
				"WHERE oid = %d " +
				"GROUP BY oid",
			oid
		);
		this.sqlExec(query);

		try {
			return rs.getInt(1);
		} catch(SQLException e) { }
		return -1;
	}


	/**
	 * Gets info about ocean with given oid and returns tuple as a string.
	 *
	 * @param  oid [description]
	 * @return     [description]
	 */
	public String getOceanInfo(int oid){
		String query = String.format(
			"SELECT *" +
				"FROM a2.ocean " +
				"WHERE oid = %d ",
			oid
		);
		this.sqlExec(query);

		try {
			if(rs.next()) return
				rs.getString(1) + ":" +
				rs.getString(2) + ":" +
				rs.getString(3);
		} catch(SQLException e) { }

		return "";
	}


	/**
	 * Updates the HDI of a tuple with given cid and year to be newHDI.
	 *
	 * @param  cid    [description]
	 * @param  year   [description]
	 * @param  newHDI [description]
	 * @return        [description]
	 */
	public boolean chgHDI(int cid, int year, float newHDI){
		String query = String.format(
			"UPDATE a2.hdi" +
				"FSET hdi_score = %d " +
				"WHERE cid = %d AND year = %d",
			newHDI, cid, year
		);
		return this.sqlUpdate(query);
	}


	/**
	 * Deletes tuple in neighbor relation where county = c1id
	 * and neighbor = c2id.
	 *
	 * @param  c1id [description]
	 * @param  c2id [description]
	 * @return      [description]
	 */
	public boolean deleteNeighbour(int c1id, int c2id){
		String query1 = String.format(
			"DELETE FROM a2.neighbour" +
				"WHERE country = %d and neighbor = %d ",
			c1id, c2id
		);

		String query2 = String.format(
			"DELETE FROM a2.neighbour" +
				"WHERE country = %d and neighbor = %d ",
			c2id, c1id
		);

		return this.sqlUpdate(query1) && this.sqlUpdate(query2);
	}


	/**
	 * Returns a list of languages spoken in the country with the given cid.
	 *
	 * @param  cid [description]
	 * @return     [description]
	 */
	public String listCountryLanguages(int cid){
		String str = "";
		String query = String.format(
			"SELECT lid, lname, (population * lpercentage) AS lpopulation " +
				"FROM a2.country c, a2.language l " +
				"WHERE c.cid = %d and c.cid = l.cid " +
				"order by lpopulation",
			cid);
		this.sqlExec(query);

		try {
			boolean rows = rs.next();
			while(rows) {
				str += rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
				rows = rs.next();
				if(rows) str += "#";
			}
		} catch(SQLException e) { }

		return str;
	}


	/**
	 * [updateHeight description]
	 *
	 * @param  cid   [description]
	 * @param  df_ht [description]
	 * @return       [description]
	 */
	public boolean updateHeight(int cid, int df_ht) {
		int h = 0;

		String query1 = String.format(
			"SELECT height " +
				"FROM a2.country " +
				"WHERE c.cid = %d",
			cid);
		this.sqlExec(query1);

		try {
			if(rs.next()) h = rs.getInt(1) - df_ht;
		} catch(SQLException e) { }

		String query2 = String.format(
			"UPDATE a2.country " +
				"SET height = %d " +
				"WHERE cid = %d",
			h, cid
		);
		return this.sqlUpdate(query2);
	}


	/**
	 * Update Database with the mostPopulousCountries table.
	 *
	 * @return [description]
	 */
	public boolean updateDB(){
		String query = "SELECT cid, cname INTO a2.mostPopulousCountries FROM a2.country WHERE population>100000000;";
		return this.sqlUpdate(query);
	}


	/**
	 * [main description]
	 * @param args[] [description]
	 */
	public static void main(String args[]) {
		Assignment2 a2 = new Assignment2();
		a2.connectDB("csc343h-a2", "postgres", "1");

		a2.insertCountry(10, "Countryj", 200, 100);
		a2.getCountriesNextToOceanCount(3);
		a2.getOceanInfo(2);
		a2.chgHDI(1, 2000, (float) 0.23);
		a2.deleteNeighbour(1, 2);
		a2.listCountryLanguages(1);
		a2.updateHeight(1, 100);
		a2.updateDB();
		a2.disconnectDB();
	}
}