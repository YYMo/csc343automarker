import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;


public class Main {

	
	/**
	 * This class is used to test 
	 * you're welcome to review/use/abuse if you wish
	 * 
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		Assignment2.LOGGING_ENABLED = true;
		Assignment2 a = new Assignment2();
		a.connectDB(args[0], args[1], args[2]);
		
		setup(a);
		
		System.out.println("Countries with oceans: " + 
				a.getCountriesNextToOceanCount(1));
		
		System.out.println("Ocean info ------>" + 
				a.getOceanInfo(1));
		
		System.out.println("update hdi ------>" + 
				a.chgHDI(1, 2001,  0.8f));
		
		System.out.println("update hdi ------>" + 
				a.chgHDI(1, 2000,  0.8f));
		
		System.out.println("delete neighbour ------>" + 
				a.deleteNeighbour(0, 79));
		
		System.out.println(a.listCountryLanguages(1));
		
		System.out.println("dec height ------>" + 
				a.updateHeight(0, 79));
		
		
		System.out.println(a.updateDB());
		
		
		a.disconnectDB();
	}
	
	
	
	// populates the tables for me to test
	public static void setup(Assignment2 a) throws SQLException {
		
		
		
		a.createStatement().execute("DROP TABLE IF EXISTS mostPopulousCountries");
		a.createStatement().execute("delete from neighbour");
		a.createStatement().execute("delete from oceanAccess");
		a.createStatement().execute("delete from ocean");
		a.createStatement().execute("delete from hdi");
		a.createStatement().execute("delete from religion");
		a.createStatement().execute("delete from language");
		a.createStatement().execute("delete from country");
		
		ArrayList<Integer> oceanIds = new ArrayList<Integer>();
		
		Random r = new Random();
		
		int oceans = r.nextInt(10);
		
		for (int h = 0; h < oceans; h++)
		{
			PreparedStatement ps = a.createPeparedStatement("insert into ocean (oid, oname, depth) values (?, ?, ?)");
			ps.setInt(1, h);
			ps.setString(2, "Ocean " + h);
			ps.setInt(3, r.nextInt());
			System.out.println("ocean " + h);
		    oceanIds.add(h);
			ps.execute();
		}
		
		ArrayList<Integer> cntIds = new ArrayList<Integer>();
		
		ArrayList<String> religions = new ArrayList<String>();
		for (int rr = 0; rr < 50; rr++)
			religions.add("r" + rr);
		
		int countries = 200;
		for (int i = 0; i < countries; i++) {
			
			int[] popluations= new int[0];
			while (popluations.length == 0) popluations = new int[r.nextInt(20)];
			int pop = 0;
			for (int p = 0; p < popluations.length; p++)
			{
				popluations[p] = r.nextInt(10000000);
				pop += popluations[p];
			}
			ArrayList<Integer> lIds = new ArrayList<Integer>();
			System.out.println("Country " + i + ": " + a.insertCountry(i, "cnt name " + i, i*100, pop));
			for (int l = 0; l < popluations.length; l++) {
				PreparedStatement ps = a.createPeparedStatement("insert into language (cid, lid, lname, lpercentage) values (?, ?, ?, ?)");
				ps.setInt(1, i);
				int rId = -1;
				while (true) {
					rId = r.nextInt(religions.size());
					if (lIds.contains(rId)) continue;
					lIds.add(rId);
					break;
				}
				ps.setInt(2, rId);
				ps.setString(3, religions.get(rId));
				ps.setFloat(4, ((float)popluations[l]/(float)pop));
				System.out.println("Lang " + i + " " + ((float)popluations[l]/(float)pop));
				ps.execute();
			}
			ArrayList<Integer> rIds = new ArrayList<Integer>();
			for (int l = 0; l <  popluations.length; l++) {
				PreparedStatement ps = a.createPeparedStatement("insert into religion (cid, rid, rname, rpercentage) values (?, ?, ?, ?)");
				ps.setInt(1, i);
				int rId = -1;
				while (true) {
					rId = r.nextInt(religions.size());
					if (rIds.contains(rId)) continue;
					rIds.add(rId);
					break;
				}
				ps.setInt(2, rId);
				ps.setString(3, religions.get(rId));
				ps.setFloat(4, ((float)popluations[l]/(float)pop));
				System.out.println("rel " + i + " " + rId);
				ps.execute();
			}
			for (int h = 0; h < r.nextInt(20); h++)
			{
				PreparedStatement ps = a.createPeparedStatement("insert into hdi (cid, year, hdi_score) values (?, ?, ?)");
				ps.setInt(1, i);
				ps.setInt(2, 2008 + h);
				ps.setFloat(3, r.nextFloat());
				System.out.println("hdi " + i + " " + h);
				ps.execute();
			}
			int oceanAccess = Math.abs(r.nextInt(oceanIds.size()));
			for (int h = 0; h < oceanAccess; h++)
			{
				PreparedStatement ps = a.createPeparedStatement("insert into oceanAccess (cid, oid) values (?, ?)");
				ps.setInt(1, i);
				ps.setInt(2, oceanIds.get(h));
				System.out.println("ocean access " + i + " " + h);
				ps.execute();
			}
			cntIds.add(i);
		}
		
		int ns = r.nextInt(10);
		for (int h = 0; h < cntIds.size(); h++)
		{
			ArrayList<Integer> neighbors = new ArrayList<Integer>();
			while (neighbors.size()<ns)
			{
				int id = cntIds.get(r.nextInt(cntIds.size()));
				if (id != cntIds.get(h) && !neighbors.contains(id))
					neighbors.add(id);
			}
			for (int n : neighbors)
			{
				PreparedStatement ps = a.createPeparedStatement("insert into neighbour (country, neighbor, length) values (?, ?, ?)");
				ps.setInt(1, cntIds.get(h));
				ps.setInt(2, n);
				ps.setInt(3, r.nextInt());
				System.out.println("neighbor " + h + " " + n);
				ps.execute();
			}
		}
		
	}

}
