import java.io.File;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class XMLCreate {

	public static void main(String arg[]) {
      

//Table data to XML 
		Document doc = null;
		try {
			doc = XMLCreate.XMLcreation();
		} catch (TransformerException | ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Document XMLcreation() throws TransformerException,ParserConfigurationException 
	{
		Scanner input=new Scanner(System.in);
		String startdate=input.next();
		Scanner input1=new Scanner(System.in);
	    String enddate=input1.next();
	    Scanner input3=new Scanner(System.in);
	    String filepath=input3.next();
		Connection connect = null;
		PreparedStatement prpstmt = null;
		PreparedStatement prpstmt1= null;
		PreparedStatement prpstmt2= null;
		ResultSet rs = null;
		ResultSet rs1=null;
		ResultSet rs2=null;
		DOMSource domSource = null;


		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.newDocument();
		Element xml_elements = doc.createElement("year_end_summary");
		doc.appendChild(xml_elements);

		try {
			try 
			{
				Class.forName("com.mysql.cj.jdbc.Driver");
				connect = DriverManager.getConnection("jdbc:mysql://db.cs.dal.ca:3306", "dharmambal", "B00824492");
				
			}
			catch (Exception e) 
			{
				System.out.println(e);
				System.exit(0);
			}
			prpstmt=connect.prepareStatement("use csci3901;");
			rs=prpstmt.executeQuery();
			prpstmt = connect.prepareStatement("select c.contactname as customer_name, c.address as street_address,c.City as city, c.region as region, c.postalcode as postal_code, c.country as country,count(o.customerid) as no_of_orders, sum(d.unitprice * d.quantity) as total_order_value from customers c join orders o on c.customerid = o.customerid join orderdetails d on o.orderid = d.orderid where o.orderdate between\""+ startdate +"\" and \""+ enddate +"\" group by c.customerid;");
			rs = prpstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int colCount = rsmd.getColumnCount();
			Element year=doc.createElement("year");
			xml_elements.appendChild(year);
			Element startDate=doc.createElement("start_date");
			startDate.appendChild(doc.createTextNode(startdate));
			year.appendChild(startDate);
			Element endDate=doc.createElement("end_date");
			endDate.appendChild(doc.createTextNode(enddate));
			year.appendChild(endDate);

			Element customerList = doc.createElement("customer_list");
			xml_elements.appendChild(customerList);

			while (rs.next()) 
			{
				Element customer = doc.createElement("customer");
				Element address = doc.createElement("address");
				
				xml_elements.appendChild(customer);
				for (int i = 1; i <= colCount; i++) 
				{
					String columnName = rsmd.getColumnLabel(i);
					Object field = rs.getObject(i);
					Element cust_elements = doc.createElement(columnName);
					if((columnName.equals("street_address"))||(columnName.equals("city"))||(columnName.equals("region"))||(columnName.equals("postal_code"))||(columnName.equals("country"))) 
					{
						cust_elements.appendChild(doc.createTextNode((field != null) ? field.toString() : ""));
						address.appendChild(cust_elements);
					} else
					{
						cust_elements.appendChild(doc.createTextNode((field != null) ? field.toString() : ""));
						customer.appendChild(cust_elements);
					}
				}
				customer.appendChild(address);
				customerList.appendChild(customer);
			}
			prpstmt2=connect.prepareStatement("select c.CategoryName as category_name, p.ProductName as product_name, s.CompanyName as supplier_name, sum(od.Quantity) as units_sold, sum(od.UnitPrice * od.Quantity) as sale_value from products p , categories c, suppliers s, orderdetails od, orders o where p.CategoryID=c.CategoryID and s.SupplierID = p.SupplierID and od.ProductID=p.ProductID and od.OrderID = o.OrderID and o.OrderDate between \""+ startdate +"\"and \""+ enddate +"\" group by c.CategoryName, p.ProductName, s.CompanyName;");
			rs2=prpstmt2.executeQuery();
			ResultSetMetaData rsm=rs2.getMetaData();
			int count=rsm.getColumnCount();
			Element productList = doc.createElement("product_list");
			xml_elements.appendChild(productList);
			while (rs2.next()) {
				Element category = doc.createElement("category");
                Element address = doc.createElement("product");
                xml_elements.appendChild(category);
				for (int i = 1; i <= count; i++) 
				{
					String columnName = rsm.getColumnLabel(i);
					Object field = rs2.getObject(i);
					Element prod_elements = doc.createElement(columnName);
					if((columnName.equals("product_name"))||(columnName.equals("supplier_name"))||(columnName.equals("units_sold"))||(columnName.equals("sale_value"))) 
					{
						prod_elements.appendChild(doc.createTextNode((field != null) ? field.toString() : ""));
						address.appendChild(prod_elements);
					} else
					{
						prod_elements.appendChild(doc.createTextNode((field != null) ? field.toString() : ""));
						category.appendChild(prod_elements);
					}
				}
				category.appendChild(address);
				productList.appendChild(category);
			}
			
			prpstmt1 = connect.prepareStatement("select distinct s.CompanyName as supplier_name, s.Address as street_address,s.City as city ,s.Region as region,s.PostalCode as postal_code, s.Country as country, sum(od.Quantity) as units_sold,sum(od.UnitPrice* od.Quantity) as sale_value from suppliers s, products p,orderdetails od, orders o where s.SupplierID=p.ProductID and p.ProductID= od.ProductID and od.OrderID=o.OrderID and o.OrderDate between\""+ startdate +"\"and \""+ enddate +"\" group by s.CompanyName, s.Address, s.City, s.Region, s.PostalCode,s.Country ;");
			rs1 = prpstmt1.executeQuery();
			ResultSetMetaData rsmd1 = rs1.getMetaData();
			int colCount1 = rsmd1.getColumnCount();
			Element supplierList = doc.createElement("supplier_list");
			xml_elements.appendChild(supplierList);
			while (rs1.next()) {
				Element supplier = doc.createElement("supplier");
                Element address = doc.createElement("address");
                xml_elements.appendChild(supplier);
				
				for (int i = 1; i <= colCount1; i++) 
				{
					String columnName = rsmd1.getColumnLabel(i);
					Object field = rs1.getObject(i);
					Element supp_elements = doc.createElement(columnName);
					if((columnName.equals("street_address"))||(columnName.equals("city"))||(columnName.equals("region"))||(columnName.equals("postal_code"))||(columnName.equals("country"))) 
					{
						supp_elements.appendChild(doc.createTextNode((field != null) ? field.toString() : ""));
						address.appendChild(supp_elements);
					} else
					{
						supp_elements.appendChild(doc.createTextNode((field != null) ? field.toString() : ""));
						supplier.appendChild(supp_elements);
					}
				}
				supplier.appendChild(address);
				supplierList.appendChild(supplier);
			}
			
			final String xmlFilePath = filepath;
			
			domSource = new DOMSource(doc);
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			StringWriter sw = new StringWriter();
			StreamResult sr = new StreamResult(sw);
			transformer.transform(domSource,sr);
            //DOMSource domSource1 = new DOMSource(doc);
            StreamResult streamResult = new StreamResult(new File(xmlFilePath));
            transformer.transform(domSource, streamResult);
            System.out.println("XML File created");

		} catch (SQLException sqlExp) {

			System.out.println("SQLExcp:" + sqlExp.toString());

		} finally {
			try {

				if (rs != null) 
				{
					rs.close();
					rs = null;
				}
				if(rs1!=null)
				{
					rs1.close();
					rs1=null;
				}
				
				if (connect != null) 
				{
					connect.close();
					connect = null;
				}
			} catch (SQLException expSQL) 
			{
				System.out
						.println("CourtroomDAO::loadCourtList:SQLExcp:CLOSING:"
								+ expSQL.toString());
			}
		}

		return doc;
	}
	}
