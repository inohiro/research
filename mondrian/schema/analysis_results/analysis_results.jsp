<%@ page session="true" contentType="text/html; charset=ISO-8859-1" %>
<%@ taglib uri="http://www.tonbeller.com/jpivot" prefix="jp" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>

<jp:mondrianQuery id="query01" 
		  jdbcDriver="com.mysql.jdbc.Driver" 
		  jdbcUrl="jdbc:mysql://localhost/analysis_results?user=inohiro&password=password" 
		  catalogUri="/WEB-INF/queries/analysis_results.xml"
		  connectionPooling="false">

select { [Measures].[value] } on columns,
       { ( [treatment].[AllTreatment], [sample].[AllSample] ) } on rows
from   results

</jp:mondrianQuery>

<c:set var="title01" scope="session">sample pivot table of 'ra' schema</c:set>
