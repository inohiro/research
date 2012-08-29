<%@ page session="true" contentType="text/html; charset=ISO-8859-1" %>
<%@ taglib uri="http://www.tonbeller.com/jpivot" prefix="jp" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>

<jp:mondrianQuery id="query01" 
		  jdbcDriver="com.mysql.jdbc.Driver" 
		  jdbcUrl="jdbc:mysql://localhost/leaf?user=inohiro&password=password" 
		  catalogUri="/WEB-INF/queries/leaf_analysis_results.xml"
		  connectionPooling="false">

select { [Measures].[value] } on columns,
       { ( [treatment].[AllTreatment], [sample].[AllSample], [gene].[AllGene] ) } on rows
from   results

/*
  http://stackoverflow.com/questions/894156/how-can-i-return-level-property-values-in-an-mdx-query
/*

</jp:mondrianQuery>

<c:set var="title01" scope="session">sample pivot table of 'leaf, tiling array analysis results integrated' schema</c:set>
