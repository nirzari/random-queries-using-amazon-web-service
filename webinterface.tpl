<html>
<head>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.4/jquery.min.js"></script>
<script type="text/javascript">
$( document ).ready(function() {
	$("#button").click(function(){
                console.log("Test2");
		var dataobj = {"hpi_type": $("#hpi_type").val(), "frequency": $("#frequency").val(), "yr": $("#yr").val(), "place_name":$("#place_name").val()};
    var start = new Date().getTime(); 
    			$.ajax({
				      type: "POST",
        			url: "/dynamic_query",
        			data: dataobj,
        			success: function(query_ans){ 
              var end = new Date().getTime();
              var time = end - start;
              var time = time/1000;
              var time_ans = "Time taken to execute query is:" +time+ " seconds";                                   
					    $("#result").html("<br>" + "<b>" + time_ans + "</b>" + "<br>" + query_ans);
              }			    
          });	
  });
});


window.onload = function () {
    var select = document.getElementById("yr");
    for(var i = 1975; i < 2016; i++) {
        var option = document.createElement('option');
        option.text  = i;
        select.add(option, 0);
    }
};
</script>
</head>
<body>
<h1>Welcome to Amazon DynamoDB</h1>
Select hpi_type:  &nbsp <select name = "hpi_type" id = "hpi_type">
  <option selected = "Selected" value ="">None</option>
  <option value= "traditional">traditional</option>
  <option value= "non-metro">non-metro</option>
  <option value = "distress-free">distress-free</option>
  <option value = "developmental">developmental</option>
  </select>
</br>
Select frequency: <select name = "frequency" id = "frequency">
  <option selected = "Selected" value ="">None</option>
  <option value= "monthly">monthly</option>
  <option value= "quarterly">quarterly</option>
  </select>
</br>  
Select yr: &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp<select id="yr" name="yr"></select>
</br>
Enter Place: &nbsp &nbsp &nbsp &nbsp <input type= "Text" name = "place_name" id = "place_name">
</br>
<input type="button" id="button" value="Submit">
<div id = "result">
</div>
</body>
</html>
