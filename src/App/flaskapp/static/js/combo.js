
$(document).ready(function(){
    $("#combo").click(function(){
        var cik = $("#cik").val();
        var startdate = $("#startdate").val();
        var enddate = $("#enddate").val();
        var disp = "combo";
        $.getJSON("/getdata", { 'cik': cik, 'start_date': startdate, 'end_date': enddate, 'disp_name': disp })
        .done(function (jsonData){
            console.log(jsonData)
            // Load the Visualization API and the package.
            google.charts.load('current', {'packages':['corechart']});
            // Set a callback to run when the Google Visualization API is loaded.
            google.charts.setOnLoadCallback(drawJson(jsonData));
            function drawJson(jsonData) {
              var data = new google.visualization.DataTable();
              data.addColumn('string', 'visit_date');
              data.addColumn('number', 'total_visits');
              data.addColumn('number', 'human_visits');

              jsonData.forEach(function (row) {
                data.addRow([
                  row.visit_date,
                  row.total_visits,
                  row.human_visits
                  ]);
              });
              var options = {'title': 'Comparison',
                             // 'width':800,
                             'height': 400,
                             'seriesType': 'bars'
                         };
              // Instantiate and draw our chart, passing in some options.
              var chart = new google.visualization.ComboChart(document.getElementById('chart'));
              chart.draw(data, options);
            }
        });
    });
});
