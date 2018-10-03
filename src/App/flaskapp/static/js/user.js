
$(document).ready(function(){
    $("#user").click(function(){
        var cik = $("#cik").val();
        var startdate = $("#startdate").val();
        var enddate = $("#enddate").val();
        var disp = "human";
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
              data.addColumn('number', 'num_of_visits');

              jsonData.forEach(function (row) {
                data.addRow([
                  row.visit_date,
                  row.num_of_visits
                  ]);
              });
              var options = {'title':'User Visits',
                              'titleTextStyle': { fontName: 'Times-Roman', postition: 'center', fontSize: '18', bold: true, italic: false },
                              'legend': 'none',
                              'hAxis': {'title': 'Date', titleTextStyle:{fontName: 'Times-Roman',fontSize: '15', bold: false, italic: false}},
                              'vAxis': {'title': 'Number of Visits', titleTextStyle:{fontName: 'Times-Roman',fontSize: '15', bold: false, italic: false}},
                              'colors': ['red'],
                             // 'width':800,
                             'height':400
                         };
              // Instantiate and draw our chart, passing in some options.
              var chart = new google.visualization.ColumnChart(document.getElementById('chart'));
              chart.draw(data, options);
            }
        });
    });
});
