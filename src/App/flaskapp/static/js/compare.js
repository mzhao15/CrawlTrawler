
$(document).ready(function(){
    $("#compare").click(function(){
        var cik = $("#cik").val();
        var startdate = $("#startdate").val();
        var enddate = $("#enddate").val();
        var disp = "compare";
        if (startdate < '2016-01-01') {
            alert('choose a starting date later than 2016-01-01');
        }
        else if (enddate >'2016-12-31') {
            alert('choose an ending date before 2016-12-31');
        }
        else if (startdate > enddate) {
            alert('ending date should be later than start date');
        }
        else{
            $.getJSON("/getdata", { 'cik': cik, 'start_date': startdate, 'end_date': enddate, 'disp_name': disp })
            .done(function (jsonData){
                // console.log(jsonData)
                if (jsonData.length == 0 ){
                    alert('No data about this company found. Try another CIK.');
                }
                else{
                    // Load the Visualization API and the package.
                    google.charts.load('current', {'packages':['corechart']});
                    // Set a callback to run when the Google Visualization API is loaded.
                    google.charts.setOnLoadCallback(drawJson(jsonData));
                    function drawJson(jsonData) {
                      var data = new google.visualization.DataTable();
                      data.addColumn('string', 'visit_date');
                      data.addColumn('number', 'human_visits');
                      data.addColumn('number', 'robot_visits');

                      jsonData.forEach(function (row) {
                        data.addRow([
                          row.visit_date,
                          row.human_visits,
                          row.robot_visits
                          ]);
                      });
                      var options = {'title': 'Comparison',
                                      'titleTextStyle': { fontName: 'Times-Roman', postition: 'center', fontSize: '18', bold: true, italic: false },
                                      'legend': { position: 'top', alignment: 'end' },
                                      'hAxis': {'title': 'Date', titleTextStyle:{fontName: 'Times-Roman',fontSize: '15', bold: false, italic: false}},
                                      'vAxis': {'title': 'Number of Visits', titleTextStyle:{fontName: 'Times-Roman',fontSize: '15', bold: false, italic: false}},
                                     // 'width':800,
                                     'height': 400,
                                     'isStacked': true,
                                     'seriesType': 'bars',
                                     'colors':['blue','red']
                                 };
                      // Instantiate and draw our chart, passing in some options.
                      var chart = new google.visualization.ComboChart(document.getElementById('chart'));
                      chart.draw(data, options);
                    }
                }
            })
            .fail(function(){
                console.log( "error" );
            });
        }
    });
});
