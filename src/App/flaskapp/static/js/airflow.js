$(document).ready(function(){
    $("#dagdate").click(function(){
        $("#output").val("");
    });
    $("#taskname").click(function(){
        $("#output").val("");
    });
    $("#submit").click(function(){
        $("#output").val("");
        var dagdate = $("#dagdate").val();
        var taskname = $("#taskname").val();
        console.log(taskname);

        if (dagdate < '2016-01-01') {
            alert('choose a starting date later than 2016-01-01');
        }
        else if (dagdate >'2016-12-31') {
            alert('choose an ending date before 2016-12-31');
        }
        else {
            $.getJSON("/getairflow", { 'dagdate': dagdate, 'taskname': taskname })
            .done( function(jsonData) {
                $("#output").val(jsonData.status);
            })
            .fail( function() {
                console.log( "error" );
            });
        }
    });
});
