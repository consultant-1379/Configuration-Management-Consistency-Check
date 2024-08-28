var isMarked = $("#isMarkedCheck").text().trim()
var markedNumber = $("#MarkedNumberCheck").text().trim()
var connectioncheck = $("#Connectioncheck").text().trim()
var executeenablecheck = $("#EnableExecuteButton").text().trim()
var selecteddate = $("#SelectedDate").text().trim()

var numRules = parseInt($("#numRules").text().trim())
$("[value='Create Rule'], [value='Save Rule'],[value='Check Rule Validity'],[value='Check Invalid Rules'], [value='Remove Duplicate Rules'], [value='Edit'], [value='Delete'], [value='Bulk Import Rules'], [value='Select Filtered Data'], [value='Cancel']").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"cursor": "pointer",
}); 

if(isMarked == 'None'){
	$("#DeleteRuleEnabled").hide();
	$("#DeleteRuleDisabled").show();
}else{
	$("#DeleteRuleEnabled").show();
	$("#DeleteRuleDisabled").hide();
}	

if((numRules > 0) && connectioncheck =='Connection OK' && executeenablecheck=='TRUE' && selecteddate!='1/1/1999 12:00 AM')   {
	$("#ExecuteRuleButton").show();
	$("#ExecuteRuleDisabled").hide();
} else {
	$("#ExecuteRuleButton").hide();
	$("#ExecuteRuleDisabled").show();
}

if(markedNumber == '1') {
    $("#EditRuleEnabled").show();
    $("#EditRuleDisabled").hide();
}else {
	$("#EditRuleEnabled").hide();
	$("#EditRuleDisabled").show();
}
	
$("[value='Delete '],[value='Edit ']").css({
		"background": "#fff",
		"border-radius":"4px",
		"letter-spacing": "0px",
		"border-color": "#CDCDCD"
}); 

$("[value='Execute Rules ']").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"border-color": "#CDCDCD"
});

$("[value='Execute Rules']").css({
		"background": "#0074D9",
		"border-radius":"4px",
		"letter-spacing": "1px",
		"cursor": "pointer",
		"color":"#FFFFFF",
		"padding-right":"1px"
	});
	
$(".required").css({
    "color":"#ff3300"
});

$(".Tooltip").css({
    "color":"#787877"
});



