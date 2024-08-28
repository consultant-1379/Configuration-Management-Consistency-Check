var Excuted = $("#RulesExcuted").text().trim()


$("[value='Discrepancies Ranking'], [value='Discrepancies Statistics'], [value='CM Rules']").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"cursor": "pointer",
});

$("[value='Discrepancies'], [value='CM Rule Manager'], [value='Discrepancies ']").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"cursor": "pointer",
}); 

if(Excuted == ''){
	$("#DiscrepanciesEnabled").hide();
	$("#DiscrepanciesDisabled").show();
}else{
	$("#DiscrepanciesEnabled").show();
	$("#DiscrepanciesDisabled").hide();
}	