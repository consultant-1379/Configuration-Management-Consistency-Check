MutationObserver = window.MutationObserver || window.WebKitMutationObserver;
//function when value changes
var changeUI = function(){
	var createRuleError = $("#createRuleError").first().text().trim()
	if (createRuleError == "") {	
		$("#saveRuleButton").show();
		$("#saveRuleButtonDisabled").hide();
	} else{
		$("#saveRuleButton").hide();
		$("#saveRuleButtonDisabled").show();
	}			
	
}

var target = document.getElementById("createRuleError");

var callback = function(mutations) {
    changeUI()
}

var observer = new MutationObserver(callback);
var opts = {
    childList: true, 
    attributes: true, 
    characterData: true, 
    subtree: true
}

observer.observe(target,opts);

changeUI()

