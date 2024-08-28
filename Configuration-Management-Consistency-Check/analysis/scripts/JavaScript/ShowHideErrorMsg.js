MutationObserver = window.MutationObserver || window.WebKitMutationObserver;

var changeUI = function(){
	var ruleName = $("#ruleName").first().text().trim()
	if (ruleName == "") {	
		$("#ruleNameError").show();
	} else{
		$("#ruleNameError").hide();
	}			

    var moClassName = $("#moClassName").first().text().trim()
	if (moClassName == "") {	
		$("#moClassNameError").show();
	} else{
		$("#moClassNameError").hide();
	}

    var attributeName = $("#attributeName").first().text().trim()
	if (attributeName == "") {	
		$("#attributeNameError").show();
	} else{
		$("#attributeNameError").hide();
	}

    var value = $("#value").first().text().trim()
	if (value == "") {	
		$("#valueError").show();
	} else{
		$("#valueError").hide();
	}
	
}

var target = document.getElementById("ruleName");
var target = document.getElementById("moClassName")
var target = document.getElementById("attributeName")
var target = document.getElementById("value")

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
