var ckhURL = "http://work:8123/?user=pgmetrics&password=pgmetr&database=pgdb";
function showload(){
	document.getElementById("modalrg").innerHTML='<div id="spinner" class="modal"><div class="modal-content" align="center"><span class="sbl-circ"></span></div></div>';
	document.getElementById("spinner").style.display="block";
}

function hideload(){
	if(document.getElementById("modalrg").innerHTML.length > 0 ){
		document.getElementById("spinner").style.display="none";
		document.getElementById("modalrg").innerHTML='';					
	}
}     

function getDatefromSnapTime( ts ){
	var out = new Date(
		ts.substring(0,4), 
		Number(ts.substring(5,7))-1, 
		ts.substring(8,10), 
		ts.substring(11,13), 
		ts.substring(14,16), 
		ts.substring(17,20), 
		0
	).getTime();
	return out;
}

function getData(query) {
	var xmlHttp = new XMLHttpRequest();
	var xmlHttpOutput;
	xmlHttp.open("POST", ckhURL, false); // false for synchronous request
	xmlHttp.send(query);
	return xmlHttp.responseText;
}

function a_getData(query,execfunc) {
	var xmlHttp = new XMLHttpRequest();
	var xmlHttpOutput;
	xmlHttp.open("POST", ckhURL, true);
	xmlHttp.send(query);
	xmlHttp.onreadystatechange = function() { 
		if (xmlHttp.readyState != 4) {return};
		if (xmlHttp.status != 200) {return;} 
		else {
			execfunc(xmlHttp.responseText);
		}
	}                
}      

function unflatten(arr) {
    var tree = [],  mappedArr = {}, arrElem, mappedElem;
    // First map the nodes of the array to an object -> create a hash table.
    for (var i = 0, len = arr.length; i < len; i++) {
        arrElem = arr[i];
        mappedArr[arrElem.id] = arrElem;
        mappedArr[arrElem.id]['children'] = [];
    }
    for (var id in mappedArr) {
        if (mappedArr.hasOwnProperty(id)) {
            mappedElem = mappedArr[id];
            // If the element is not at the root level, add it to its parent array of children.
            if (mappedElem.parentid) {
				try{
                        mappedArr[mappedElem.parentid]['children'].push(
                                { "id": mappedElem.id, "parentid": mappedElem.parentid, "children": mappedElem.children }
                        );
                    tree.push(mappedElem);
				}catch{}
            }
            // If the element is at the root level, add it to first level elements array.
            else {
                tree.push(mappedElem);
            }
        }
    }
    return tree;
}
