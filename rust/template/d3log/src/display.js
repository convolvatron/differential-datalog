<meta http-equiv="Content-Type" content="text/html; charset="utf-8">
<html style="width:100%;height:100%;">
<body onload ="start()" style="width:100%;height:100%;">
<script>

var svg = false
var socket = false
var connWait = 0
var svgns = "http://www.w3.org/2000/svg"

//                       x.addEventListener("click",
//                                          function (evt) {putBatch(x.click)})

function clear() {
    if (svg != false) {
        svg.parentNode.removeChild(svg)
    }
    svg = document.createElementNS("http://www.w3.org/2000/svg","svg")
    svg.setAttributeNS(null, "width", "100%")
    svg.setAttributeNS(null, "height", "100%")
    document.body.appendChild(svg)
}

function send(item) {
    socket.send(JSON.stringify(item))
}

var nodes = {}

function push(obj, key, value){
    if (key== "text") {
        // delete old one on negation
        var textNode = document.createTextNode(val)
        obj.appendChild(textNode);
    } else {
        obj.obj.setAttributeNS(null,k,val)
    }
}

function set(obj, k, val) {
    obj[k] = val;
    if (obj.has("obj")) {
        push(obj["obj"], k, val);
    }
}

function websocket(url) {  
    setTimeout(function() {
        socket = new WebSocket(url)
        
        socket.onopen = function(evt){
            console.log("onopen");
            clear()
        }
        
        socket.onmessage = function(event){
            var msg = JSON.parse(event.data)
            for (const [key, value] of Object.entries(msg)) {
                for (const fact of value){
                    let f = fact[0];
                    if (!(f.u in nodes)) {
                        nodes[f.u] = {}
                    }
                    let obj = nodes[f.u];
                    console.log(key, fact[0]);                
                    switch(key){
                    case "display::Kind":
                        let o = document.createElementNS(svgns, f.c);
                        for (const [key, value] of obj){
                            push(o, key, value);
                        }
                        parent.appendChild(o);
                        obj.obj=o;
                        break;
                    case "display::Position":
                        set(obj, "position", val)
                        break;
                    case "display::Color":
                        set(obj, "color", val)                    
                        break;
                    case "display::Text":
                        set(obj, "text", val)                                        
	                break;
                    case "display::Radius":
                        set(obj, "r", val)                                        
                    }
                }
            }
        }
        
        socket.onclose = 
            function(evt){
	        svg.setAttributeNS(null, "fill", "grey") 
	        connWait = connWait * 2 + 1000
	        if (connWait > 5000) {
		    connWait = 5000
	        }
	        websocket(url)
	    }
    }, connWait)
}

function start() {
    terms = document.baseURI.split(':')
    terms[0] = 'ws'
    websocket(terms.join(":"))
}
</script>
</body>        
</html>
