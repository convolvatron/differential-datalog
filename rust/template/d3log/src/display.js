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
    // backpointer to intermeidate object?
    let kind = obj.getAttributeNS(null,"kind")
    if (kind == "circle") {
        if (key == "x") {
            key = "cx";
        }
        if (key == "y") {
            key = "cy";
        }        
    }
    if (kind == "line") {
        if (key == "x") {
            key = "x1";
        }
        if (key == "y") {
            key = "y1";
        }        
    }
    
    switch (key){
    case "text":
        tobj = document.createElementNS(svgns, "text");
        var textNode = document.createTextNode(value)
        // demo slime - get from intermed
        tobj.setAttributeNS(null,"x",obj.getAttributeNS(null,"cx"))
        tobj.setAttributeNS(null,"y",obj.getAttributeNS(null,"cy"))
        // delete old one on negation        
        obj.text = tobj;
        tobj.appendChild(textNode);
        svg.appendChild(tobj);
        break;
    default:
        obj.setAttributeNS(null,key,value)
    }
}

function set(obj, k, val) {
    obj[k] = val;
    if ("obj" in obj) {
        push(obj["obj"], k, val);
    }
}

function websocket(url) {  
    setTimeout(function() {
        socket = new WebSocket(url)
        
        socket.onopen = function(evt){
            clear()
        }
        
        socket.onmessage = function(event){
            var msg = JSON.parse(event.data)
            for (var key in msg) {
                let value = msg[key]
                for (const fact of value){
                    let f = fact[0];
                    let w = fact[1];

                   if (!(f.u in nodes)) {
                        nodes[f.u] = {}
                    }
                    let obj = nodes[f.u];
                    console.log(key, f)
                    switch(key){
                    case "display::Kind":
                        if (w > 0) { 
                            let o = document.createElementNS(svgns, f.kind);
                            set(obj, "kind", f.kind)
                            o.setAttributeNS(null, "kind", f.kind);
                            for (var key in obj){
                                push(o, key, obj[key]);
                            }
                            svg.appendChild(o);
                            obj.obj=o;
                        } else {
                            if (obj.obj.text) {
                                svg.removeChild(obj.obj.text);
                            }
                            svg.removeChild(obj.obj);
                        }
                        break;
                    case "display::Position":
                        set(obj, "x", f.x)
                        set(obj, "y", f.y)                        
                        break;
                    case "display::NextPosition":
                        set(obj, "x2", f.x)
                        set(obj, "y2", f.y)                        
                        break;                        
                    case "display::Color":
                        set(obj, "fill", f.color)                    
                        break;
                    case "display::Border":
                        set(obj, "stroke", f.color)                    
                        break;                        
                    case "display::Width":
                        set(obj, "stroke-width", f.width)                    
                        break;                        
                    case "display::Text":
                        set(obj, "text", f.text)                                        
	                break;
                    case "display::Radius":
                        set(obj, "r",f.r)                                        
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
