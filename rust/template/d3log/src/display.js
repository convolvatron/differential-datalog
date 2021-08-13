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

function push_text(obj, key, value) {
    if ((key == "text") && (obj.svg)){
        tobj = document.createElementNS(svgns, "text");
        var textNode = document.createTextNode(value)
        tobj.setAttributeNS(null,"x",obj.x);
        tobj.setAttributeNS(null,"y",obj.y);
        tobj.setAttributeNS(null,"text-anchor","middle");
        tobj.setAttributeNS(null,"font-size","24");
        obj.textobj = tobj;
        tobj.appendChild(textNode);
        svg.appendChild(tobj);
    }
}


// this takes svg, not inter
function push(obj, key, value){
    if (obj.svg) {
        kind = obj.kind;
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
            break;
        default:
            obj.svg.setAttributeNS(null,key,value)
        }
    }
}

function set(obj, k, val) {
    obj[k] = val;
    if ("obj" in obj) {
        push(obj, k, val);
        push_text(obj, k, val);        
    }
}

function create(obj) {
    if ((!obj.svg) && (obj.kind)) {
        let o = document.createElementNS(svgns, obj.kind);
        o.setAttributeNS(null, "obj", obj);
        obj.svg=o;        
        for (var key in obj){
            push(obj, key, obj[key]);
        }
        
        svg.appendChild(o);
        for (var key in obj){
            push_text(obj, key, obj[key]);
        }                                        
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
                        console.log("insert", f.u)
                        nodes[f.u] = {}
                    }
                    let obj = nodes[f.u];

                    console.log("prop", f.u, key)
                    switch(key){
                    case "display::Kind":
                        if (w > 0) {
                            set(obj, "kind", f.kind)                            
                        } else {
                            if (obj.textobj) {
                                svg.removeChild(obj.textobj);
                            }
                            if (obj.svg) {
                                svg.removeChild(obj.svg);
                            }
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
                    case "display::Opacity":
                        set(obj, "opacity", f.opacity)                    
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
            for (var k in nodes){
                console.log("digested", k)
            }
            for (var k in nodes){
                create(nodes[k]);
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
