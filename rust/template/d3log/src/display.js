<meta http-equiv="Content-Type" content="text/html; charset="utf-8">
<html style="width:100%;height:100%;">
<body onload ="start()" style="width:100%;height:100%;">
<script>

var svg = false
var socket = false
var connWait = 0
var svgns = "http://www.w3.org/2000/svg"

function putBatch(k) {
}

function update(obj, prop, value, weight) {
    if (prop in obj) {
        weight += obj[prop][1]
    }
    console.log("update", obj, prop, value, weight);
    obj[prop] = [value, weight];

    if (svg in obj) {
        if (weight > 0) {
            push(obj, k, val);
        }
        // text is drawn as a separate pass w/ the associated object for stacking
        if (weight > 0) {        
            push_text(obj);
        }
    }    
}

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

function push_text(obj) {
    if ("text" in obj && obj.svg) {
        t = obj.text;
        if (t[1] > 0) {
            tobj = document.createElementNS(svgns, "text");
            var textNode = document.createTextNode(t[0])
            // position multiplicity
            tobj.setAttributeNS(null,"x",obj.x[0]);
            tobj.setAttributeNS(null,"y",obj.y[0]);
            tobj.setAttributeNS(null,"text-anchor","middle");
            tobj.setAttributeNS(null,"dominant-baseline","middle");        
            tobj.setAttributeNS(null,"font-size","24");
            obj.textobj = tobj;
            tobj.appendChild(textNode);
            svg.appendChild(tobj);
        } else {
            if (textobj in obj) {
                svg.removeChild(obj.textobj);
            }
        }
    }
}

function remap(m, k) {
    return (k in m) ? m[k] : k 
}

function push(obj, key){
    if (obj.svg) {
        o = obj[key]
        kind = obj.kind[0];

        
        if (kind == "circle") {
            key = remap({"x":"cx", "y":"cy"},key)
        }
        
        if (kind == "line") {
            key = remap({"x":"x1", "y":"x2"},key)
        }
        
        if (o[1] > 0) {
            switch (key){
            case "text":
                break;
            default:
                obj.svg.setAttributeNS(null,key,o[0], kind)
            }
        }
    }
}


function create(obj) {
    if ("kind" in obj) {
        k = obj.kind;
        if ((!obj.svg) && (k[1] > 0)) {
            let o = document.createElementNS(svgns, k[0]);
            // svg doesn't seem to be upset if we put junk in here
            o.setAttributeNS(null, "obj", obj);
            o.addEventListener("click",
                               function (evt) {putBatch(x.click)})
        
            obj.svg=o;        
            for (var key in obj){
                push(obj, key)
            }
            svg.appendChild(o);
            // stacking order - i suppose if we know this is a graph
            // the line segments can be under
            push_text(obj);
        }
        if ((obj in svg) && (k[1] <= 0)) {
            svg.removeChild(obj.svg);        
            delete obj.svg
        }
    }
}

function websocket(url) {  
    setTimeout(function() {
        socket = new WebSocket(url)
        
        socket.onopen = function(evt){
            connWait = 0
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

                    switch(key){
                    case "display::Kind":
                        update(obj, "kind", f.kind, w)
                        break;
                    case "display::Position":
                        update(obj, "x", f.x, w)
                        update(obj, "y", f.y, w)
                        break;
                    case "display::NextPosition":
                        update(obj, "x2", f.x, w)
                        update(obj, "y2", f.y, w)
                        break;                        
                    case "display::Color":
                        update(obj, "fill", f.color, w) 
                        break;
                    case "display::Border":
                        update(obj, "stroke", f.color, w)                    
                        break;
                    case "display::Opacity":
                        update(obj, "opacity", f.opacity, w)                    
                        break;                                                
                    case "display::Width":
                        update(obj, "stroke-width", f.width, w)                    
                        break;                        
                    case "display::Text":
                        update(obj, "text", f.text, w)                                        
	                break;
                    case "display::Radius":
                        update(obj, "r", f.r, w)                                        
                    }
                }
            }
            for (var k in nodes){
                n = nodes[k]
                if (("x" in n) && ("y" in n) && ("kind" in n) && !("svg" in n))
                    create(n)
            }
        }
        
        socket.onclose = 
            function(evt){
	        svg.setAttributeNS(null, "fill", "grey") 
	        connWait = connWait * 2 + 100
	        if (connWait > 2000) {
		    connWait = 2000
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
