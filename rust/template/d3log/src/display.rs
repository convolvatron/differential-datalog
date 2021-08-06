use crate::{
    async_error, fact, function, send_error, Batch, Evaluator, FactSet, Instance, Node, RecordSet,
    Transport,
};
use differential_datalog::record::{IntoRecord, Record};
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use hyper_websocket_lite::{server_upgrade, AsyncClient};
use rand::prelude::*;
use std::borrow::Cow;
use std::convert::Infallible;
use std::str::from_utf8;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use websocket_codec::{Message, Opcode}; //MessageCodec

#[derive(Clone)]
pub struct Display {
    instance: Arc<Instance>,
}

#[derive(Clone)]
pub struct Browser {
    eval: Evaluator,
    uuid: Node,
    s: tokio::sync::mpsc::Sender<Message>,
}

impl Transport for Browser {
    fn send(self: &Self, b: Batch) {
        let a = self.clone();
        let rb = RecordSet::from(self.eval.clone(), b.data);
        // async error
        let encoded = serde_json::to_string(&rb).expect("send json encoding error");
        tokio::spawn(async move { a.s.send(Message::text(encoded)).await });
    }
}

const JS_BATCH_HANDLER: &[u8] = include_bytes!("display.js");

async fn on_client(d: Display, stream_mut: AsyncClient) {
    let (tx, mut rx) = channel(100);

    let b = Browser {
        eval: d.instance.eval.clone(),
        uuid: random::<u128>(),
        s: tx.clone(),
    };
    d.instance.forwarder.register(b.uuid, Arc::new(b.clone()));

    d.instance.broadcast.clone().send(fact!(
        display::Browser,
        t => d.instance.eval.clone().now().into_record(),
        uuid => b.uuid.into_record()));

    let (mut stx, srx) = stream_mut.split();

    let e2 = d.instance.eval.clone();
    tokio::spawn(async move {
        loop {
            if let Some(msg) = rx.recv().await {
                async_error!(e2.clone(), stx.send(msg).await);
            }
        }
    });

    let mut crx = srx;
    let e2 = d.instance.eval.clone();
    loop {
        let (msg, next) = crx.into_future().await;

        match msg {
            Some(Ok(msg)) => match msg.opcode() {
                Opcode::Text | Opcode::Binary => {
                    // async error
                    let v = &msg.data().to_vec();
                    let s = from_utf8(v).expect("display json utf8 error");
                    println!("browser data {}", s.clone());
                    let v: RecordSet =
                        serde_json::from_str(s.clone()).expect("display json parse error");
                    d.instance
                        .eval_port
                        .clone()
                        .send(Batch::new(FactSet::Empty(), FactSet::Record(v)));
                }
                Opcode::Ping => {
                    async_error!(e2.clone(), tx.send(Message::pong(msg.into_data())).await);
                }
                Opcode::Close => {}
                Opcode::Pong => {}
            },
            Some(Err(_err)) => {
                async_error!(e2.clone(), tx.send(Message::close(None)).await);
            }
            None => {}
        }
        crx = next;
    }
}

async fn hello(d: Display, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    if req.headers().contains_key("upgrade") {
        return server_upgrade(req, move |x| on_client(d.clone(), x))
            .await
            .map_err(|_| panic!("upgrade"));
    }
    Ok(Response::new(Body::from(JS_BATCH_HANDLER)))
}

impl Display {
    pub async fn new(instance: Arc<Instance>, port: u16) {
        let d = Display {
            instance: instance.clone(),
        };

        let addr = ([0, 0, 0, 0], port).into();
        let dclone = d.clone();
        if let Ok(server) = Server::try_bind(&addr) {
            let server = server.serve(make_service_fn(move |_conn| {
                let d = dclone.clone();
                async move { Ok::<_, Infallible>(service_fn(move |req| hello(d.clone(), req))) }
            }));

            async_error!(instance.eval.clone(), server.await);
        }
    }
}
