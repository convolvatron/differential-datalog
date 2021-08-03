// provide a tcp implementation of Transport
// depends on a relational table in d3_application.dl - TcpAddress(x: D3logLocationId, destination: string)
// to provide location id to tcp address mapping
//
// do not currently attempt to use a single duplex connection between two nodes, instead
// running one in each direction
//
// there is a likely race when a second send comes in before the connection attempt started
// by the first has completed, resulting in a dead connection
//
// put a queue on the TcpPeer to allow for misordering wrt TcpAddress and to cover setup

use tokio::{
    io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener, net::TcpStream, runtime::Runtime,
    sync::Mutex,
};

use crate::{
    async_error, async_expect_some, fact, function, json_framer::JsonFramer, nega_fact, send_error,
    Batch, DDValueBatch, Dred, Error, Evaluator, Factset, Instance, Port, RecordBatch, Transport,
};

use differential_datalog::record::*;
use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;

struct AddressListener {
    instance: Arc<Instance>,
}

impl Transport for AddressListener {
    fn send(&self, b: Batch) {
        for (_r, v, _w) in &RecordBatch::from(self.instance.eval.clone(), b) {
            let destination =
                async_expect_some!(self.instance.eval, v.get_struct_field("destination"));
            let location = async_expect_some!(self.instance.eval, v.get_struct_field("location"));
            match destination {
                // what are the unused fields?
                Record::String(string) => {
                    let address = string.parse();
                    let loc: u128 = async_error!(
                        self.instance.eval.clone(),
                        FromRecord::from_record(&location)
                    );
                    // we add an entry to forward this nid to this tcp address
                    let peer = Arc::new(TcpPeer {
                        management: self.instance.broadcast.clone(),
                        eval: self.instance.eval.clone(),
                        tcp_inner: Arc::new(Mutex::new(TcpPeerInternal {
                            eval: self.instance.eval.clone(),
                            stream: None,
                            address: address.unwrap(), //async error
                                                       // sends: Vec::new(),
                        })),
                        rt: self.instance.rt.clone(),
                    });
                    self.instance.forwarder.register(loc, peer);
                }
                _ => async_error!(
                    self.instance.eval.clone(),
                    Err(Error::new(format!(
                        "bad tcp address {}",
                        destination.to_string()
                    )))
                ),
            }
        }
    }
}

// associated? to?
pub fn tcp_bind(instance: Arc<Instance>) -> Result<(), Error> {
    instance.dispatch.register(
        "d3_application::TcpAddress",
        Arc::new(AddressListener {
            instance: instance.clone(),
        }),
    )?;

    let clone = instance.clone();
    instance.rt.spawn(async move {
        // xxx get external  ip address
        let listener = async_error!(clone.eval.clone(), TcpListener::bind("127.0.0.1:0").await);
        let addr = listener.local_addr().unwrap();

        clone.broadcast.send(fact!(
                d3_application::TcpAddress,
                location => clone.uuid.into_record(),
                destination => addr.to_string().into_record()));

        let clone = clone.clone();
        loop {
            // exchange ids..we dont need the initiators identity because this is all feed forward, but
            // it helps track whats going on

            let (socket, _a) = async_error!(clone.eval.clone(), listener.accept().await);

            clone.clone().broadcast.clone().send(fact!(
                    d3_application::ConnectionStatus,
                    time => clone.eval.now().into_record(),
                    me => clone.uuid.into_record(),
                    them => clone.uuid.into_record()));

            // well, this is a huge .. something. if i just use the socket
            // in the async move block, it actually gets dropped
            let sclone = Arc::new(Mutex::new(socket));

            let (dred, dred_port) = Dred::new(clone.clone().eval.clone(), clone.eval_port.clone());

            let clone2 = clone.clone();
            clone.rt.spawn(async move {
                let mut jf = JsonFramer::new();
                let mut buffer = [0; 64];
                loop {
                    // xxx - remove socket from peer table on error and post notification
                    match sclone.lock().await.read(&mut buffer).await {
                        Ok(bytes_input) => {
                            for i in jf
                                .append(&buffer[0..bytes_input])
                                .expect("json coding error")
                            {
                                dred_port.send(Batch::new(
                                    Factset::Empty(),
                                    Factset::Value(async_error!(
                                        clone2.eval.clone(),
                                        clone2.eval.clone().deserialize_batch(i)
                                    )),
                                ));
                            }
                        }
                        Err(_) => {
                            // call Dred close to retract all the facts
                            dred.close();
                            // Retract the connection status fact too!
                            // good! maybe we can just keep a copy of the original assertion?
                            clone2.broadcast.send(nega_fact!(
                                    d3_application::ConnectionStatus,
                                    time => clone2.clone().eval.clone().now().into_record(),
                                    me => clone2.clone().uuid.into_record(),
                                    them => clone2.clone().uuid.into_record()));
                        }
                    }
                }
            });
        }
    });
    Ok(())
}

struct TcpPeerInternal {
    address: SocketAddr,
    stream: Option<Arc<Mutex<TcpStream>>>,
    eval: Evaluator,
}

#[derive(Clone)]
struct TcpPeer {
    tcp_inner: Arc<Mutex<TcpPeerInternal>>,
    eval: Evaluator,
    management: Port,
    rt: Arc<Runtime>,
}

impl Transport for TcpPeer {
    fn send(&self, b: Batch) {
        let tcp_inner_clone = self.tcp_inner.clone();
        // xxx - do these join handles need to be collected and waited upon for
        // resource recovery?
        self.rt.spawn(async move {
            let mut tcp_peer = tcp_inner_clone.lock().await;

            if tcp_peer.stream.is_none() {
                // xxx use async_error
                tcp_peer.stream = match TcpStream::connect(tcp_peer.address).await {
                    Ok(x) => Some(Arc::new(Mutex::new(x))),
                    Err(_x) => panic!("connection failure {}", tcp_peer.address),
                };
            };

            let eval = tcp_peer.eval.clone();
            let ddval_batch = async_error!(eval.clone(), DDValueBatch::from(&(*eval), b));
            let bytes = async_error!(eval.clone(), eval.clone().serialize_batch(ddval_batch));

            async_error!(
                eval.clone(),
                tcp_peer
                    .stream
                    .as_ref()
                    .unwrap()
                    .lock()
                    .await
                    .write_all(&bytes)
                    .await
            );
        });
    }
}
