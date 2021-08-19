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

use tokio::{io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener, net::TcpStream, sync::Mutex};

use crate::{
    async_error, async_expect_some, fact, function, json_framer::JsonFramer, recfact, send_error,
    Batch, Dred, Error, Evaluator, FactSet, Instance, RecordSet, Transport,
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
        for (_r, v, _w) in &RecordSet::from(self.instance.eval.clone(), b.data) {
            let destination =
                async_expect_some!(self.instance.eval, v.get_struct_field("destination"));
            let location = async_expect_some!(self.instance.eval, v.get_struct_field("location"));
            match destination {
                // what are the unused fields?
                Record::String(string) => {
                    let address = string.parse();
                    let loc: u128 = async_error!(
                        self.instance.eval.clone(),
                        FromRecord::from_record(location)
                    );
                    // we add an entry to forward this nid to this tcp address
                    let peer = Arc::new(TcpPeer {
                        instance: self.instance.clone(),
                        tcp_inner: Arc::new(Mutex::new(TcpPeerInternal {
                            eval: self.instance.eval.clone(),
                            stream: None,
                            address: address.unwrap(), //async error
                                                       // sends: Vec::new(),
                        })),
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

// associated? with?
pub fn tcp_bind(instance: Arc<Instance>) -> Result<(), Error> {
    instance.dispatch.register(
        "d3_application::TcpAddress",
        Arc::new(AddressListener {
            instance: instance.clone(),
        }),
    )?;

    let i2 = instance.clone();
    instance.rt.spawn(async move {
        // xxx get external  ip address
        let listener = async_error!(i2.eval.clone(), TcpListener::bind("127.0.0.1:0").await);
        let addr = listener.local_addr().unwrap();

        i2.broadcast.send(fact!(
            d3_application::TcpAddress,
            location => i2.uuid.into_record(),
            destination => addr.to_string().into_record()));

        let i3 = i2.clone();
        loop {
            // exchange ids..we dont need the initiators identity because this is all feed forward, but
            // it helps track whats going on
            let (socket, _a) = async_error!(i3.eval.clone(), listener.accept().await);
            TcpInput::new(socket, i3.clone());
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
    instance: Arc<Instance>,
}

struct TcpInput {
    instance: Arc<Instance>,
    dred: Dred,
    record: RecordSet,
}

// pass the output port down?
impl TcpInput {
    fn new(socket: TcpStream, instance: Arc<Instance>) -> Arc<TcpInput> {
        let (dred, dred_port) = Dred::new(instance.eval.clone(), instance.under.clone());
        let f = recfact!(
            d3_application::ConnectionStatus,
            time => instance.eval.now().into_record(),
            me => instance.uuid.into_record(),
            them => instance.uuid.into_record());

        let ti = Arc::new(TcpInput {
            dred,
            instance: instance.clone(),
            record: f,
        });

        instance.clone().broadcast.clone().send(Batch::new(
            FactSet::Empty(),
            FactSet::Record(ti.record.clone()),
        ));

        let sclone = Arc::new(Mutex::new(socket));
        let t2 = ti.clone();
        instance.rt.spawn(async move {
            let mut jf = JsonFramer::new();
            let mut buffer = [0; 64];
            loop {
                match sclone.lock().await.read(&mut buffer).await {
                    Ok(bytes_input) => {
                        for i in jf
                            .append(&buffer[0..bytes_input])
                            .expect("json coding error")
                        {
                            let b = async_error!(
                                t2.clone().instance.eval.clone(),
                                Batch::deserialize(i.clone())
                            );
                            dred_port.send(b);
                        }
                        if bytes_input == 0 {
                            t2.clone().shutdown();
                            break;
                        }
                    }
                    Err(_) => {
                        t2.clone().shutdown();
                        break;
                    }
                };
            }
        });
        ti
    }

    fn shutdown(&self) {
        self.dred.close(); // with_metadata for the discrete version
        self.instance.broadcast.send(Batch::new(
            FactSet::Empty(),
            FactSet::Record(self.record.clone().negate()),
        ));
    }
}

impl Transport for TcpPeer {
    fn send(&self, b: Batch) {
        let tcp_inner_clone = self.tcp_inner.clone();
        // xxx - do these join handles need to be collected and waited upon for
        // resource recovery?
        self.instance.rt.spawn(async move {
            let mut tcp_peer = tcp_inner_clone.lock().await;

            if tcp_peer.stream.is_none() {
                // xxx use async_error
                tcp_peer.stream = match TcpStream::connect(tcp_peer.address).await {
                    Ok(x) => Some(Arc::new(Mutex::new(x))),
                    Err(_x) => panic!("connection failure {}", tcp_peer.address),
                };
            };

            let eval = tcp_peer.eval.clone();
            // no vals today, check back tomorrow
            let b = Batch::new(
                FactSet::Record(RecordSet::from(eval.clone(), b.clone().meta)),
                FactSet::Record(RecordSet::from(eval.clone(), b.clone().data)),
            );
            let bytes = async_error!(eval.clone(), b.clone().serialize());

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
