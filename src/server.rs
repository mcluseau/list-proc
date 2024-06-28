use anyhow::format_err;
use log::{debug, info, warn};
use std::collections::{BTreeMap as Map, BTreeSet as Set};
use std::fs;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net;
use tokio::sync::mpsc;

use crate::config::Config;

enum ClientEvent {
    Take { id: u32, item: Vec<u8> },
    Done { id: u32 },
    Failed { id: u32 },
    Closed { id: u32 },
}

#[derive(Debug)]
enum ClientStatus {
    Processing(Vec<u8>),
    Idle,
}
impl ClientStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Processing(item) => format!("{:?}", String::from_utf8_lossy(&item)),
            Self::Idle => "<idle>".to_string(),
        }
    }
}

pub async fn serve(
    socket_path: String,
    list_file: String,
    state_file: Option<String>,
    cfg: Config,
) -> anyhow::Result<()> {
    let mut done: Set<Vec<u8>> = Set::new();

    if let Some(state_file) = state_file.clone() {
        // touch state
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(state_file.clone())
            .map_err(|e| format_err!("failed to touch {state_file}: {e}"))?;

        // load state
        for item in fs::read(state_file)?.split(|b| *b == b'\n') {
            done.insert(item.to_vec());
        }
    }

    info!("listening on {socket_path}");
    let _ = std::fs::remove_file(&socket_path);
    let listener = net::UnixListener::bind(socket_path.clone())?;

    scopeguard::defer! {
        let _ = std::fs::remove_file(&socket_path);
    };

    let (item_tx, item_rx) = async_channel::unbounded();

    // we want to count so read all items and fill the queue
    let list = fs::read(list_file)?;
    let list = list
        .split(|b| *b == b'\n')
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();

    let mut counters = Counters::new(list.len());

    for item in list {
        let item = item.to_vec();

        if done.contains(&item.to_vec()) {
            counters.add_done();
        } else {
            item_tx.try_send(item.to_vec()).unwrap();
        }
    }
    item_tx.close();

    if counters.n_done != 0 {
        let (d, t) = (counters.n_done, counters.n_total);
        info!("starting with {d}/{t} items already done");
    }
    if counters.n_todo == 0 {
        return Ok(());
    }

    use tokio::signal::unix::{signal, SignalKind};
    let mut signals = signal(SignalKind::interrupt())?;
    let stop = Arc::new(AtomicBool::new(false));

    // pre-serialize the config line
    let mut cfg =
        serde_json::to_vec(&cfg).map_err(|e| format_err!("failed to serialize config: {e}"))?;
    cfg.push(b'\n');
    let cfg = Arc::new(cfg);

    let clients: Map<u32, ClientStatus> = Map::new();

    let (events_tx, mut events_rx) = mpsc::channel(8);

    let mut server = Server {
        events_tx,
        clients,
        item_rx,
        cfg,
        stop,
        state_file,
        counters,
    };

    let mut next_id = 0;
    loop {
        tokio::select!(
            _ = signals.recv() => {
                if server.signal_stop() {
                    return Ok(());
                }
            },
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;

                let id = next_id;
                next_id += 1;

                server.accept_client(id, stream);
            }
            event = events_rx.recv() => {
                if server.recv_event(event.unwrap())? {
                    return Ok(());
                }
            }
        );
    }
}

struct Server {
    events_tx: mpsc::Sender<ClientEvent>,
    clients: Map<u32, ClientStatus>,
    item_rx: async_channel::Receiver<Vec<u8>>,
    cfg: Arc<Vec<u8>>,
    stop: Arc<AtomicBool>,
    state_file: Option<String>,
    counters: Counters,
}
impl Server {
    fn signal_stop(&mut self) -> bool {
        if self.stop.load(Ordering::Relaxed) == false {
            info!("interrupted, stopping clients");
            self.stop.store(true, Ordering::Relaxed);
            if self.clients.is_empty() {
                return true;
            }
        } else {
            info!("interrupted again, exiting immediately");
            return true;
        }
        false
    }

    fn accept_client(&mut self, id: u32, stream: net::UnixStream) {
        self.clients.insert(id, ClientStatus::Idle);

        let mut stream = Stream {
            id,
            stream,
            queue: self.item_rx.clone(),
            events_tx: self.events_tx.clone(),
            cfg: self.cfg.clone(),
            stop: self.stop.clone(),
        };

        tokio::spawn(async move {
            if let Err(e) = stream.handle().await {
                warn!("client {id} failed: {e}");
            }
            stream
                .events_tx
                .send(ClientEvent::Closed { id })
                .await
                .unwrap();
        });
    }

    fn recv_event(&mut self, event: ClientEvent) -> anyhow::Result<bool> {
        match event {
            ClientEvent::Take { id, item } => {
                self.clients.insert(id, ClientStatus::Processing(item));
                self.counters.processing();
            }
            ClientEvent::Done { id } => {
                let item = self.clients.insert(id, ClientStatus::Idle).unwrap(); // can't be done if none
                let ClientStatus::Processing(mut item) = item else {
                    panic!("done when not processing!");
                };
                if let Some(state_file) = self.state_file.clone() {
                    let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open(state_file)
                        .unwrap();
                    item.push(b'\n');
                    file.write(item.as_slice())?;
                }
                self.counters.done();
            }
            ClientEvent::Failed { id } => {
                self.clients.insert(id, ClientStatus::Idle);
                self.counters.failed();
            }
            ClientEvent::Closed { id } => {
                if let Some(ClientStatus::Processing(_)) = self.clients.get(&id) {
                    self.counters.failed();
                }
                self.clients.remove(&id);
            }
        };

        info!(
            "{} | {}",
            self.counters,
            self.clients
                .values()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        );

        if self.stop.load(Ordering::Relaxed) {
            info!("all clients are stopped");
            return Ok(true);
        } else if self.counters.finished() && self.clients.is_empty() {
            info!("all items have been processed");
            return Ok(true);
        }
        Ok(false)
    }
}

struct Counters {
    n_processing: usize,
    n_done: usize,
    n_failed: usize,
    n_todo: usize,
    n_total: usize,
}
impl Counters {
    fn new(total: usize) -> Counters {
        Counters {
            n_processing: 0,
            n_done: 0,
            n_failed: 0,
            n_todo: total,
            n_total: total,
        }
    }

    fn add_done(&mut self) {
        self.n_todo -= 1;
        self.n_done += 1;
    }

    fn processing(&mut self) {
        self.n_todo -= 1;
        self.n_processing += 1;
    }
    fn done(&mut self) {
        self.n_processing -= 1;
        self.n_done += 1;
    }
    fn failed(&mut self) {
        self.n_processing -= 1;
        self.n_failed += 1;
    }

    fn finished(&self) -> bool {
        return self.n_todo == 0 && self.n_processing == 0;
    }
}

use std::fmt;
impl fmt::Display for Counters {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let p = self.n_processing;
        let d = self.n_done;
        let f = self.n_failed;
        let r = self.n_todo;
        let t = self.n_total;
        write!(
            formatter,
            "{p} processing, {d}/{t} done, {f}/{t} failed, {r}/{t} todo"
        )
    }
}

struct Stream {
    id: u32,
    stream: net::UnixStream,
    queue: async_channel::Receiver<Vec<u8>>,
    events_tx: mpsc::Sender<ClientEvent>,
    cfg: Arc<Vec<u8>>,
    stop: Arc<AtomicBool>,
}
impl Stream {
    async fn handle(&mut self) -> anyhow::Result<()> {
        let id = self.id;
        info!("{id}| new client connected");

        let (stream_in, mut stream_out) = self.stream.split();

        stream_out.write(self.cfg.as_slice()).await?;

        let mut reader = BufReader::new(stream_in);
        let mut resp = Vec::new();

        loop {
            if self.stop.load(Ordering::Relaxed) {
                debug!("{id}| stopped");
                return Ok(());
            }

            resp.clear();
            reader.read_until(b'\n', &mut resp).await?;
            match resp.as_slice() {
                b"next\n" => {}
                b"" => {
                    return Ok(());
                }
                r => {
                    info!("{id}| invalid request: {:?}", String::from_utf8_lossy(r));
                    return Ok(());
                }
            }

            let Ok(mut item) = self.queue.recv().await else {
                debug!("{id}| queue finished");
                return Ok(());
            };

            self.events_tx
                .send(ClientEvent::Take {
                    id,
                    item: item.clone(),
                })
                .await?;

            let item_str = String::from_utf8_lossy(&item.as_slice()).to_string();
            item.push(b'\n');

            debug!("{id}| sending item: {item_str:?}");

            stream_out.write(item.as_slice()).await?;

            resp.clear();
            reader.read_until(b'\n', &mut resp).await?;
            match resp.as_slice() {
                b"done\n" => {
                    debug!("{id}| item {item_str:?} done");
                    self.events_tx.send(ClientEvent::Done { id }).await?;
                }
                b"failed\n" => {
                    debug!("{id}| item {item_str:?} done");
                    self.events_tx.send(ClientEvent::Failed { id }).await?;
                }
                r => {
                    info!("{id}| invalid reply: {:?}", String::from_utf8_lossy(r));
                    return Ok(());
                }
            }
        }
    }
}
