use anyhow::format_err;
use log::{debug, info, warn};
use std::collections::{BTreeMap as Map, BTreeSet as Set};
use std::fs;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net;
use tokio::sync::mpsc;

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

    let n_todo = list.len();
    let mut n_done = 0;

    for item in list {
        let item = item.to_vec();

        if done.contains(&item.to_vec()) {
            n_done += 1;
        } else {
            item_tx.try_send(item.to_vec()).unwrap();
        }
    }
    item_tx.close();

    if n_done != 0 {
        info!("starting with {n_done}/{n_todo} items already done");
    }

    use tokio::signal::unix::{signal, SignalKind};
    let mut signals = signal(SignalKind::interrupt())?;
    let stop = Arc::new(AtomicBool::new(false));

    let mut clients: Map<u32, ClientStatus> = Map::new();
    let mut n_failed = 0;

    let (events_tx, mut events_rx) = mpsc::channel(8);

    let mut next_id = 0;
    loop {
        tokio::select!(
            _ = signals.recv() => {
                if stop.load(Ordering::Relaxed) == false {
                    info!("interrupted, stopping clients");
                    stop.store(true, Ordering::Relaxed);
                    if clients.is_empty() {
                        return Ok(());
                    }
                } else {
                    info!("interrupted again, exiting immediately");
                    return Ok(());
                }
            },
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;

                let id = next_id;
                next_id += 1;

                clients.insert(id, ClientStatus::Idle);

                let item_rx = item_rx.clone();
                let events_tx = events_tx.clone();
                let stop = stop.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_stream(id, stream, item_rx, &events_tx, stop).await {
                        warn!("client {id} failed: {e}");
                    }
                    events_tx.send(ClientEvent::Closed{id}).await.unwrap();
                });
            }
            event = events_rx.recv() => {
                match event.unwrap() {
                    ClientEvent::Take{id, item} => {
                        clients.insert(id, ClientStatus::Processing(item));
                    },
                    ClientEvent::Done{id} => {
                        let item = clients.insert(id, ClientStatus::Idle).unwrap(); // can't be done if none
                        let ClientStatus::Processing(mut item) = item else {
                            panic!("done when not processing!");
                        };
                        if let Some(state_file) = state_file.clone() {
                            let mut file = fs::OpenOptions::new().append(true).open(state_file).unwrap();
                            item.push(b'\n');
                            file.write(item.as_slice())?;
                        }
                        n_done += 1;
                    },
                    ClientEvent::Failed{id} => {
                        clients.insert(id, ClientStatus::Idle);
                        n_failed += 1;
                    },
                    ClientEvent::Closed{id} => {
                        if let Some(ClientStatus::Processing(_)) = clients.get(&id) {
                            n_failed += 1;
                        }
                        clients.remove(&id);
                    },
                };

                info!("{n_done} done, {n_failed} failed, {n_todo} total | {}", clients.values().map(|v| v.to_string()).collect::<Vec<_>>().join(" "));

                if stop.load(Ordering::Relaxed) {
                    info!("all clients are stopped");
                    return Ok(());
                } else if n_done+n_failed == n_todo && clients.is_empty() {
                    info!("all items have been processed");
                    return Ok(());
                }
            }
        );
    }
}

async fn handle_stream(
    id: u32,
    mut stream: net::UnixStream,
    queue: async_channel::Receiver<Vec<u8>>,
    events_tx: &mpsc::Sender<ClientEvent>,
    stop: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    info!("{id}| new client connected");

    let (stream_in, stream_out) = stream.split();

    let mut reader = BufReader::new(stream_in);
    let mut resp = Vec::new();

    loop {
        if stop.load(Ordering::Relaxed) {
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

        let Ok(mut item) = queue.recv().await else {
            debug!("{id}| queue finished");
            return Ok(());
        };

        events_tx
            .send(ClientEvent::Take {
                id,
                item: item.clone(),
            })
            .await?;

        let item_str = String::from_utf8_lossy(&item.as_slice()).to_string();
        item.push(b'\n');

        debug!("{id}| sending item: {item_str:?}");

        stream_out.writable().await?;
        stream_out.try_write(item.as_slice())?;

        resp.clear();
        reader.read_until(b'\n', &mut resp).await?;
        match resp.as_slice() {
            b"done\n" => {
                debug!("{id}| item {item_str:?} done");
                events_tx.send(ClientEvent::Done { id }).await?;
            }
            b"failed\n" => {
                debug!("{id}| item {item_str:?} done");
                events_tx.send(ClientEvent::Failed { id }).await?;
            }
            r => {
                info!("{id}| invalid reply: {:?}", String::from_utf8_lossy(r));
                return Ok(());
            }
        }
    }
}
