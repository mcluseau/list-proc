use anyhow::format_err;
use log::{error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net;

use crate::config::Config;

pub async fn process(socket_path: String) -> anyhow::Result<()> {
    info!("connecting to {socket_path}");
    let mut stream = net::UnixStream::connect(socket_path).await?;
    let (stream_in, mut stream_out) = stream.split();

    let stop = Arc::new(AtomicBool::new(false));
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut signals = signal(SignalKind::interrupt())?;
        let stop = stop.clone();
        tokio::spawn(async move {
            signals.recv().await.unwrap();
            info!("interrupted, stopping after current task");
            stop.store(true, Ordering::Relaxed);
            signals.recv().await.unwrap();
            info!("interrupted again, exiting immediately");
            std::process::exit(0);
        });
    }

    let mut reader = BufReader::new(stream_in);

    let mut item = Vec::new();

    reader
        .read_until(b'\n', &mut item)
        .await
        .map_err(|e| format_err!("failed to read config: {e}"))?;
    let cfg: Config =
        serde_json::from_slice(item.as_slice()).map_err(|e| format_err!("invalid config: {e}"))?;

    loop {
        if stop.load(Ordering::Relaxed) {
            info!("stopped");
            std::process::exit(0);
        }

        stream_out.write(b"next\n").await?;
        item.clear();
        if reader
            .read_until(b'\n', &mut item)
            .await
            .map_err(|e| format_err!("failed to read next item: {e}"))?
            == 0
        {
            info!("finished");
            return Ok(());
        }

        let item = &item.as_slice()[0..item.len() - 1];

        let item_str = String::from_utf8_lossy(item);
        info!("received item: {item_str:?}");

        let cfg = cfg.clone();
        let mut cmd = tokio::process::Command::new(cfg.cmd);
        cmd.args(cfg.args);

        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        cmd.env(cfg.env, OsString::from_vec(item.to_vec()));

        let result = cmd
            .status()
            .await
            .map_err(|e| format_err!("failed to run command: {e}"))?;

        let status = if result.success() {
            info!("item {item_str:?}: {result}");
            b"done\n".as_slice()
        } else {
            error!("item {item_str:?}: {result}");
            b"failed\n".as_slice()
        };
        stream_out
            .write(status)
            .await
            .map_err(|e| format_err!("failed to send result: {e}"))?;
    }
}
