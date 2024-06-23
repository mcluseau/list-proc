use anyhow::format_err;
use log::{error, info};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net;

pub async fn process(
    socket_path: String,
    env: String,
    cmd: String,
    args: Vec<String>,
) -> anyhow::Result<()> {
    info!("connecting to {socket_path}");
    let mut stream = net::UnixStream::connect(socket_path).await?;
    let (stream_in, stream_out) = stream.split();
    stream_out.writable().await?;

    let mut reader = BufReader::new(stream_in);

    let mut item = Vec::new();
    loop {
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

        let mut cmd = tokio::process::Command::new(cmd.clone());
        cmd.args(args.clone());

        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        cmd.env(env.clone(), OsString::from_vec(item.to_vec()));

        let result = cmd
            .status()
            .await
            .map_err(|e| format_err!("failed to run command: {e}"))?;

        if result.success() {
            info!("item {item_str:?}: {result}");
            stream_out.try_write(b"done\n")
        } else {
            error!("item {item_str:?}: {result}");
            stream_out.try_write(b"failed\n")
        }
        .map_err(|e| format_err!("failed to send result: {e}"))?;
    }
}
