use bytes::Bytes;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures_util::{SinkExt, StreamExt};

use crate::protocol::{self, FetchRequest, ProduceRequest, Record, Request, Response, PROTOCOL_VERSION};

pub struct Client {
    framed: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Client {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let codec = LengthDelimitedCodec::builder().length_field_length(4).new_codec();
        let mut framed = Framed::new(stream, codec);

        // handshake
        let handshake = Request::Handshake { client_version: PROTOCOL_VERSION };
        framed.send(Bytes::from(protocol::encode(&handshake)?)).await?;
        if let Some(res) = framed.next().await.transpose()? {
            match protocol::decode::<Response>(&res)? {
                Response::HandshakeOk { server_version } if server_version == PROTOCOL_VERSION => {}
                Response::HandshakeOk { server_version } => anyhow::bail!("protocol version mismatch: server {server_version} client {PROTOCOL_VERSION}"),
                other => anyhow::bail!("unexpected handshake response: {:?}", other),
            }
        } else {
            anyhow::bail!("no handshake response")
        }

        Ok(Self { framed })
    }

    pub async fn produce(&mut self, topic: &str, partition: u32, records: Vec<Record>, auth: Option<String>) -> anyhow::Result<Response> {
        let req = Request::Produce(ProduceRequest {
            topic: topic.to_string(),
            partition,
            records,
            auth,
        });
        self.send(req).await
    }

    pub async fn fetch(&mut self, topic: &str, partition: u32, offset: u64, max_bytes: u32, auth: Option<String>) -> anyhow::Result<Response> {
        let req = Request::Fetch(FetchRequest {
            topic: topic.to_string(),
            partition,
            offset,
            max_bytes,
            auth,
        });
        self.send(req).await
    }

    async fn send(&mut self, req: Request) -> anyhow::Result<Response> {
        let bytes = protocol::encode(&req)?;
        self.framed.send(Bytes::from(bytes)).await?;
        if let Some(frame) = self.framed.next().await.transpose()? {
            let resp: Response = protocol::decode(&frame)?;
            Ok(resp)
        } else {
            anyhow::bail!("connection closed")
        }
    }
}
