/// debug server wrapper
use tide::Endpoint;

use common::result::{io_result, Result};

#[cfg(test)]
pub struct DebugServer {
    server: tide::Server<()>,
    address: String,
    port: u16,
}

impl DebugServer {
    #[cfg(test)]
    pub fn new(address: String, port: u16) -> Self {
        Self {
            server: Default::default(),
            address,
            port,
        }
    }

    pub fn register<H: Endpoint<()>>(&mut self, url: String, handle: H) -> Result<()> {
        self.server.at(url.as_str()).post(handle);
        Ok(())
    }

    pub async fn run(self) -> Result<()> {
        let listen_addr = format!("{}:{}", self.address, self.port);
        let r_l = self.server.listen(listen_addr).await;
        io_result(r_l)?;
        Ok(())
    }
}
