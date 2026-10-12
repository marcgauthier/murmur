//! Byte leases bound buffered frames independently of entry counts.
use crate::*;
use crate::protocol::Message;
use tokio::sync::{mpsc,Semaphore,OwnedSemaphorePermit};
pub struct Queued {pub message:Message,_global:OwnedSemaphorePermit,_peer:OwnedSemaphorePermit}
#[derive(Clone)]
pub struct Sender {send:mpsc::Sender<Queued>,global:Arc<Semaphore>,peer:Arc<Semaphore>,maximum:usize}
impl Sender {
    pub fn new(global:Arc<Semaphore>,maximum:usize)->(Self,mpsc::Receiver<Queued>) {let(send,recv)=mpsc::channel(128);(Self{send,global,peer:Arc::new(Semaphore::new(maximum)),maximum},recv)}
    fn size(&self,message:&Message)->Result<u32>{let bytes=encode(message)?.len()+12;if bytes>self.maximum || bytes>u32::MAX as usize {return Err(Error::Overloaded);}Ok(bytes as u32)}
    pub fn try_send(&self,message:Message)->Result<()> {let size=self.size(&message)?;let global=self.global.clone().try_acquire_many_owned(size).map_err(|_|Error::Overloaded)?;let peer=self.peer.clone().try_acquire_many_owned(size).map_err(|_|Error::Overloaded)?;self.send.try_send(Queued{message,_global:global,_peer:peer}).map_err(|_|Error::Overloaded)}
    pub async fn send(&self,message:Message)->Result<()> {let size=self.size(&message)?;let global=self.global.clone().acquire_many_owned(size).await.map_err(|_|Error::Unavailable("queue closed".into()))?;let peer=self.peer.clone().acquire_many_owned(size).await.map_err(|_|Error::Unavailable("queue closed".into()))?;self.send.send(Queued{message,_global:global,_peer:peer}).await.map_err(|_|Error::Unavailable("queue closed".into()))}
}
