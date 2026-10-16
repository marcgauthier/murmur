//! Byte leases with a reserved control lane. A session drains independently of producers.
use crate::*;
use crate::protocol::Message;
use tokio::sync::{mpsc,Semaphore,OwnedSemaphorePermit};
use std::sync::atomic::{AtomicUsize,Ordering};
static NEXT_ID:AtomicUsize=AtomicUsize::new(1);
pub struct Queued {pub message:Message,pub(crate) prepared:Option<Arc<crate::protocol::Prepared>>,_global:OwnedSemaphorePermit,_peer:OwnedSemaphorePermit}
#[derive(Clone)]
pub struct Sender {send:mpsc::Sender<Queued>,control:mpsc::Sender<Queued>,global:Arc<Semaphore>,control_global:Arc<Semaphore>,peer:Arc<Semaphore>,control_peer:Arc<Semaphore>,maximum:usize,id:usize}
pub struct Receiver {data:mpsc::Receiver<Queued>,control:mpsc::Receiver<Queued>}
impl Receiver {
    pub async fn recv(&mut self)->Option<Queued> {tokio::select! {biased; q=self.control.recv()=>q,q=self.data.recv()=>q}}
    #[cfg(test)]
    pub fn try_recv(&mut self)->std::result::Result<Queued,mpsc::error::TryRecvError> {self.control.try_recv().or_else(|_|self.data.try_recv())}
}
impl Sender {
    #[cfg(test)]
    pub fn new(global:Arc<Semaphore>,maximum:usize)->(Self,Receiver) {Self::with_control(global,Arc::new(Semaphore::new(64<<10)),maximum)}
    pub fn with_control(global:Arc<Semaphore>,control_global:Arc<Semaphore>,maximum:usize)->(Self,Receiver) {
        let(send,data)=mpsc::channel(128);let(control,control_recv)=mpsc::channel(128);
        (Self{send,control,global,control_global,peer:Arc::new(Semaphore::new(maximum-(64<<10))),control_peer:Arc::new(Semaphore::new(64<<10)),maximum,id:NEXT_ID.fetch_add(1,Ordering::Relaxed)},Receiver{data,control:control_recv})
    }
    pub fn available(&self)->usize {self.peer.available_permits()}
    pub fn id(&self)->usize {self.id}
    fn control(message:&Message)->bool {!matches!(message,Message::Batches(_)|Message::PlumtreeData(_)|Message::TransactionChunk(_)|Message::SnapshotChunk{..})}
    fn size(&self,message:&Message)->Result<u32>{let bytes=encode(message)?.len()+12;if bytes>self.maximum || bytes>u32::MAX as usize {return Err(Error::Overloaded);}Ok(bytes as u32)}
    pub fn try_send(&self,message:Message)->Result<()> {
        let size=self.size(&message)?;let control=Self::control(&message) && size<=64<<10;
        let(global,peer,send)=if control {(&self.control_global,&self.control_peer,&self.control)}else{(&self.global,&self.peer,&self.send)};
        let global=global.clone().try_acquire_many_owned(size).map_err(|_|Error::Overloaded)?;
        let peer=peer.clone().try_acquire_many_owned(size).map_err(|_|Error::Overloaded)?;
        send.try_send(Queued{message,prepared:None,_global:global,_peer:peer}).map_err(|_|Error::Overloaded)
    }
    pub(crate) fn try_send_prepared(&self,message:Message,prepared:Arc<crate::protocol::Prepared>)->Result<()> {
        if prepared.charge>self.maximum || prepared.charge>u32::MAX as usize{return Err(Error::Overloaded);}
        let size=prepared.charge as u32;let control=Self::control(&message)&&size<=64<<10;
        let(global,peer,send)=if control{(&self.control_global,&self.control_peer,&self.control)}else{(&self.global,&self.peer,&self.send)};
        let global=global.clone().try_acquire_many_owned(size).map_err(|_|Error::Overloaded)?;
        let peer=peer.clone().try_acquire_many_owned(size).map_err(|_|Error::Overloaded)?;
        send.try_send(Queued{message,prepared:Some(prepared),_global:global,_peer:peer}).map_err(|_|Error::Overloaded)
    }
    pub async fn send(&self,message:Message)->Result<()> {
        tokio::time::timeout(Duration::from_secs(5),async {
            let size=self.size(&message)?;let control=Self::control(&message) && size<=64<<10;
            let(global,peer,send)=if control {(&self.control_global,&self.control_peer,&self.control)}else{(&self.global,&self.peer,&self.send)};
            let global=global.clone().acquire_many_owned(size).await.map_err(|_|Error::Unavailable("queue closed".into()))?;
            let peer=peer.clone().acquire_many_owned(size).await.map_err(|_|Error::Unavailable("queue closed".into()))?;
            send.send(Queued{message,prepared:None,_global:global,_peer:peer}).await.map_err(|_|Error::Unavailable("queue closed".into()))
        }).await.map_err(|_|Error::Unavailable("queue deadline".into()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn saturated_data_keeps_control_capacity_and_disconnect_releases_waiters() {
        let (send,mut recv)=Sender::new(Arc::new(Semaphore::new(1<<20)),1<<20);
        let data=Message::SnapshotChunk{transfer:[1;16],index:0,data:vec![1;64<<10]};
        while send.try_send(data.clone()).is_ok() {}
        send.try_send(Message::Ping(1)).unwrap();assert!(matches!(recv.recv().await.unwrap().message,Message::Ping(1)));
        let waiting=send.clone();let task=tokio::spawn(async move{waiting.send(data).await});tokio::task::yield_now().await;drop(recv);
        let error=tokio::time::timeout(Duration::from_millis(100),task).await.unwrap().unwrap().unwrap_err();assert!(matches!(error,Error::Unavailable(_)));
    }
}
