//! Causal state persisted in Spool, never in SQLite metadata tables.
use crate::*;
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum Policy {Lww,Counter,Set,Min,Max}
#[derive(Clone,Debug,Default,PartialEq,Eq,Serialize,Deserialize)]
pub struct Counter {pub positive:BTreeMap<(NodeId,NodeId),u64>,pub negative:BTreeMap<(NodeId,NodeId),u64>}
impl Counter {
    pub fn add(&mut self,database:NodeId,node:NodeId,delta:i64)->Result<()> {let component=if delta>=0 {&mut self.positive}else{&mut self.negative};let value=component.entry((database,node)).or_default();*value=value.checked_add(delta.unsigned_abs()).ok_or_else(||Error::Protocol("counter component overflow".into()))?;Ok(())}
    pub fn join(&mut self,other:&Self)->Result<()> {for(target,source)in[(&mut self.positive,&other.positive),(&mut self.negative,&other.negative)]{for(actor,n)in source{let v=target.entry(*actor).or_default();*v=(*v).max(*n);}}Ok(())}
    pub fn value(&self)->Result<i64> {let positive=self.positive.values().try_fold(0i128,|s,n|s.checked_add(*n as i128)).ok_or_else(||Error::Protocol("counter overflow".into()))?;let negative=self.negative.values().try_fold(0i128,|s,n|s.checked_add(*n as i128)).ok_or_else(||Error::Protocol("counter overflow".into()))?;i64::try_from(positive-negative).map_err(|_|Error::Protocol("counter exceeds SQLite integer range".into()))}
}
#[derive(Clone,Debug,PartialEq,Eq,PartialOrd,Ord,Serialize,Deserialize)]
pub struct Tag {pub database:NodeId,pub node:NodeId,pub transaction:NodeId,pub operation:u32}
#[derive(Clone,Debug,Default,PartialEq,Eq,Serialize,Deserialize)]
pub struct Set {pub additions:BTreeMap<Vec<u8>,BTreeSet<Tag>>,pub removals:BTreeSet<Tag>}
impl Set {
    pub fn add(&mut self,value:Vec<u8>,tag:Tag) {self.additions.entry(value).or_default().insert(tag);}
    pub fn remove(&mut self,value:&[u8]) {if let Some(tags)=self.additions.get(value){self.removals.extend(tags.iter().cloned());}}
    pub fn join(&mut self,other:&Self) {for(value,tags)in &other.additions{self.additions.entry(value.clone()).or_default().extend(tags.iter().cloned());}self.removals.extend(other.removals.iter().cloned());}
    pub fn values(&self)->Vec<Vec<u8>> {self.additions.iter().filter(|(_,tags)|tags.iter().any(|tag|!self.removals.contains(tag))).map(|(v,_)|v.clone()).collect()}
}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
pub enum Causal {Counter(Counter),Set(Set)}
impl Causal {pub fn join(&mut self,other:&Self)->Result<()> {match(self,other){(Self::Counter(a),Self::Counter(b))=>a.join(b),(Self::Set(a),Self::Set(b))=>{a.join(b);Ok(())},_=>Err(Error::Protocol("merge policy changed".into()))}}}
