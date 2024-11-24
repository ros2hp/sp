use super::*;

use tokio::time::Instant;

use crate::cache::ReverseCache;
use crate::service::stats::{Waits, Event};
use crate::lru;



// Reverse_SK is the SK value for the Child of form R#<parent-node-type>#:<parent-edge-attribute-sn>
type ReverseSK = String;

#[derive(Eq, PartialEq, Hash, Debug, Clone, PartialOrd, Ord)]
pub struct RKey(pub Uuid, pub ReverseSK);

impl RKey {
    pub fn new(n: Uuid, reverse_sk: ReverseSK) -> RKey {
        RKey(n, reverse_sk)
    }

    pub async fn add_reverse_edge(&self
                            ,task : usize
                            ,dyn_client: &DynamoClient
                            ,table_name: &str
                            //
                            ,lru : Arc<tokio::sync::Mutex<lru::LRUevict>>
                            ,cache: Arc<tokio::sync::Mutex<ReverseCache>>
                            //
                            ,persist_query_ch: tokio::sync::mpsc::Sender<QueryMsg>
                            ,persist_client_send_ch: tokio::sync::mpsc::Sender<bool>
                            ,persist_srv_resp_rx: &mut tokio::sync::mpsc::Receiver<bool> 
                            //
                            ,target : &Uuid
                            ,bid: usize
                            ,id : usize
                            //
                            ,waits : Waits
    ) {
        let mut before: Instant;

        ////println!("** RKEY add_reverse_edge:  RKEY={:?} about to lock cache",self);
        let mut cache_guard = cache.lock().await;
        
        match cache_guard.0.get(&self) {
            
            None => {
                ////println!("{} RKEY add_reverse_edge: - Not Cached: RKEY {:?}", task, self);
                
                // grab lock on RNode and release cache lock - this prevents concurrent updates to RNode 
                // and optimises cache concurrency by releasing lock asap
                let mut arc_rnode = RNode::new_with_key(self);
                // add to cache and release lock 
                cache_guard.0.insert(self.clone(), arc_rnode.clone());
                let mut rnode_guard = arc_rnode.lock().await;
                drop(cache_guard);
                {
                    // ======================
                    // HAS NODE BEEN EVICTED  
                    // ======================
                    // replace lru_guard.evict() - IP moved to Persist Service
                    self.wait_if_evicted(task, persist_query_ch, persist_client_send_ch, persist_srv_resp_rx, waits.clone()).await;
                }
                rnode_guard.load_OvB_metadata_from_db(dyn_client, table_name, self).await;
                rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);
                //drop(rnode_guard);
                // and attach to LRU (which handle the evictions)

                //println!("{} RKEY add_reverse_edge: - not cached: about to lock LRU {:?}", task,self );
                before = Instant::now();
                let mut lru_guard= lru.lock().await;
                waits.record(Event::LRU_Mutex, Instant::now().duration_since(before)).await;

                ////println!("{} RKEY add_reverse_edge: - not caChed: about to lru-attach...{:?}", task, self);
                before = Instant::now();
                lru_guard.attach(task, self.clone(), cache.clone(), waits.clone()).await; 
                waits.record(Event::Attach, Instant::now().duration_since(before)).await;    
                //println!("{} RKEY add_reverse_edge: - not cached exit {:?}", task,self);          
            }
            
            Some(rnode_) => {

                // grab lock on RNode and release cache lock - this prevents concurrent updates to RNode 
                // and optimises cache concurrency by releasing lock asap
                let arc_rnode=rnode_.clone();
                drop(cache_guard);

                // grab lock on node. Concurrent task, Evict, may have evicted the node.
                let mut rnode_guard = arc_rnode.lock().await;    
                // ======================
                // HAS NODE BEING EVICTED 
                // ======================
                if rnode_guard.evicted {
                    // if so, must wait for the evict-persist process to complete - setup comms with persist.
                    //println!("{} RKEY 1: node read from cache but detected it has been evicted....{:?}",task, self);
                    self.wait_if_evicted(task, persist_query_ch, persist_client_send_ch, persist_srv_resp_rx, waits.clone()).await;
                    // load node from database and attach to LRU
                    rnode_guard.load_OvB_metadata_from_db(dyn_client, table_name, self).await;
                    rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);
                    //drop(rnode_guard);
                    let mut before = Instant::now();
                    let mut lru_guard= lru.lock().await;
                    waits.record(Event::LRU_Mutex, Instant::now().duration_since(before)).await;
                    before = Instant::now();
                    lru_guard.attach(task, self.clone(), cache.clone(), waits.clone()).await;   
                    waits.record(Event::Attach, Instant::now().duration_since(before)).await; 
                    rnode_guard.evicted=false;
                                      
                } else {
                                         
                    //println!("{} RKEY 1 add_reverse_edge: - in cache about add_reverse_edge {:?}", task, self);    
                    rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);   
                    //drop(rnode_guard);
                    //println!("{} RKEY 2 add_reverse_edge: - in cache lock lru {:?}", task, self);  
                    before = Instant::now();  
                    let mut lru_guard= lru.lock().await;
                    waits.record(Event::LRU_Mutex, Instant::now().duration_since(before)).await;
                    //println!("{} RKEY 3 add_reverse_edge: - in cache sync excute of move_to_head {:?}", task, self); 
                    before = Instant::now(); 
                    lru_guard.move_to_head(task, self.clone()).await;  
                    waits.record(Event::Move_to_Head, Instant::now().duration_since(before)).await;
                    //println!("{} RKEY 4 add_reverse_edge: - cached exit {:?}", task,self);  
                }
            }
        }
         
    }

    async fn wait_if_evicted(
        &self
        ,task : usize
        ,persist_query_ch: tokio::sync::mpsc::Sender<QueryMsg>
        ,persist_client_send_ch: tokio::sync::mpsc::Sender<bool>
        ,persist_srv_resp_rx: &mut tokio::sync::mpsc::Receiver<bool>  
        //
        ,waits : Waits  
    )  {
                // wait for evict service to give go ahead...(completed persisting)
                // or ack that it completed already.
                let mut before: Instant = Instant::now();
                if let Err(e) = persist_query_ch
                                .send(QueryMsg::new(self.clone(), persist_client_send_ch.clone()))
                                .await
                                {
                                    panic!("evict channel comm failed = {}", e);
                                }
                waits.record(Event::Chan_Persist_Query, Instant::now().duration_since(before)).await;
 
                // wait for persist to complete
                before = Instant::now();
                let persist_resp = match persist_srv_resp_rx.recv().await {
                    Some(resp) => resp,
                    None => {
                        panic!("communication with evict service failed")
                        
                    }
                    };
                waits.record(Event::Chan_Persist_Query_Resp, Instant::now().duration_since(before)).await;
 
                //println!("{} RKEY add_reverse_edge: node eviced FINISHED waiting - recv'd ACK from PERSIT {:?}", task, self);
                if persist_resp {
                    // ====================================
                    // wait for completion msg from Persist
                    // ====================================
                    before = Instant::now();
                    persist_srv_resp_rx.recv().await;
                    waits.record(Event::Chan_Persist_Wait, Instant::now().duration_since(before)).await;
                }
    }
}
