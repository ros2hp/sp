use super::*;

use crate::cache::ReverseCache;

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
                            ,persist_srv_resp_ch: &mut tokio::sync::mpsc::Receiver<bool> 
                            //
                            ,target : &Uuid
                            ,bid: usize
                            ,id : usize
    ) {
    
        println!("** RKEY add_reverse_edge:  rkey={:?} about to lock cache",self);
        let mut cache_guard = cache.lock().await;
        
        match cache_guard.0.get(&self) {
            
            None => {
                println!("{}  - RKEY add_reverse_edge: - Not Cached: rkey {:?}", task, self);
                
                // grab lock on RNode and release cache lock - this prevents concurrent updates to RNode 
                // and optimises cache concurrency by releasing lock asap
                let mut arc_rnode = RNode::new_with_key(self);
                // add to cache and release lock 
                cache_guard.0.insert(self.clone(), arc_rnode.clone());
                let mut rnode_guard = arc_rnode.lock().await;
                drop(cache_guard);
                {
                    // ======================
                    // HAS NODE BEING EVICTED 
                    // ======================
                    // must wait for the evict-persist process to complete 
                    let mut lru_guard= lru.lock().await;
                    if let Some(_) = lru_guard.evict.get(self) {
                        drop(lru_guard);
                        self.wait_till_persisted(task, lru.clone(), persist_query_ch, persist_client_send_ch, persist_srv_resp_ch).await;
                    }
                }
                rnode_guard.load_OvB_metadata_from_db(dyn_client, table_name, self).await;
                rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);
                // and attach to LRU (which handle the evictions)
                //println!("{} - RkEY add_reverse_edge: - not caChed: about to lock LRU", task);
                let mut lru_guard= lru.lock().await;
                //println!("{} - RkEY add_reverse_edge: - not caChed: about to lru-attach...{:?}", task, self);
                lru_guard.attach(task, self.clone(), cache.clone()).await;
                //println!("{} - RkEY add_reverse_edge: - not cached exit {:?}", task, self);                
            }
            
            Some(rnode_) => {

                // grab lock on RNode and release cache lock - this prevents concurrent updates to RNode 
                // and optimises cache concurrency by releasing lock asap
                let arc_rnode=rnode_.clone();
                drop(cache_guard);
                {   // prevent eviction
                    println!("RKEY: inside cache lock - about to lock LRU {:?}",self);
                    let mut lru_guard= lru.lock().await;
                    lru_guard.set_inuse(self.clone());  
                }
                // grab lock on node. Concurrent task, Evict, may have evicted the node.
                let mut rnode_guard = arc_rnode.lock().await;    
                // ======================
                // HAS NODE BEING EVICTED 
                // ======================
                if rnode_guard.evicted {
                    // must wait for the evict-persist process to complete - setup comms with persist.
                    println!("RKEY: node read from cache but detected it has been evicted....{:?}",self);
                    self. wait_till_persisted(task, lru.clone(), persist_query_ch, persist_client_send_ch, persist_srv_resp_ch).await;
                    // load node from database and attach to LRU
                    rnode_guard.load_OvB_metadata_from_db(dyn_client, table_name, self).await;
                    rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);
                    let mut lru_guard= lru.lock().await;
                    lru_guard.attach(task, self.clone(), cache.clone()).await;              
                    
                } else {
                  
                    // mark the LRU entry as immune to eviction
                    // LRU may have to be a service as we want sync immunity with LRU operations
                    println!("RKEY: node read from cache  {:?}",self);
                    let mut lru_guard= lru.lock().await;
                    //lru_guard.set_inuse(self.clone());                          
                    println!("{} - RkEY add_reverse_edge: - in cache: true about add_reverse_edge {:?}", task, self);    
                    rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);   
                    lru_guard.move_to_head(task, self.clone()).await;  
                    lru_guard.unset_inuse(&self); 
                }
            }
        }
    }

    async fn wait_till_persisted(
        &self
        ,task : usize
        ,lru : Arc<tokio::sync::Mutex<lru::LRUevict>>
        ,persist_query_ch: tokio::sync::mpsc::Sender<QueryMsg>
        ,persist_client_send_ch: tokio::sync::mpsc::Sender<bool>
        ,persist_srv_resp_ch: &mut tokio::sync::mpsc::Receiver<bool>    
    )  {
                // wait for evict service to give go ahead...(completed persisting)
                // or ack that it completed already.
                if let Err(e) = persist_query_ch
                                .send(QueryMsg::new(self.clone(), persist_client_send_ch.clone()))
                                .await
                                {
                                    panic!("evict channel comm failed = {}", e);
                                }
                // wait for persist to complete
                println!("{} - RKEY add_reverse_edge: not cached - was EVICTED previously, waiting on Persist", task);
                
                let persist_resp = match persist_srv_resp_ch.recv().await {
                    Some(resp) => resp,
                    None => {
                        panic!("communication with evict service failed")
                        
                    }
                    };
                println!("{} - RKEY add_reverse_edge: not cached - FINISHED waiting - recv ACK from PERSIT", task);
                // remove evict status
                let mut lru_guard= lru.lock().await;
                lru_guard.evict.remove(self);
                // true is node is persisting
                if persist_resp {
                    drop(lru_guard);
                    // ====================================
                    // wait for completion msg from Persist
                    // ====================================
                    persist_srv_resp_ch.recv().await;
                }
    }
}

