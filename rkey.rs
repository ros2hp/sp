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
                            ,dyn_client: &DynamoClient
                            ,table_name: &str
                            //
                            ,lru : Arc<tokio::sync::Mutex<lru::LRUevict>>
                            ,cache: Arc<tokio::sync::Mutex<ReverseCache>>
                            //
                            ,evict_query_ch: tokio::sync::mpsc::Sender<QueryMsg>
                            ,evict_client_send_ch: tokio::sync::mpsc::Sender<bool>
                            ,evict_srv_resp_ch: &mut tokio::sync::mpsc::Receiver<bool> 
                            //
                            ,target : &Uuid
                            ,bid: usize
                            ,id : usize
    ) {
    
        println!("** RKEY add_reverse_edge:  rkey={:?} about to lock cache",self);
        let cache_guard = cache.lock().await;
        
        match cache_guard.0.get(&self) {
        
            None => {
                println!("RKEY add_reverse_edge: - match None: rkey {:?}",self);
                drop(cache_guard);      
                // not in cache but maybe persisting - ask the Evict Service as its responsible
                // for for evicting nodes from the cache and persisting to the database.
                // if the evicted noded is waiting in the Evict queue then the Evict service will
                // remove dequeue the entry. Node is still in the cache at this point.
                println!("rkEY add_reverse_edge: - query Evict for persisting status");
                if let Err(e) = evict_query_ch
                  .send(QueryMsg::new(self.clone(), evict_client_send_ch.clone()))
                  .await
                {
                    panic!("evict channel comm failed = {}", e);
                }
                // Evict Service respsonds with True if the evicted node is persisting otherwise False.
                println!("rkEY add_reverse_edge: - wait for query Evict for persisting status");
                let evict_resp = match evict_srv_resp_ch.recv().await {
                  Some(resp) => resp,
                  None => {
                      panic!("communication with evict service failed")
                  }
                };
                // If persisting then wait for Ack from Evict service that persisting has completed.
                println!("rkEY add_reverse_edge: - wait for query Evict for persisting status DONE - {}",evict_resp);
                if evict_resp {
                    println!("rkEY add_reverse_edge: - wait for persisting to complete");          
                    // wait for evict to persist node
                    evict_srv_resp_ch.recv().await;
                } 
                // as the node is not cached we must create one and populate from the database
                println!("RKEY: not cached then create a new one");
                let mut rnode = RNode::new_with_key(self);
                rnode.load_from_db(dyn_client, table_name, self).await;
                rnode.add_reverse_edge(target.clone(), bid as u32, id as u32);
                // now add to cache
                {
                    println!("rkEY add_reverse_edge: - match None: about to lock cache");
                    let mut cache_guard = cache.lock().await;
                    println!("rkEY add_reverse_edge: - match None: cache locked");
                    cache_guard.0.insert(self.clone(), Arc::new(tokio::sync::Mutex::new(rnode)));
                }  
                // and attach to LRU (which handle the evictions)
                println!("rkEY add_reverse_edge: - match None: about to lock LRU");
                let mut lru_guard= lru.lock().await;
                println!("rkEY add_reverse_edge: - match None: about to lru-attach...{:?}",self);
                lru_guard.attach(self.clone()).await;
                println!("rkEY add_reverse_edge: - match None: about to lru-attach...{:?}....DONE",self);                
            }
            
            Some(rnode_) => {

                let rnode=rnode_.clone();
                drop(cache_guard);
                // mark the LRU entry as immune to eviction
                {   // LRU may have to be a service as we want sync immunity with LRU operations
                    let mut lru_guard= lru.lock().await;
                    lru_guard.set_immunity(self.clone()); 
                }
                println!("rkEY add_reverse_edge: - match RNODE about to send on evict_query_ch {:?}",self); 
                // cached but maybe queued for Eviction - ask the Evict Service.
                if let Err(e) = evict_query_ch
                  .send(QueryMsg::new(self.clone(), evict_client_send_ch.clone()))
                  .await
                {
                    panic!("evict channel comm failed = {}", e);
                }
                println!("rkEY add_reverse_edge: - match RNODE about to rcv on evict_srv_resp_ch");
                // Evict Service will respond with either
                // False - not queued for eviction or persisting.
                // True - currently being persisted to db
                let evict_resp = match evict_srv_resp_ch.recv().await {
                  Some(resp) => resp,
                  None => {
                      panic!("communication with evict service failed")
                  }
                };
                println!("rkEY add_reverse_edge: - match RNODE got resp [{}] on evict_srv_resp_ch",evict_resp);
                if evict_resp {
                    // node being evicted and cannot be aborted as currently persisting.
                    println!("RKEY add_reverse_edge: - match RNODE: about to wait on evict_srv_resp_ch");
                    
                    evict_srv_resp_ch.recv().await;
                    
                    // create a fresh node and populate from db
                    println!("rkEY add_reverse_edge: - match RNODE: false make rnode");
                    let mut rnode = RNode::new_with_key(self);
                    println!("rkEY add_reverse_edge: - match RNODE: false about to load_from_db..");
                    rnode.load_from_db(dyn_client, table_name, self).await;        
                    println!("rkEY add_reverse_edge: - match RNODE: false about to add_reverse_edge...");
                    rnode.add_reverse_edge(target.clone(), bid as u32, id as u32);
                    
                    // insert into LRU and cache
                    {
                        println!("rkEY add_reverse_edge: - match RNODE: false cache lock freed about to lock lru");
                        let mut lru_guard= lru.lock().await;
                        lru_guard.attach(self.clone()).await; 
                        lru_guard.unset_immunity(&self); 
                    }
                    {
                        println!("rkEY add_reverse_edge: - match RNODE: false about to lock cache");
                        let mut cache_guard = cache.lock().await;
                        println!("rkEY add_reverse_edge: - match RNODE: cache locked");
                        cache_guard.0.insert(self.clone(), Arc::new(tokio::sync::Mutex::new(rnode)));
                    }   
                
                } else {

                    {
                        println!("rkEY add_reverse_edge: - match RNODE: about to cache.lock()");
                        let cache_guard = cache.lock().await;
                        println!("rkEY add_reverse_edge: - match RNODE: about to cache.lock() - DONE, rnode.lock()...");
                        let mut rnode_guard = rnode.lock().await;
                        println!("rkEY add_reverse_edge: - match RNODE: true about add_reverse_edge");
                        rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);
                        println!("rkEY add_reverse_edge: - match RNODE: rue about add_reverse_edge - DONE");
                    }
                    println!("rkEY add_reverse_edge: - match RNODE: true about to lock lru");
                    let mut lru_guard = lru.lock().await;
                    println!("rkEY add_reverse_edge: - match RNODE: about to lru_guard move_to_head....");
                    lru_guard.move_to_head(self.clone()).await;
                    println!("rkEY add_reverse_edge: - match RNODE: about to lru_guard move_to_head....DONE");
                    lru_guard.unset_immunity(&self); 
                }
                println!("rkEY add_reverse_edge: - match RNODE: true - about to release inner locks");
            }

        }
        println!("rkEY add_reverse_edge: - release cache lock");
    }
}