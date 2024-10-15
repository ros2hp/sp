use super::*;

use crate::lru;

// Reverse_SK is the SK value for the Child of form R#<parent-node-type>#:<parent-edge-attribute-sn>
type ReverseSK = String;

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct RKey(pub Uuid, pub ReverseSK);

impl RKey {
    pub fn new(n: Uuid, reverse_sk: ReverseSK) -> RKey {
        RKey(n, reverse_sk)
    }

    pub async fn add_reverse_edge(&self
                            ,dyn_client: &DynamoClient
                            ,table_name: &str
                            ,lru : Arc<tokio::sync::Mutex<lru::LRUcache>>
                            ,evict_query_ch: tokio::sync::mpsc::Sender<QueryMsg>
                            ,evict_client_send_ch: tokio::sync::mpsc::Sender<bool>
                            ,evict_srv_resp_ch: &mut tokio::sync::mpsc::Receiver<bool> 
                            ,target : &Uuid
                            ,bid: usize
                            ,id : usize
    ) {
    
        println!("rkey add_reverse_edge  rkey={:?}",self);
    
        let mut lru_guard = lru.lock().await;
        let mut cache_guard = lru_guard.cache.lock().await;

    
        match cache_guard.0.get(self) {
        
            None => {
                // rkey not in cache - release locks before communicating with evict service.
                drop(cache_guard);
                drop(lru_guard);
                // not cached ... check if node currently queued in eviction service.
                if let Err(e) = evict_query_ch
                    .send(QueryMsg::new(self.clone(), evict_client_send_ch.clone()))
                    .await
                {
                    panic!("evict channel comm failed = {}", e);
                }
                let resp = match evict_srv_resp_ch.recv().await {
                    Some(resp) => resp,
                    None => {
                        panic!("communication with evict service failed")
                    }
                };
                if resp {
                    // wait while node is evicted...
                    evict_srv_resp_ch.recv().await;
                }
                let node = RNode::new_with_key(&self);
                // acqure locks and add node to cache
                lru_guard = lru.lock().await;
                let mut cache_guard = lru_guard.cache.lock().await;

                // create node and populate from db
                let node = RNode::new_with_key(self);
                let arc_node = cache_guard.insert(self.clone(), node);
                drop(cache_guard);
                //
                let mut node_guard = arc_node.lock().await;
                // add node to LRU head
                let lru_len = lru_guard.attach(&arc_node, &mut node_guard).await;
                drop(lru_guard);
                // populate node data from db
                node_guard.load_from_db(dyn_client, table_name, self).await;

                node_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);

                println!("^^^ node_guard.target_uid.len() {}", node_guard.target_uid.len());
            }
            
            Some(cache_node_) => {
            
                println!(">>> found in cache {:?}",self);
                // clone so scope of cache_guard borrow ends here.
                let cache_node = cache_node_.clone();
                // prevent overlap in nonmutable and mutable references to lru_guard
                drop(cache_guard);
                //   exists in lru so move from current lru position to head (i.e detach then attach)
                let cache_node = cache_node.clone();
                let mut node_guard = cache_node.lock().await;
                //
                lru_guard.move_to_head(&cache_node, &mut node_guard).await;
            }
        }
    }
}