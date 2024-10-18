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
    
        println!("rkey add_reverse_edge  rkey={:?}",self);

        let cache_guard = cache.lock().await;
        
        match cache_guard.0.get(&self) {
        
            None => {
                drop(cache_guard);
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
                // create node and populate from db
                let mut rnode = RNode::new_with_key(self);

                rnode.load_from_db(dyn_client, table_name, self).await;

                rnode.add_reverse_edge(target.clone(), bid as u32, id as u32);

                {
                    let mut cache_guard = cache.lock().await;
                    cache_guard.0.insert(self.clone(), Arc::new(tokio::sync::Mutex::new(rnode)));
                }

                let mut lru_guard= lru.lock().await;
                lru_guard.attach(self.clone()).await;
            }
            
            Some(rnode) => {
  
                let mut lru_guard = lru.lock().await;
                let mut rnode_guard = rnode.lock().await;
                rnode_guard.add_reverse_edge(target.clone(), bid as u32, id as u32);
                lru_guard.move_to_head(self.clone());
            }
        }
    }
}