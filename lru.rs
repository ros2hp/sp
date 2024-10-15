use crate::node::NodeState;
use crate::node::RNode;
use std::sync::{Arc, Weak};
#[allow(unused_imports)]
use tokio::sync::MutexGuard;

use super::*;


// =======================
//  Reverse Cache
// =======================
// cache responsibility is to synchronise access to db across multiple Tokio tasks on a single cache entry.
// The state of the node edge will determine the type of update required, either embedded or OvB.
// Each cache update will be saved to db to keep both in sync.
// All mutations of the cache hashmap need to be serialized.
pub struct ReverseCache(pub HashMap<RKey, Arc<tokio::sync::Mutex<RNode>>>);

impl ReverseCache {
    fn new() -> Arc<Mutex<ReverseCache>> {
        Arc::new(Mutex::new(ReverseCache(HashMap::new())))
    }

    pub fn get(&mut self, rkey: &RKey) -> Option<Arc<tokio::sync::Mutex<RNode>>> {
        //self.0.get(rkey).and_then(|v| Some(Arc::clone(v)))
        match self.0.get(rkey) {
            None => None,
            Some(re) => Some(Arc::clone(re)),
        }
    }

    pub fn insert(&mut self, rkey: RKey, rnode: RNode) -> Arc<tokio::sync::Mutex<RNode>> {
        let arcnode = Arc::new(tokio::sync::Mutex::new(rnode));
        let y = arcnode.clone();
        self.0.insert(rkey, arcnode);
        y
    }
}


pub struct LRUcache {
    capacity: usize,
    pub cache: Arc<tokio::sync::Mutex<ReverseCache>>,
    //
    evict_submit_ch: tokio::sync::mpsc::Sender<(RKey, Arc<tokio::sync::Mutex<RNode>>)>,
    //
    pub head: Option<Arc<tokio::sync::Mutex<RNode>>>, // Option<HashValue> i.e. Option<ptr>
    tail: Option<Arc<tokio::sync::Mutex<RNode>>>,
}

impl LRUcache {
    pub fn new(
        cap: usize,
        ch: tokio::sync::mpsc::Sender<(RKey, Arc<tokio::sync::Mutex<RNode>>)>,
    ) -> Arc<tokio::sync::Mutex<Self>> {
        Arc::new(tokio::sync::Mutex::new(LRUcache {
            capacity: cap,
            cache: ReverseCache::new(), //
            evict_submit_ch: ch,        //
            head: None,
            tail: None,
        }))
    }
}

// implement attach & move_to_head as trait methods.
// Makes more sense however for these methods to be part of the LRUcache itself - just epxeriementing with traits.
pub trait LRU {
    async fn attach(
        &mut self, // , cache_guard:  &mut tokio::sync::MutexGuard<'_, ReverseCache>
        arc_node: &Arc<tokio::sync::Mutex<RNode>>,
        node_guard: &mut tokio::sync::MutexGuard<'_, RNode>,
    );

    //pub async fn detach(&mut self
    async fn move_to_head(
        &mut self,
        node: &Arc<tokio::sync::Mutex<RNode>>,
        node_guard: &mut MutexGuard<'_, RNode>,
    );
}

impl LRU for MutexGuard<'_, LRUcache> {
    
    // prerequisite - node has been confirmed to be in lru-cache.
    async fn move_to_head(
        &mut self,
        node: &Arc<tokio::sync::Mutex<RNode>>,
        node_guard: &mut MutexGuard<'_, RNode>,
    ) {
    
        match node_guard.prev {
          None => {println!(">> move_to_head: {} prev is None",node_guard.node.to_string());}
          _ => {println!(">> move_to_head: {} prev is Some",node_guard.node.to_string());}
        }
        match node_guard.next {
            None => {println!(">> move_to_head: {} next is None",node_guard.node.to_string());}
            _ =>  {println!(">> move_to_head: {} next is Some",node_guard.node.to_string());}
        }
    
        // abort if node is at head of lru
        match self.head {
            None => {
                println!("LRU empty - about to add node");
            }
            Some(ref v) => {
                let head = v.lock().await;
            
                println!(">> move_to_head: lru head is {} current node {} ", head.node.to_string(), node_guard.node.to_string());
                if Arc::as_ptr(v) == Arc::as_ptr(node) {
                    // node already at head
                    println!(">> move_to_head: {} already at head of LRU ",node_guard.node.to_string());
                    return;
                }
            }
        }
        // detach node from LRU chain before attaching at head
        match node_guard.next {
            None => {
                println!(">> move_to_head: {} at tail of LRU ",node_guard.node.to_string());
                // must be tail of lru
                match node_guard.prev.as_ref().unwrap().upgrade() {
                    None => {
                        panic!("detach error: cannot upgrade prev")
                    }
                    Some(ref prev_) => {
                        let prev = Arc::clone(prev_);
                        {
                            let mut prev_guard = prev.lock().await;
                            prev_guard.next = None;
                        } // unlock
                        self.tail = Some(prev);
                    }
                }
            }

            Some(ref next_) => {
                // in mid lru
                println!(">> move_to_head: {} mid LRU ",node_guard.node.to_string());
                let next = Arc::clone(next_);
                // lock next node in lru - about to relink it
                {
                    let mut next_guard = next.lock().await;
    
                    if let Some(ref prev_) = node_guard.prev {
                        let prev = Weak::clone(prev_);
                        next_guard.set_prev(prev);
                    } else {
                        println!("move_to_head: expected Some got None in prev for {}",node_guard.node.to_string());
                    }
                }
                match node_guard.prev.as_ref().unwrap().upgrade() {
                    None => {
                        println!("Detach error: Weak failed to be upgraded...{}",node_guard.node.to_string());
                    }
                    Some(v) => {
                        let mut prev_guard = v.lock().await;
                        if let Some(ref next_) = node_guard.next {
                            let next = Arc::clone(next_);
                            prev_guard.set_next(next);
                        }
                    }
                }
            }
        }
        node_guard.prev = None;
        //node_guard.set_next(self.head.as_ref().unwrap().clone());
        {
            match self.head {
                None =>  {}
                Some(ref n) => {
                    node_guard.set_next(n.clone());
                    // point current
                    println!(">> attach: set next nodes prev point to new node....");
                    let mut head_node = n.lock().await;
                    head_node.set_prev(Arc::downgrade(node));
                }
            }
        }
        // attach node to head of lru
        println!("move_to_head: set head of LRU to  {}",node_guard.node.to_string());
        self.head = Some(node.clone());
        
        match node_guard.prev {
          None => {println!(">> move_to_head: completed.  {} prev is None",node_guard.node.to_string());}
          _ => {println!(">> move_to_head: completed. {} prev is Some",node_guard.node.to_string());}
        }
  }





    async fn attach(
        &mut self, // , cache_guard:  &mut tokio::sync::MutexGuard<'_, ReverseCache>
        arc_node: &Arc<tokio::sync::Mutex<RNode>>,
        node_guard: &mut tokio::sync::MutexGuard<'_, RNode>,
    ) {

        
        let mut evict_last = false;
        {
            // consider relocating evict to a dedicated service - e.g. executed every 3 seconds.
            let cache_guard = self.cache.lock().await;
            // evict tail node if lRU at 90% capacity
            println!("LRU::attach size {} of {} {:?}",cache_guard.0.len(),self.capacity,node_guard.node);
            evict_last = cache_guard.0.len() > self.capacity;
        }
        if evict_last {
            println!("evict...");
            // unlink tail node from lru and notify evict service.
            // Clone RNode as about to purge it from cache.
            let evict_arc_node = self.tail.as_ref().unwrap().clone();
            
                let mut evict_node = evict_arc_node.lock().await;
                evict_node.state = NodeState::Evicting;
                let evict_rkey = super::RKey::new(evict_node.node.clone(), evict_node.rvs_sk.clone());
                if let None = evict_node.prev {
                    panic!(">> attach: evicting node {} - last on LRU. Expected Prev to be Some not None.",evict_node.node.to_string());
                } 
                let prev = evict_node.prev.as_ref().unwrap().upgrade();
                match prev {
                    None => {
                        println!("attach error: could not upgrade evicted node previous Arc")
                    }
                    Some(n) => {
                        self.tail = Some(n.clone());
                        let mut tail_node_guard = n.lock().await;
                        tail_node_guard.next = None;
                    }
                }
    
                let mut cache_guard = self.cache.lock().await;
                cache_guard.0.remove(&evict_rkey);
            
            // notify evict service to asynchronously save to db
            if let Err(err) = self
                .evict_submit_ch
                .send((evict_rkey.clone(), evict_arc_node.clone()))
                .await
            {
                println!("Error sending on Evict channel: [{}]", err);
            }
        } // unlock cache_guard, evict_node
        
        // attach to head of LRU
        node_guard.prev = None;
        match self.head {
            None => { 
                    // empty LRU 
                    println!("<<< attach: empty LRU set head and tail");
                    node_guard.next = None;
                    self.tail = Some(arc_node.clone());
                    }
            Some(ref n) => {
                node_guard.set_next(n.clone());
                // point current
                println!(">> attach: set next nodes prev point to new node....");
                let mut next_node = n.lock().await;
                next_node.set_prev(Arc::downgrade(arc_node));
            }
        }
        // attach node to head of lru
        self.head = Some(arc_node.clone());
       
       
       
       
        
        match node_guard.prev {
          None => {println!(">> attach: {} prev is None",node_guard.node.to_string());}
          _ => {println!(">> attach: {} prev is Some",node_guard.node.to_string());}
        }
        match node_guard.next {
            None => {println!(">> attach: {} next is None",node_guard.node.to_string());}
            _ =>  {println!(">> attach : {} next is Some",node_guard.node.to_string());}
        }
        
    }
}