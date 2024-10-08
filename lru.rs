use crate::node::NodeState;
use crate::node::RNode;
use std::sync::{Arc, Weak};
#[allow(unused_imports)]
use tokio::sync::MutexGuard;

use super::*;


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
        // abort if node at head of lru, as subsequent attach will only add it back.
        match self.head {
            None => {
                println!("LRU empty - about to populate");
            }
            Some(ref v) => {
                if Arc::as_ptr(v) == Arc::as_ptr(node) {
                    // node already at head
                    return;
                }
            }
        }
        // detach node from LRU
        match node_guard.next {
            None => {
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
                let next = Arc::clone(next_);
                // lock next node in lru - about to relink it
                let mut next_guard = next.lock().await;

                if let Some(ref prev_) = node_guard.prev {
                    let prev = Weak::clone(prev_);
                    next_guard.set_prev(prev);
                } else {
                    println!("detach: expected Some got None in prev")
                }

                match node_guard.prev.as_ref().unwrap().upgrade() {
                    None => {
                        println!("Detach error: Weak failed to be upgraded...")
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
        node_guard.set_next(self.head.as_ref().unwrap().clone());

        // attach node to head of lru
        self.head = Some(node.clone());
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
            drop(evict_node); // unlock
                              // evict from cache. RNode exists outside of cache (held in a cloned Arc), which has a cloned version passed to Eviction service
            cache_guard.0.remove(&evict_rkey);
            // notify evict service to asynchronously save to db
            if let Err(err) = self
                .evict_submit_ch
                .send((evict_rkey.clone(), evict_arc_node))
                .await
            {
                println!("Error sending on Evict channel: [{}]", err);
            }
        } // unlock cache_guard, evict_node
        node_guard.prev = None;
        node_guard.set_next(self.head.as_ref().unwrap().clone());

        // attach node to head of lru
        self.head = Some(arc_node.clone());
    }
}

