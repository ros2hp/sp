
use crate::rkey::RKey;
use crate::node::RNode;
use crate::cache::ReverseCache;

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Weak};

use tokio::sync::{Mutex, MutexGuard};

// lru is used only to drive lru_entry eviction.
// the lru_entry cache is separate
#[derive(Clone)]
pub struct Entry{
    pub key: RKey,
    //
    pub next: Option<Arc<Mutex<Entry>>>,
    pub prev: Option<Weak<Mutex<Entry>>>,
}
use super::*;

impl Entry {
    fn new(rkey : RKey) -> Entry {
        Entry{key: rkey
            ,next: None
            ,prev: None
        }
    }
}

// impl Drop for Entry {
//         fn drop(&mut self) {
//         println!("\nDROP LRU Entry {:?}\n",self.key);
//     }
// }


pub struct LRUevict {
    capacity: usize,
    pub cnt : usize,
    // pointer to Entry value in the LRU linked list for a RKey
    lookup : HashMap<RKey,Arc<Mutex<Entry>>>,
    //
    persist_submit_ch: tokio::sync::mpsc::Sender<(RKey, Arc<Mutex<RNode>>, tokio::sync::mpsc::Sender<bool>)>,
    //
    pub head: Option<Arc<Mutex<Entry>>>,
    tail: Option<Arc<Mutex<Entry>>>,
}


// implement attach & move_to_head as trait methods.
// // Makes more sense however for these methods to be part of the LRUevict itself - just epxeriementing with traits.
// pub trait LRU {
//     fn attach(
//         &mut self, // , cache_guard:  &mut tokio::sync::MutexGuard<'_, ReverseCache>
//         rkey: RKey,
//     );

//     //pub async fn detach(&mut self
//     fn move_to_head(
//         &mut self,
//         rkey: RKey,
//     );
// }


impl LRUevict { //impl LRU for MutexGuard<'_, LRUevict> {
    
    pub fn new(
        cap: usize,
        ch: tokio::sync::mpsc::Sender<(RKey, Arc<Mutex<RNode>>, tokio::sync::mpsc::Sender<bool> )>,
    ) -> (Arc<tokio::sync::Mutex<Self>>, Arc<Mutex<ReverseCache>>) {
        (
        Arc::new(tokio::sync::Mutex::new(LRUevict{
            capacity: cap,
            cnt: 0,
            lookup: HashMap::new(),
            persist_submit_ch: ch,
            head: None,
            tail: None,
        })),
        ReverseCache::new()
        )
    }
    
    async fn print(&self) {

        let mut entry = self.head.clone();
        println!("print LRU chain");
        let mut i = 0;
        while let Some(entry_) = entry {
            i+=1;
            let rkey = entry_.lock().await.key.clone();
            println!("LRU entry {} {:?}",i, rkey);
            entry = entry_.lock().await.next.clone();      
        }
    }
    // prerequisite - lru_entry has been confirmed NOT to be in lru-cache.
    // note: can only execute methods on LRUevict if lock has been acquired via Arc<Mutex<LRU>>
    pub async fn attach(
        &mut self, // , cache_guard:  &mut tokio::sync::MutexGuard<'_, ReverseCache>
        task : usize, 
        rkey : RKey,
        mut cache: Arc<Mutex<ReverseCache>>,
        waits : Waits,
    ) {
        // calling routine (RKEY) is hold lock on RNode(RKEY)
        
        //println!("   ");
        //println!("{} LRU attach enter {:?}",task, rkey);
        //self.print().await;     
        let mut lc = 0;   
        while self.cnt > self.capacity && lc < 2 {
        
            lc += 1;
            // ================================
            // Evict the tail entry in the LRU 
            // ================================
            let lru_evict_entry = self.tail.as_ref().unwrap().clone();

            //println!("{} 1 LRU attach evict - about to acquire LRU-Entry lock {:?}",task, rkey);
            let mut evict_entry = lru_evict_entry.lock().await;

            //println!("{} 2 LRU attach evict - about to acquire cache lock {:?}",task, evict_entry.key);
            let pre_lock = Instant::now();
            let mut cache_guard = cache.lock().await;
            waits.record(Event::Cache_Mutex, Instant::now().duration_since(pre_lock)).await;

            let Some(arc_evict_node_) = cache_guard.0.get(&evict_entry.key)  
                        else { //println!("{} LRU: PANIC - attach evict processing: expect entry in cache {:?}",task, evict_entry.key);
                               panic!("LRU: attach evict processing: expect entry in cache {:?}",evict_entry.key)};
            let arc_evict_node=arc_evict_node_.clone();
            drop(cache_guard);
            // ===========================================================
            // try to acquire lock on evict node, otherwise abort eviction
            // ===========================================================
            //println!("{} 3 LRU attach evict - acquire evict-node lock  {:?} evict key {:?}",task, rkey, evict_entry.key);
            //if let Ok(mut evict_node_guard) = arc_evict_node.try_lock() {
            let tlock_result = arc_evict_node.try_lock();
            match tlock_result {

                Ok(mut evict_node_guard)  => {

                    // create channel to synchronise with persist service
                    let (client_ch, mut client_rx) = tokio::sync::mpsc::channel::<bool>(1);
            
                    evict_node_guard.evicted=true;
                    // ============================
                    // remove node from cache
                    // ============================
                    //println!("{} 4 LRU attach evict -  acquire cache lock again {:?} evict key {:?}", task, rkey, evict_entry.key);
                    let mut cache_guard = cache.lock().await;
                    //println!("{} 2 LRU attach evict - acquired cache lock {:?} evict key {:?}", task, rkey, evict_entry.key);
                    //println!("{} 5 LRU attach evict - cached locked - remove from Cache  evict node {:?} rkey {:?}", task,  evict_entry.key, rkey,);
                    cache_guard.0.remove(&evict_entry.key);
                    // ===================================
                    // detach evict entry from tail of LRU
                    // ===================================         
                    match evict_entry.prev {
                        None => {panic!("LRU attach - evict_entry - expected prev got None")}
                        Some(ref v) => {
                            match v.upgrade() {
                                None => {panic!("LRU attach - evict_entry - could not upgrade")}
                                Some(new_tail) => {
                                    let mut new_tail_guard = new_tail.lock().await;
                                    new_tail_guard.next = None;
                                    self.tail = Some(new_tail.clone()); 
                                }
                            }
                        }
                    }
                    evict_entry.prev=None;
                    evict_entry.next=None;
                    self.cnt-=1;
                    // ======================================
                    // notify persist service - synchronised.
                    // ======================================
                    //println!("{} 6 LRU attach notify evict service ...passing arc_node for rkey {:?} evict {:?}",task, rkey, evict_entry.key);
                    if let Err(err) = self
                                        .persist_submit_ch
                                        .send((evict_entry.key.clone(), arc_evict_node.clone(), client_ch.clone() ))
                                        .await
                                    {
                                        //println!("{} LRU Error sending on Evict channel: [{}]",task, err);
                                    }
                    // sync with persist - so evict node exists in persisting state (hashmap'd) while the lock on evict node is active
                    if let None = client_rx.recv().await {
                        panic!("LRU read from client_rx is None ");
                    }
                    // =====================
                    // remove from lru lookup 
                    // =====================
                    self.lookup.remove(&evict_entry.key);
                    //println!("{} 7 LRU attach - release evict-node lock {:?}",task, evict_entry.key);
                } 
            
                Err(err) =>  {
                    // Abort eviction - as node is being accessed.
                    // TODO check if error is "node locked"
                    //println!("{} 3x LRU attach - lock cannot be acquired - abort eviction {:?}",task, evict_entry.key);
                    }
            }
        }
        // ======================
        // attach to head of LRU
        // ======================
        let arc_new_entry = Arc::new(Mutex::new(Entry::new(rkey.clone())));
        match self.head.clone() {
            None => { 
                // empty LRU 
                println!("LRU <<< attach: empty LRU set head and tail");     
                self.head = Some(arc_new_entry.clone());
                self.tail = Some(arc_new_entry.clone());
                }
            
            Some(e) => {
                let mut new_entry = arc_new_entry.lock().await;
                let mut old_head_guard = e.lock().await;
                // set old head prev to point to new entry
                old_head_guard.prev = Some(Arc::downgrade(&arc_new_entry));
                // set new entry next to point to old head entry & prev to NOne   
                new_entry.next = Some(e.clone());
                new_entry.prev = None;
                // set LRU head to point to new entry
                self.head=Some(arc_new_entry.clone());
                
                if let None = new_entry.next {
                        println!("LRU INCONSISTENCY attach: expected Some for next but got NONE {:?}",rkey);
                }
                if let Some(_) = new_entry.prev {
                        println!("LRU INCONSISTENCY attach: expected None for prev but got NONE {:?}",rkey);
                }
                if let None = new_entry.next {
                        println!("LRU INCONSISTENCY attach: expected entry to have next but got NONE {:?}",rkey);
                }
                if let Some(_) = new_entry.prev {
                        println!("LRU INCONSISTENCY attach: expected entry to have prev set to NONE {:?}",rkey);
                }
                if let None = old_head_guard.prev {
                        println!("LRU INCONSISTENCY attach: expected entry to have prev set to NONE {:?}",rkey);
                }
            }
        }
        self.lookup.insert(rkey, arc_new_entry);
        
        if let None = self.head {
                println!("LRU INCONSISTENCY attach: expected LRU to have head but got NONE")
        }
        if let None = self.tail {
                println!("LRU INCONSISTENCY attach: expected LRU to have tail but got NONE")
        }
        self.cnt+=1;
        //println!("{} LRU: attach add cnt {}",task, self.cnt);
        //self.print().await;
    }
    
    
    // prerequisite - lru_entry has been confirmed to be in lru-cache.\/
    // to execute a method a lock has been taken out on the LRU
    pub async fn move_to_head(
        &mut self,
        task: usize, 
        rkey: RKey,
    ) {  
        println!("--------------");
        println!("***** LRU move_to_head {:?} ********",rkey);
        // abort if lru_entry is at head of lru
        match self.head {
            None => {
                panic!("LRU empty - expected entries");
            }
            Some(ref v) => {
                let hd: RKey = v.lock().await.key.clone();
                if hd == rkey {
                    // rkey already at head
                    println!("LRU entry already at head - return");
                    return
                }    
            }
        }
        // lookup entry in map
        let lru_entry = match self.lookup.get(&rkey) {
            None => {   //println!("{} LRU - move_to_head  PANIC lookup returned none for {:?}",task, rkey);
                        panic!("LRU: move_to_head - lookup returned None for {:?}",rkey);
                    }
            Some(v) => v.clone()
        };
        {
            // DETACH the entry before attaching to LRU head
            
            let mut lru_entry_guard = lru_entry.lock().await;
            // NEW CODE to fix eviction and new request at same time on a Node
            if let None = lru_entry_guard.prev {
                //println!("LRU INCONSISTENCY move_to_head: expected entry to have prev but got NONE {:?}",rkey);
                if let None = lru_entry_guard.next {
                    // should not happend
                    panic!("LRU move_to_head : got a entry with no prev or next set (ie. a new node) - some synchronisation gone astray")
                }
            }

            // check if moving tail entry
            if let None = lru_entry_guard.next {
            
                //println!("{} LRU move_to_head detach tail entry {:?}",task, rkey);
                let prev_ = lru_entry_guard.prev.as_ref().unwrap();
                let Some(prev_upgrade) =  prev_.upgrade() else {panic!("at tail: failed to upgrade weak lru_entry {:?}",rkey)};
                let mut prev_guard = prev_upgrade.lock().await;
                prev_guard.next = None;
                self.tail = Some(lru_entry_guard.prev.as_ref().unwrap().upgrade().as_ref().unwrap().clone());
                
            } else {
                
                let prev_ = lru_entry_guard.prev.as_ref().unwrap();
                let Some(prev_upgrade) =  prev_.upgrade() else {panic!("failed to upgrade weak lru_entry {:?}",rkey)};
                let mut prev_guard = prev_upgrade.lock().await;
    
                let next_ = lru_entry_guard.next.as_ref().unwrap();
                let mut next_guard= next_.lock().await;

                prev_guard.next = Some(next_.clone());
                next_guard.prev = Some(Arc::downgrade(&prev_upgrade));           

            }
            //println!("{} LRU - move_to_head  Detach complete {:?}",task, rkey);
        }
        // ATTACH
        let mut lru_entry_guard = lru_entry.lock().await;
        lru_entry_guard.next = self.head.clone();
        
        // adjust old head entry previous pointer to new entry (aka LRU head)
        match self.head {
            None => {
                 panic!("LRU empty - expected entries");
            }
            Some(ref v) => {
                let mut hd = v.lock().await;
                hd.prev = Some(Arc::downgrade(&lru_entry)); 
                //println!("{} LRU move_to_head: attach old head {:?} prev to {:?} ",task, hd.key, lru_entry_guard.key);
            }
        }
        lru_entry_guard.prev = None;
        self.head = Some(lru_entry.clone());
        //println!("{} LRU move_to_head: exit {:?}",task,rkey);
    }
}
