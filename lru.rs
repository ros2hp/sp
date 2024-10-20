
use crate::rkey::RKey;
use crate::node::RNode;
use crate::cache::ReverseCache;

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

use tokio::sync::Mutex;


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
pub struct LRUevict {
    capacity: usize,
    cnt : usize,
    // pointer to Entry value in the LRU linked list for a RKey
    lookup : HashMap<RKey,Arc<Mutex<Entry>>>,
    //
    evict_submit_ch: tokio::sync::mpsc::Sender<RKey>,
    //
    pub head: Option<Arc<Mutex<Entry>>>,
    tail: Option<Arc<Mutex<Entry>>>,
}


impl LRUevict {

    pub fn new(
        cap: usize,
        ch: tokio::sync::mpsc::Sender<RKey>,
    ) -> (Arc<tokio::sync::Mutex<Self>>, Arc<Mutex<ReverseCache>>) {
        (
        Arc::new(tokio::sync::Mutex::new(LRUevict {
            capacity: cap,
            cnt: 0,
            lookup: HashMap::new(),
            evict_submit_ch: ch,
            head: None,
            tail: None,
        })),
        ReverseCache::new()
        )
    }
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
    
    // prerequisite - lru_entry has been confirmed to be in lru-cache.
    pub async fn move_to_head(
        &mut self,
        rkey: RKey,
    ) {  
        // abort if lru_entry is at head of lru
        match self.head {
            None => {
                println!("LRU empty - about to add an entry");
            }
            Some(ref v) => {
                if v.lock().await.key == rkey {
                    // rkey already at head
                    return
                }    
            }
        }

        let lru_entry=Arc::clone(self.lookup.get_mut(&rkey).as_ref().unwrap());
        {
            // detach the entry before attaching to LRU head
            let mut lru_entry_guard = lru_entry.lock().await;

            let prev_ = lru_entry_guard.prev.as_ref().unwrap();
            let Some(prev_upgrade) =  prev_.upgrade() else {panic!("failed to upgrade weak lru_entry")};
            let mut prev_guard = prev_upgrade.lock().await;

            let next_ = lru_entry_guard.next.as_ref().unwrap();
            let mut next_guard= next_.lock().await;

            prev_guard.next = Some(next_.clone());
            next_guard.prev = Some(Arc::downgrade(&prev_upgrade));
        }
        let mut lru_entry_guard = lru_entry.lock().await;
        lru_entry_guard.next = self.head.clone();
        lru_entry_guard.prev = None;

        self.head = Some(lru_entry.clone());

    }




    // prerequisite - lru_entry has been confirmed NOT to be in lru-cache.
    // note: can only execute methods on LRUevict if lock on is has been acquired as it is embedded in Arc<Mutex<>>
    pub async fn attach(
        &mut self, // , cache_guard:  &mut tokio::sync::MutexGuard<'_, ReverseCache>
        rkey : RKey,
    ) {
        if self.cnt > self.capacity {
            println!("evict...");
            // unlink tail lru_entry from lru and notify evict service.
            // Clone REntry as about to purge it from cache.
            let evict_lru_entry = self.tail.as_ref().unwrap().clone();

            let Some(new_tail) = evict_lru_entry.lock().await.prev.as_ref().unwrap().upgrade() else {panic!("Expecte Some")};
            let mut new_tail_guard = new_tail.lock().await;
            new_tail_guard.next = None;
            self.tail = Some(new_tail.clone());
            self.cnt-=1;
            
            // notify evict service to asynchronously save to db before removing from cache
            let evict_rkey = evict_lru_entry.lock().await.key.clone();
            if let Err(err) = self
                .evict_submit_ch
                .send(evict_rkey)
                .await
            {
                println!("Error sending on Evict channel: [{}]", err);
            }
        } 
        // attach to head of LRU
        let mut new_lru_entry = Entry::new(rkey.clone()); 
        let e=Arc::new(Mutex::new(new_lru_entry.clone()));
        match self.head.clone() {
            None => { 
                // empty LRU 
                println!("LRU <<< attach: empty LRU set head and tail");            
                self.head = Some(e.clone());
                self.tail = Some(e.clone());
                }
            Some(ref e) => {
                println!("LRU <<< NEW head {:?}",rkey.clone()); 
                let mut head_guard = e.lock().await;
                new_lru_entry.next = Some(e.clone());
                let arc_new_lru_entry = Arc::new(Mutex::new(new_lru_entry));
                head_guard.prev = Some(Arc::downgrade(&arc_new_lru_entry.clone()));
                self.head=Some(arc_new_lru_entry);
            }
        }
        self.cnt+=1;
        println!("LRU cnt {}",self.cnt);

        self.lookup.insert(rkey, e); 
        println!("LRU lookup len {}",self.lookup.len());
        
    }
}