use std::collections::HashMap;
use std::sync::Arc;
use std::u128;

use tokio::time::{Duration, Instant};
use tokio::task;
#[derive(Eq, Hash, PartialEq, Debug)]
pub enum Event {
    // mutex waits
    LRU_Mutex,
    Cache_Mutex,
    // operations
    Attach,
    Move_to_Head,
    // channel waits
    Chan_Persist_Query,
    Chan_Persist_Query_Resp,
    Chan_Persist_Wait,
    Task_Send,
    Task_Recv,
    Task_Remain_Recv,
    // Dynamodb
    Get_Item,
    Persist_Embedded,
    Persist_ovb_set,
    Persist_ovb_append,
    Persist_meta,
}

pub struct Waits{
    record_ch: tokio::sync::mpsc::Sender<(Event, Duration, Duration)>,
    record_dur: Instant,
}


impl Waits {    
    
//     pub fn new(send : tokio::sync::mpsc::Sender<(Event, Duration)> 
// )-> Arc<Waits> {
//     Arc::new(Waits{
//         record_ch : send,
//     })
// }

    pub fn new(send : tokio::sync::mpsc::Sender<(Event, Duration, Duration)> 
    )-> Waits {
        Waits{
            record_ch : send,
            record_dur: Instant::now(),
        }
    }


    pub async fn record(&self, e : Event, dur : Duration) {
        //println!("STATS: send {:?} {}",e, dur.as_nanos());
        if let Err(err) = self.record_ch.send((e, Instant::now().duration_since(self.record_dur),  dur)).await {
            println!("STATS: Error on send to record_ch {}",err);
        };
    }

}

impl Clone for Waits {

    fn clone(&self) -> Self {
        //println!("STATS: clone");
        Waits{ record_ch : self.record_ch.clone(), record_dur: self.record_dur.clone()}
    }
}

pub fn start_service(mut stats_rx : tokio::sync::mpsc::Receiver<(Event, Duration, Duration)>
                    ,mut close_service_rx :tokio::sync::mpsc::Receiver<bool>
) -> task::JoinHandle<()> {

    println!("STATS: start stats service....");
    let mut waits_repo: HashMap<Event, Vec<(Duration, Duration)>> =  HashMap::new();

    let stats_server = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased; 
                Some((e, rec_dur, dur)) = stats_rx.recv() => {

                    println!("STATS: stats_rx {:?} {}",e, dur.as_nanos());
                    match waits_repo.get_mut(&e) {
                        Some(vec) => { vec.push((rec_dur, dur)); }
                        None => { waits_repo.insert(e,  vec!((rec_dur, dur))); }
                    }
                }

                // Stats does not respond to shutdown broadcast rather it is synchronised with 
                // Persist shutdown.
                _ = close_service_rx.recv() => {
                    println!("STATS: Shutdown stats service....");
                    // ==========================
                    // close channel - then drain
                    // ==========================
                    stats_rx.close();
                    // =======================
                    // drain remaining values
                    // =======================
                    let mut cnt = 0;
                    while let Some((e, rec_dur, dur)) = stats_rx.recv().await {
                            cnt+=1;
                            println!("STATS: consume remaining...{}",cnt);
                            match waits_repo.get_mut(&e) {
                                Some(vec) => { vec.push((rec_dur, dur)); }
                                None => {break;}
                            }                        
                    }
                    println!("STATS: repo size {}",waits_repo.len());
                    for (k,v) in waits_repo.iter() { 
                        println!("WAITS: repo size {:?} len {}",k,v.len());
                    }
                    for (k,v) in waits_repo.iter() {
                        let len : u128 = v.len() as u128;
                        let mut s : u128 = 0;  
                        let zero_dur : Duration = Duration::from_nanos(0);                 
                        let mut max : Duration = Duration::from_nanos(0);
                        let mut min : Duration = Duration::from_nanos(u64::MAX);
                        let mut mean : u128 = 0;

                        mean = s/len;
                        println!("WAITS {:?} len {} mean {} min {} max {}", k, len, mean, min.as_nanos(), max.as_nanos());
                    }
                    break;
                }
            }
        }
        println!("STATS: EXIT");
    });

    stats_server
}


