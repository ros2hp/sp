use crate::cache::ReverseCache;
use crate::types;
use crate::lru::LRUevict;

use crate::node::RNode;

use crate::{QueryMsg, RKey};

use std::collections::{HashMap, HashSet, VecDeque};

use std::sync::Arc;

use std::mem;
//
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::operation::update_item::{UpdateItemError, UpdateItemOutput};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

use aws_smithy_runtime_api::client::result::SdkError;

use tokio::task;
use tokio::time::{sleep, Duration, Instant};
use tokio::sync::Mutex;

use uuid::Uuid;

const MAX_PRESIST_TASKS: u8 = 20;

struct Lookup(HashMap<RKey, Arc<Mutex<RNode>>>);

impl Lookup {
    fn new() -> Lookup {
        Lookup(HashMap::new())
    }
}

struct Persisting(HashSet<RKey>);

impl Persisting {
    fn new() -> Self {
        Persisting(HashSet::new())
    }
}

// Pending eviction queue
struct PendingQ(VecDeque<RKey>);

impl PendingQ {
    fn new() -> Self {
        PendingQ(VecDeque::new())
    }
    
    fn remove(&mut self, rkey: &RKey) {
        let mut ri = 0;
        let mut found = false;

        for (i,v) in self.0.iter().enumerate() {
            if *v == *rkey {
                ri = i;
                found = true;
                break;
            }
        }
        if found {
            self.0.remove(ri);
        }
    }
}

//  container for clients querying evict service
struct QueryClient(HashMap<RKey, tokio::sync::mpsc::Sender<bool>>);

impl QueryClient {
    fn new() -> Self {
        QueryClient(HashMap::new())
    }
}

struct Persisted(HashSet<RKey>);


impl Persisted {

    fn new() -> Arc<Mutex<Persisted>> {
       Arc::new(Mutex::new(Persisted(HashSet::new())))
    }
}

pub fn start_service(
    dynamo_client: DynamoClient,
    table_name_: impl Into<String>,
    // channels
    mut evict_submit_rx: tokio::sync::mpsc::Receiver<(RKey, Arc<Mutex<RNode>>)>,
    mut client_query_rx: tokio::sync::mpsc::Receiver<QueryMsg>,
    mut shutdown_ch: tokio::sync::broadcast::Receiver<u8>,
) -> task::JoinHandle<()> {

    let table_name = table_name_.into();

    //let mut start = Instant::now();

    println!("PERSIST  starting evict service: table [{}] ", table_name);

    let mut persisting = Persisting::new();
    let mut persisted = Persisted::new(); // temmporary - initialise to zero ovb metadata when first persisted
    let mut lookup = Lookup::new();
    let mut pendingQ = PendingQ::new();
    let mut query_client = QueryClient::new();
    let mut tasks = 0;

    // evict channel used to acknowledge to a waiting client that the associated node has completed eviction.
    let (evict_completed_send_ch, mut evict_completed_rx) =
        tokio::sync::mpsc::channel::<RKey>(MAX_PRESIST_TASKS as usize);

    //let backoff_queue : VecDeque = VecDeque::new();
    let dyn_client = dynamo_client.clone();
    let tbl_name = table_name.clone();
    // evict service only handles
    let evict_server = tokio::spawn(async move {
        loop {
            //let evict_complete_send_ch_=evict_completed_send_ch.clone();
            tokio::select! {
                biased;         // removes random number generation - normal processing will determine order so select! can follow it.
                // note: recv() is cancellable, meaning select! can cancel a recv() without loosing data in the channel.
                // select! will be forced to cancel recv() if another branch event happens e.g. recv() on shutdown_channel.
                Some((rkey, arc_node)) = evict_submit_rx.recv() => {

                    //  no locks acquired  - apart from Cache in async routine, which is therefore safe.

                        //println!("PERSIST : submit evict for {:?} tasks [{}]",rkey, tasks);
    
                        if tasks >= MAX_PRESIST_TASKS {
                        
                            pendingQ.0.push_front(rkey.clone());
                            {
                            lookup.0.insert(rkey.clone(), arc_node);
                            }
                            println!("PERSIST  EVIct: submit - max tasks reached add to pendingQ now {}",pendingQ.0.len())
    
                        } else {
    
                            persisting.0.insert(rkey.clone());
                            // spawn async task to persist node
                            let dyn_client_ = dyn_client.clone();
                            let tbl_name_ = tbl_name.clone();
                            let evict_complete_send_ch_=evict_completed_send_ch.clone();
                            let persisted_=persisted.clone();
                            tasks+=1;
    
                            //println!("PERSIST : ASYNC call to persist_rnode tasks {}",tasks);
                            tokio::spawn(async move {
    
                                // save Node data to db
                                persist_rnode(
                                    &dyn_client_
                                    ,tbl_name_
                                    ,arc_node
                                    ,evict_complete_send_ch_
                                    ,persisted_
                                ).await;
    
                            });
                    }
                },

                Some(evict_rkey) = evict_completed_rx.recv() => {

                    tasks-=1;
                    
                    //println!("PERSIST : completed msg: tasks {}",tasks);
                    // remove from Pending Queue
                    pendingQ.remove(&evict_rkey);
                    {
                    lookup.0.remove(&evict_rkey);
                    }
                    persisting.0.remove(&evict_rkey);

                    // send ack to client if one is waiting on query channel
                    //println!("PERSIST : send complete persist ACK to client - if registered. {:?}",evict_rkey);
                    if let Some(client_ch) = query_client.0.get(&evict_rkey) {
                        //println!("PERSIST :   Yes.. ABOUT to send ACK to query that evict completed ");
                        // send ack of completed eviction to waiting client
                        if let Err(err) = client_ch.send(true).await {
                            panic!("Error in sending to waiting client that rkey is evicited [{}]",err)
                        }
                        //
                        query_client.0.remove(&evict_rkey);
                        //println!("PERSIST  EVIct: ABOUT to send ACK to query that evict completed - DONE");
                    }
                    //println!("PERSIST  EVIct: is client waiting..- DONE ");
                    // // process next node in evict Pending Queue
                    if let Some(arc_node) = pendingQ.0.pop_back() {
                                        //println!("PERSIST : persist next entry in pendingQ....");
                                        // spawn async task to persist node
                                        let dyn_client_ = dyn_client.clone();
                                        let tbl_name_ = tbl_name.clone();
                                        let evict_complete_send_ch_=evict_completed_send_ch.clone();
                                        let Some(arc_node_) = lookup.0.get(&evict_rkey) else {panic!("Persist service: expected arc_node in Lookup")};
                                        let arc_node=arc_node_.clone();
                                        let persisted_=persisted.clone();
                                        tasks+=1;

                                        tokio::spawn(async move {
    
                                                // save Node data to db
                                                persist_rnode(
                                                    &dyn_client_
                                                    ,tbl_name_
                                                    ,arc_node.clone()
                                                    ,evict_complete_send_ch_
                                                    ,persisted_
                                                ).await;
                                        });
                    }
                    //println!("PERSIST : complete task exit");
                },

                Some(query_msg) = client_query_rx.recv() => {

                     // no locks acquired - may asynchronously acquire cache lock - safe.

                    // ACK to client whether node is currently persisting
                    //println!("PERSIST : client query for {:?}",query_msg.0);
                    if let Some(_) = persisting.0.get(&query_msg.0) {
                        // register for notification of persist completion.
                        query_client.0.insert(query_msg.0.clone(), query_msg.1.clone());
                        // send ACK (true) to client 
                        //println!("PERSIST : send ACK (true) to client {:?}",query_msg.0);
                        if let Err(err) = query_msg.1.send(true).await {
                            panic!("Error in sending query_msg [{}]",err)
                        };
                        
                    } else {
                        //println!("PERSIST : send ACK (false) to client {:?}",query_msg.0);
                        // send ACK (false) to client 
                        if let Err(err) = query_msg.1.send(false).await {
                            panic!("Error in sending query_msg [{}]",err)
                        };                
                    }

                    //println!("PERSIST :  client_query exit {:?}",query_msg.0);
                },


                _ = shutdown_ch.recv(), if tasks == 0 => {
                        ////println!("PERSIST  Shutdown of evict service started. Waiting for remaining evict tasks [{}]to complete...",tasks);
                        while tasks > 0 {
                            //println!("PERSIST  ...waiting on {} tasks",tasks);
                            let Some(evict_rkey) = evict_completed_rx.recv().await else {panic!("Inconsistency; expected task complete msg got None...")};
                            tasks-=1;
                            // send to client if one is waiting on query channel. Does not block as buffer size is 1.
                            if let Some(client_ch) = query_client.0.get(&evict_rkey) {
                                // send ack of completed eviction to waiting client
                                if let Err(err) = client_ch.send(true).await {
                                    panic!("Error in sending to waiting client that rkey is evicited [{}]",err)
                                }
                                //
                                query_client.0.remove(&evict_rkey);
                            }
                        }
                        // exit loop
                        break;
                },
            }
        } // end-loop
    });
    evict_server
}


async fn persist_rnode(
    dyn_client: &DynamoClient,
    table_name_: impl Into<String>,
    arc_node: Arc<tokio::sync::Mutex<RNode>>,
    evict_completed_send_ch: tokio::sync::mpsc::Sender<RKey>,
    persisted : Arc<Mutex<Persisted>>,
) {
    // at this point, cache is source-of-truth updated with db values if edge exists.
    // use db cache values to decide nature of updates to db
    // Note for LIST_APPEND Dynamodb will scan the entire attribute value before appending, so List should be relatively small < 10000.

    //println!( "PRESIST Persit started ....rkey = {:?}", rkey);
    
    let table_name: String = table_name_.into();
    let mut target_uid: Vec<AttributeValue>;
    let mut target_bid: Vec<AttributeValue>;
    let mut target_id: Vec<AttributeValue>;
    let mut update_expression: &str;
    
    let mut node = arc_node.lock().await;
    let rkey = RKey::new(node.node, node.rvs_sk.clone());
    let init_cnt = node.init_cnt as usize;
    let edge_cnt = node.target_uid.len() + init_cnt;
    
    println!("\n------------------------- PERSIST  init_cnt, target_uid  edge_cnt {}  {}  {}  {:?} ",init_cnt, node.target_uid.len(), edge_cnt, rkey);
    if init_cnt <= crate::EMBEDDED_CHILD_NODES {
    
        println!("PERSIST  ..init_cnt < EMBEDDED. {:?}", rkey);
    
        if node.target_uid.len() <= crate::EMBEDDED_CHILD_NODES - init_cnt {
            // consume all of node.target*
            target_uid = mem::take(&mut node.target_uid);
            target_id = mem::take(&mut node.target_id);
            target_bid = mem::take(&mut node.target_bid);
        } else {
            // consume portion of node.target*
            target_uid = node
                .target_uid
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_uid, &mut node.target_uid);
            target_id = node
                .target_id
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_id, &mut node.target_id);
            target_bid = node
                .target_bid
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_bid, &mut node.target_bid);
        }

        if init_cnt == 0 {
            // no data in db
            update_expression = "SET #cnt = :cnt, #target = :tuid,   #bid = :bid , #id = :id";
             //update_expression = "SET #cnt = :cnt, #target = :tuid , #id = :id";
        } else {
            // append to existing data
           update_expression = "SET #target=list_append(#target, :tuid), #id=list_append(#id,:id), #bid=list_append(#bid,:bid), #cnt = :cnt";
           //update_expression = "SET #target=list_append(#target, :tuid), #id=list_append(#id,:id), #cnt = :cnt";
        }
        
        println!("PERSIST SAVE embedde ..node.obcnt {}, node.ocur  {:?} upd expression [{}]  {:?}",node.obcnt,node.ocur , update_expression,rkey);
        
        //update edge_item
        let result = dyn_client
            .update_item()
            .table_name(table_name.clone())
            .key(
                types::PK,
                AttributeValue::B(Blob::new(rkey.0.clone().as_bytes())),
            )
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .update_expression(update_expression)
            // reverse edge
            .expression_attribute_names("#cnt", types::CNT)
            .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
            .expression_attribute_names("#target", types::TARGET_UID)
            .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
            .expression_attribute_names("#id", types::TARGET_ID)
            .expression_attribute_values(":id", AttributeValue::L(target_id))
            .expression_attribute_names("#bid", types::TARGET_BID)
            .expression_attribute_values(":bid", AttributeValue::L(target_bid))
            //.return_values(ReturnValue::AllNew)
            .send()
            .await;

            handle_result(&rkey, result);

    }

    ////println!("PERSIST  PERSIT node.target_uid.len() {}",node.target_uid.len());
    // consume the target_* fields by moving them into overflow batches and persisting the batch
    // note if node has been loaded from db must drive off ovb meta data which gives state of current 
    // population of overflwo batches

    println!("PERSIST  node.target_uid.len()  {}    {:?}",node.target_uid.len(),rkey);
    while node.target_uid.len() > 0 {

        println!("PERSIST  logic target_uid > 0 value {}  {:?}", node.target_uid.len(), rkey );
    
        let mut target_uid: Vec<AttributeValue> = vec![];
        let mut target_bid: Vec<AttributeValue> = vec![];
        let mut target_id: Vec<AttributeValue> = vec![];
        let mut sk_w_bid : String;
        
        match node.ocur {
            None => {
                // first OvB
                node.obcnt=crate::OV_MAX_BATCH_SIZE;  // force zero free space - see later.
                node.ocur = Some(0);
                println!("PERSIST   initialise target_uid > 0  {:?}", rkey );
                continue;
                }
            Some(mut ocur) => {
                // obcnt is space consumed in current batch of current OvB. 0 if new batch.
                println!("PERSIST   aa ocur {}  node.obcnt {}  {:?}",ocur, node.obcnt, rkey );
                // if space left in current batch consume it...
                
                // if crate::OV_MAX_BATCH_SIZE <= obcnt {
                //     println!("PERSIST : expected obcnt {} < rate::OV_MAX_BATCH_SIZE {}",obcnt, crate::OV_MAX_BATCH_SIZE);
                //     panic!("PERSIST: expected obcnt {} < rate::OV_MAX_BATCH_SIZE {}",obcnt, crate::OV_MAX_BATCH_SIZE);
                // }
                let batch_freespace = crate::OV_MAX_BATCH_SIZE - node.obcnt;

                println!("PERSIST  batch_freespace {}    {:?}", batch_freespace, rkey);

                if batch_freespace > 0 {
                
                    // consume last of node.target*
                    if node.target_uid.len() <= batch_freespace {
                        println!("PERSIST   before take {} target_uid.len  {}  node.target_uid.len() {} obcnt {}   {:?}",batch_freespace , target_uid.len(),  node.target_uid.len()  , node.obcnt, rkey );
                        // consume all of node.target*
                        target_uid = mem::take(&mut node.target_uid);
                        target_bid = mem::take(&mut node.target_bid);
                        target_id = mem::take(&mut node.target_id);
                        node.obcnt += target_uid.len();
                        
                        println!("PERSIST   after take {} target_uid.len  {}  node.target_uid.len() {} obcnt {}   {:?}",batch_freespace , target_uid.len(),  node.target_uid.len()  , node.obcnt, rkey );

                    } else {
                        
                        // consume portion of node.target*
                        target_uid = node
                            .target_uid
                            .split_off(batch_freespace);
                        std::mem::swap(&mut target_uid, &mut node.target_uid);
                        target_bid = node.target_bid.split_off(batch_freespace);
                        std::mem::swap(&mut target_bid, &mut node.target_bid);
                        target_id = node.target_id.split_off(batch_freespace);
                        std::mem::swap(&mut target_id, &mut node.target_id);
                        node.obcnt=crate::OV_MAX_BATCH_SIZE;
                        println!("PERSIST  after split {} target_uid {}  node.target_uid.len() {} obcnt {}   {:?}",batch_freespace , target_uid.len(), node.target_uid.len()  , node.obcnt, rkey );
  
                    }
                                         
                    update_expression = "SET #target=list_append(#target, :tuid), #bid=list_append(#bid, :bid), #id=list_append(#id, :id)";  
                    println!("PERSIST : >> oid.len {}.   {:?}",node.oid.len(),rkey);
                    
                    sk_w_bid = rkey.1.clone();
                    sk_w_bid.push('%');
                    sk_w_bid.push_str(&node.obid[ocur as usize].to_string());
                    println!("PERSIST  sk_w_bid {} {:?}",sk_w_bid, rkey);          
                
                } else {
                
                    // create a new batch optionally in a new OvB
                    if node.ovb.len() < crate::MAX_OV_BLOCKS {
                        // create a new OvB
                        node.ovb.push(Uuid::new_v4());
                        //node.ovb.push(AttributeValue::B(Blob::new(Uuid::new_v4() as bytes)));
                        node.obid.push(1);
                        node.obcnt = 0;
                        node.ocur = Some(node.ovb.len() as u8 - 1);
                        println!("PERSIST   node.ovb.len() {} < MAX_OV_BLOCKS, node.ocur {:?} obcnd {}  {:?}", node.ovb.len(), node.ocur , node.obcnt, rkey);
                        
                    } else {

                        // change current ovb (ie. block)
                        ocur+=1;
                        if ocur as usize == crate::MAX_OV_BLOCKS {
                                ocur = 0;
                        }
                        node.ocur = Some(ocur);
                            
                        println!("PERSIST   33 node.ocur, ocur {}  {}", node.ocur.unwrap(), ocur);
                        node.obid[ocur as usize] += 1;
                        node.obcnt = 0;
                    }
                    println!("PERSIST   node.ovb.len() {} < MAX_OV_BLOCKS, node.ocur {:?} obcnd {}  {:?}", node.ovb.len(), node.ocur , node.obcnt, rkey);
                        
                    if node.target_uid.len() <= crate::OV_MAX_BATCH_SIZE {

                        // consume remaining node.target*
                        target_uid = mem::take(&mut node.target_uid);
                        target_bid = mem::take(&mut node.target_bid);
                        target_id = mem::take(&mut node.target_id);
                        node.obcnt += target_uid.len();
                    
                    } else {

                        // consume leading portion of node.target*
                        target_uid = node.target_uid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_uid, &mut node.target_uid);
                        target_bid = node.target_bid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_bid, &mut node.target_bid);
                        target_id = node.target_id.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_id, &mut node.target_id);
                        node.obcnt=crate::OV_MAX_BATCH_SIZE;
                    }
                    // ================
                    // add OvB batches
                    // ================
                    sk_w_bid = rkey.1.clone();
                    sk_w_bid.push('%');
                    sk_w_bid.push_str(&node.obid[ocur as usize].to_string());
    
                    update_expression = "SET #target = :tuid, #bid=:bid, #id = :id";

                }
                // ================
                // add OvB batches
                // ================
                println!("PERSIST : ..write OvB batch upd [{}] pk {}  sk [{}]",update_expression,node.ovb[node.ocur.unwrap() as usize] , sk_w_bid);
                println!("PERSIST SAVE OvB ..node.obcnt {}, node.ocur  {:?} upd expression [{}]  {:?}",node.obcnt,node.ocur , update_expression,rkey);
   
                let result = dyn_client
                    .update_item()
                    .table_name(table_name.clone())
                    .key(
                        types::PK,
                        AttributeValue::B(Blob::new(
                            node.ovb[node.ocur.unwrap() as usize].as_bytes(),
                        )),
                    )
                    .key(types::SK, AttributeValue::S(sk_w_bid.clone()))
                    .update_expression(update_expression)
                    // reverse edge
                    .expression_attribute_names("#target", types::TARGET_UID)
                    .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
                    .expression_attribute_names("#bid", types::TARGET_BID)
                    .expression_attribute_values(":bid", AttributeValue::L(target_bid))
                    .expression_attribute_names("#id", types::TARGET_ID)
                    .expression_attribute_values(":id", AttributeValue::L(target_id))
                    //.return_values(ReturnValue::AllNew)
                    .send()
                    .await;

                handle_result(&rkey, result);
                println!("PERSIST : batch written.....{:?}",rkey);
            }
        }
    } // end while
    
  
    //update OvB meta on edge predicate only if OvB are used.
    if node.ovb.len() > 0 {
        //update_expression = "SET #cnt = :cnt, #ovb = :ovb";//, #obid = :obid, #obcnt = :obcnt, #ocur = :ocur";
        update_expression = "SET  #cnt = :cnt, #ovb = :ovb, #obid = :obid, #obcnt = :obcnt, #ocur = :ocur";
        //println!("PERSIST  PRESIST Service: OvB metadata only - {} {:?}  {}",update_expression, rkey.0.clone(), rkey.1.clone());
        //let cnt = node.ovb.len() + init_cnt as usize;
        let ocur = match node.ocur {
            None => 0,
            Some(v) => v,
        };
         println!("PERSIST  PERSIT - save metadata...{:?}",rkey);
        let result = dyn_client
            .update_item()
            .table_name(table_name.clone())
            .key(types::PK, AttributeValue::B(Blob::new(rkey.0.clone())))
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .update_expression(update_expression)
            // OvB metadata
            .expression_attribute_names("#cnt", types::CNT)
            .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
            .expression_attribute_names("#ovb", types::OVB)
            .expression_attribute_values(":ovb", types::uuid_to_av_lb(&node.ovb))
            .expression_attribute_names("#obid", types::OVB_BID)
            .expression_attribute_values(":obid", types::u32_to_av_ln(&node.obid))
            .expression_attribute_names("#obcnt", types::OVB_CNT)
            .expression_attribute_values(":obcnt", AttributeValue::N(node.obcnt.to_string()))
            .expression_attribute_names("#ocur", types::OVB_CUR)
            .expression_attribute_values(":ocur", AttributeValue::N(ocur.to_string()))
            //.return_values(ReturnValue::AllNew)
            .send()
            .await;

        handle_result(&rkey, result);
        
    }

    // send task completed msg to evict service
    if let Err(err) = evict_completed_send_ch.send(rkey.clone()).await {
        println!(
            "Sending completed evict msg to waiting client failed: {}",
            err
        );
    }
}

fn handle_result(rkey: &RKey, result: Result<UpdateItemOutput, SdkError<UpdateItemError, HttpResponse>>) {
    match result {
        Ok(_out) => {
            //println!("PERSIST  PRESIST Service: Persist successful update...")
        }
        Err(err) => match err {
            SdkError::ConstructionFailure(_cf) => {
                println!("PERSIST   Persist Service: Persist  update error ConstructionFailure...")
            }
            SdkError::TimeoutError(_te) => {
                println!("PERSIST   Persist Service: Persist  update error TimeoutError")
            }
            SdkError::DispatchFailure(_df) => {
                println!("PERSIST   Persist Service: Persist  update error...DispatchFailure")
            }
            SdkError::ResponseError(_re) => {
                println!("PERSIST   Persist Service: Persist  update error ResponseError")
            }
            SdkError::ServiceError(_se) => {
            println!("PERSIST  x Persist Service: Persist  update error ServiceError {:?}",rkey);
            println!("PERSIST  y Persist Service: Persist  update error ServiceError {:?}",rkey);
                panic!(" Persist Service: Persist  update error ServiceError {:?}",rkey);
            }
            _ => {}
        },
    }
}
