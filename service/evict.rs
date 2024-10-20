use crate::types;
use crate::cache::ReverseCache;

use crate::node::{RNode, EvictState};

use crate::{QueryMsg, RKey};

use std::collections::{HashMap, HashSet};

use std::sync::{Arc};
use std::mem;
//
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::operation::update_item::{UpdateItemOutput,UpdateItemError};
use aws_sdk_dynamodb::config::http::HttpResponse;

use aws_smithy_runtime_api::client::result::SdkError;

use tokio::time::{sleep, Duration, Instant};
use tokio::task;

use uuid::Uuid;

const MAX_EVICT_TASKS: u8 = 20;

// pending eviction container
struct Pending(HashSet<RKey>);

impl Pending {
    fn new() -> Self {
        Pending(HashSet::new())
    }
}

//  container for clients querying evict service
struct QueryClient(HashMap<RKey, tokio::sync::mpsc::Sender<bool>>);

impl QueryClient {
    fn new() -> Self {
        QueryClient(HashMap::new())
    }
}

pub fn start_service(
    dynamo_client: DynamoClient,
    table_name_: impl Into<String>,
    // channels
    mut evict_submit_rx: tokio::sync::mpsc::Receiver<RKey>,
    mut client_query_rx: tokio::sync::mpsc::Receiver<QueryMsg>,
    mut shutdown_ch: tokio::sync::broadcast::Receiver<u8>,
    mut cache: Arc<tokio::sync::Mutex<ReverseCache>>,
) -> task::JoinHandle<()> {
    println!("evict service...started.");

    let table_name = table_name_.into();

    //let mut start = Instant::now();

    println!("starting evict service: table [{}] ", table_name);

    let mut pending = Pending::new();
    let mut query_client = QueryClient::new();
    let mut tasks = 0;

    // evict channel used to acknowledge to a waiting client that the associated node has completed eviction.
    let (evict_completed_send_ch, mut evict_completed_rx) =
        tokio::sync::mpsc::channel::<RKey>(MAX_EVICT_TASKS as usize);

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
                Some(rkey) = evict_submit_rx.recv() => {

                    let arc_node: Arc<tokio::sync::Mutex<RNode>>;
                    {
                        let cache_guard = cache.lock().await;
                        arc_node = cache_guard.0.get(&rkey).unwrap().clone();
                    }

                    pending.0.insert(rkey.clone());

                    tasks += 1;
                    
                    println!("Evict Service: submit event for {:?} tasks [{}]",rkey, tasks);

                    if tasks >= MAX_EVICT_TASKS {

                        // block until persist done
                        let dyn_client_ = dyn_client.clone();
                        let tbl_name_ = tbl_name.clone();
                        let evict_complete_send_ch_=evict_completed_send_ch.clone();

                        persist_rnode(
                            &dyn_client_
                           ,tbl_name_
                           ,&rkey
                           ,arc_node.clone()
                           ,evict_complete_send_ch_
                        ).await;

                    } else {

                        // spawn async task to persist node
                        let dyn_client_ = dyn_client.clone();
                        let tbl_name_ = tbl_name.clone();
                        let evict_complete_send_ch_=evict_completed_send_ch.clone();

                        tokio::spawn(async move {

                            // save Node data to db
                            persist_rnode(
                                &dyn_client_
                                ,tbl_name_
                                ,&rkey
                                ,arc_node.clone()
                                ,evict_complete_send_ch_
                            ).await;

                        });
                    }
                },

                Some(evict_rkey) = evict_completed_rx.recv() => {

                    tasks-=1;
                    
                    println!("Event SErvice: completed msg: tasks {}",tasks);
                    pending.0.remove(&evict_rkey);  
                    {
                        // evict rkey from cache
                        let mut cache_guard = cache.lock().await;
                        cache_guard.0.remove(&evict_rkey);
                    }
                    // send to client if one is waiting on query channel
                    if let Some(client_ch) = query_client.0.get(&evict_rkey) {
                        // send ack of completed eviction to waiting client
                        if let Err(err) = client_ch.send(true).await {
                            panic!("Error in sending to waiting client that rkey is evicited [{}]",err)
                        }
                        //
                        query_client.0.remove(&evict_rkey);
                    }
                },

                Some(query_msg) = client_query_rx.recv() => {

                    let mut ack_sent=false;
                    let result = match pending.0.contains(&query_msg.0) {
                        true => {
                            {
                                // node is to be evicted
                                let cache_guard = cache.lock().await;
                                let arc_node = cache_guard.0.get(&query_msg.0).unwrap().clone();
                                let mut node = arc_node.lock().await;

                                if node.state == EvictState::Persist {
                                    // save client details to ack when persist completes
                                    query_client.0.insert(query_msg.0.clone(), query_msg.1.clone());
                                 
                                } else {

                                    // remove from evict queue and send ack (false).                                    
                                    node.state = EvictState::Abort;
                                    pending.0.remove(&query_msg.0);
                                    node.state = EvictState::Available;
                                    // 
                                    if let Err(err) = query_msg.1.send(false).await {
                                        panic!("Error in sending query_msg [{}]",err)
                                    };
                                    ack_sent=true;
                                } 
                            }
                            true
                            },

                        false => false
                    };
                    if !ack_sent {
                        if let Err(err) = query_msg.1.send(result).await {
                            panic!("Error in sending query_msg [{}]",err)
                        };
                    }
                },

                _ = shutdown_ch.recv(), if tasks == 0 => {
                        println!("Shutdown of evict service started. Waiting for remaining evict tasks [{}]to complete...",tasks);
                        while tasks > 0 {
                            println!("...waiting on {} tasks",tasks);
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
    rkey: &RKey,
    arc_node: Arc<tokio::sync::Mutex<RNode>>,
    evict_completed_send_ch: tokio::sync::mpsc::Sender<RKey>,
) {
    // at this point, cache is source-of-truth updated with db values if edge exists.
    // use db cache values to decide nature of updates to db
    // Note for LIST_APPEND Dynamodb will scan the entire attribute value before appending, so List should be relatively small < 10000.
    let mut node = arc_node.lock().await;
    if node.state == EvictState::Abort {
        return
    }
    node.state= EvictState::Persist;
    let table_name: String = table_name_.into();
    let mut target_uid: Vec<AttributeValue>;
    let mut target_bid: Vec<AttributeValue>;
    let mut target_id: Vec<AttributeValue>;
    let mut update_expression: &str;
    let edge_cnt = node.target_uid.len();
    let init_cnt = node.init_cnt as usize;
    
    println!("Evict Service: Persist....init_cnt {}",init_cnt);

    if init_cnt <= crate::EMBEDDED_CHILD_NODES  {

        if node.target_uid.len() <= crate::EMBEDDED_CHILD_NODES - init_cnt {
            // consume all of node.target*
            target_uid = mem::take(&mut node.target_uid);
            target_id = mem::take(&mut node.target_id);
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
        }

        if init_cnt == 0 {
            // no data in db
            //update_expression = "SET #target = :tuid, #bid = :bid, #id = :id, #cnt = :cnt";
            update_expression = "SET #cnt = :cnt, #target = :tuid,  #id = :id";
        } else {
            // append to existing data
            update_expression = "SET #target=LIST_APPEND(#target, :tuid), #id=LIST_APPEND(#id,:id), #cnt = #cnt+:cnt";
        }

        // update edge item
        let result = dyn_client
            .update_item()
            .table_name(table_name.clone())
            .key(types::PK, AttributeValue::B(Blob::new(rkey.0.clone().as_bytes())))
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .update_expression(update_expression)
            // reverse edge
            .expression_attribute_names("#cnt", types::CNT)
            .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
            .expression_attribute_names("#target", types::TARGET_UID)
            .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
            .expression_attribute_names("#id", types::TARGET_ID)
            .expression_attribute_values(":id", AttributeValue::L(target_id))
            //.return_values(ReturnValue::AllNew)
            .send()
            .await;

        handle_result(result);

    }

    // consume the target_* fields by moving them into overflow batches and persisting the batch
    while node.target_uid.len() > 0 {
       
        let mut target_uid: Vec<AttributeValue> = vec![];
        let mut target_bid: Vec<AttributeValue> = vec![];
        let mut target_id: Vec<AttributeValue> = vec![];

        match node.ocur {
            None => {
                // create a new OvB
                node.ovb.push(Uuid::new_v4());
                node.obid.push(1);
                node.ocur = Some(node.ovb.len() as u8 - 1);
            }
            Some(ocur) => {
                let oblen: usize = node.oblen[ocur as usize] as usize;

                if crate::OV_MAX_BATCH_SIZE - oblen > 0 {
                    // use remaining space in batch
                    if node.target_uid.len() 
                        <= crate::OV_MAX_BATCH_SIZE - oblen 
                    {
                        target_uid = mem::take(&mut node.target_uid);
                        target_bid = mem::take(&mut node.target_bid);
                        target_id = mem::take(&mut node.target_id);
                    } else {
                        target_uid = node.target_uid.split_off(crate::OV_MAX_BATCH_SIZE as usize - oblen as usize);
                        std::mem::swap(&mut target_uid, &mut node.target_uid);
                        target_bid = node.target_bid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_bid, &mut node.target_bid);
                        target_id = node.target_id.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_id, &mut node.target_id);
                    }
                } else {
                    // create or select an OvB

                    if node.ovb.len() < crate::MAX_OV_BLOCKS {
                        // create a new OvB
                        node.ovb.push(Uuid::new_v4());
                        //node.ovb.push(AttributeValue::B(Blob::new(Uuid::new_v4() as bytes)));
                        node.obid.push(1);
                        node.ocur = Some(node.ovb.len() as u8 - 1);
                    } else {
                        let ocur: u8 = node.ocur.unwrap();

                        // pick from existing (round robin)
                        if let Some(mut ocur) = node.ocur {
                            ocur += 1;
                            if ocur as usize == crate::MAX_OV_BLOCKS {
                                ocur = 0;
                            }
                            node.ocur = Some(ocur);
                        } else {
                            node.ocur = Some(0);
                        }
                        node.obid[ocur as usize] += 1;
                    }

                    if node.target_uid.len() <= crate::OV_MAX_BATCH_SIZE {
                        target_uid = mem::take(&mut node.target_uid);
                        target_bid = mem::take(&mut node.target_bid);
                        target_id = mem::take(&mut node.target_id);
                    } else {
                        target_uid = node.target_uid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_uid, &mut node.target_uid);
                        target_bid = node.target_bid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_bid, &mut node.target_bid);
                        target_id = node.target_id.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_id, &mut node.target_id);
                    }
                }

                // ================
                // add OvB batches
                // ================
                let mut sk_w_bid = rkey.1.clone();
                sk_w_bid.push('%');
                sk_w_bid.push_str(&node.obid[ocur as usize].to_string());

                update_expression = "SET #target = :tuid, #bid=:bid, #id = id";

                let result = dyn_client
                    .update_item()
                    .table_name(table_name.clone())
                    .key(types::PK, AttributeValue::B(Blob::new(node.ovb[node.ocur.unwrap() as usize].as_bytes())))
                    .key(types::SK, AttributeValue::S(sk_w_bid.clone()))
                    .update_expression(update_expression)
                    // reverse edge
                    .expression_attribute_names("#target", types::TARGET_UID)
                    .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
                    .expression_attribute_names("#id", types::TARGET_BID)
                    .expression_attribute_values(":bid", AttributeValue::L(target_bid))
                    .expression_attribute_names("#id", types::TARGET_ID)
                    .expression_attribute_values(":id", AttributeValue::L(target_id))
                    //.return_values(ReturnValue::AllNew)
                    .send()
                    .await;

                handle_result(result);
            }
        }
    } // end while
      // update OvB meta on edge predicate only if OvB are used.
    if node.ovb.len() > 0 {
        update_expression = "SET #cnt = :cnt, #ovb = :ovb, #obid = :obid, #ocur = :ocur";
        let cnt = node.ovb.len() + init_cnt as usize;
        let ocur = match node.ocur {
            None => 0,
            Some(v) => v,
        };
        let result = dyn_client
            .update_item()
            .table_name(table_name.clone())
            .key(types::PK, AttributeValue::B(Blob::new(rkey.0.clone())))
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .update_expression(update_expression)
            // OvB metadata
            .expression_attribute_names("#cnt", types::CNT)
            .expression_attribute_values(":cnt", AttributeValue::N(cnt.to_string()))
            .expression_attribute_names("#ovb", types::OVB)
            .expression_attribute_values(":ovb", types::uuid_to_av_lb(&node.ovb))
            .expression_attribute_names("#obid", types::OVB_BID)
            .expression_attribute_values(":obid", types::u32_to_av_ln(&node.obid))
            .expression_attribute_names("#ocur", types::OVB_CUR)
            .expression_attribute_values(":ocur", AttributeValue::N(ocur.to_string()))
            //.return_values(ReturnValue::AllNew)
            .send()
            .await;

        handle_result(result);
    }
    // send task completed msg to evict service
    if let Err(err) = evict_completed_send_ch.send(rkey.clone()).await {
        println!(
            "Sending completed evict msg to waiting client failed: {}",
            err
        );
    }
}

fn  handle_result(result: Result<UpdateItemOutput, SdkError<UpdateItemError, HttpResponse>> ) {
    
    match result {
        Ok(_out) => { println!(" Evict Service: Persist successful update...")}
        Err(err) => {
            match err {
                SdkError::ConstructionFailure(_cf) => { println!(" Evict Service: Persist  update error ConstructionFailure...")},
                SdkError::TimeoutError(_te) => { println!(" Evict Service: Persist  update.error TimeoutError")},
                SdkError::DispatchFailure(_df) => { println!(" Evict Service: Persist  update error...DispatchFailure")},
                SdkError::ResponseError(_re) => { println!(" Evict Service: Persist  update. error ResponseError")},
                SdkError::ServiceError(_se) => { println!(" Evict Service: Persist  update. error ServiceError")},
                _ => {},
            }
        }
    }
}