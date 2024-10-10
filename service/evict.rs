use crate::types;

use crate::node::RNode;
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

const MAX_EVICT_TASKS: u8 = 9;

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
    mut evict_submit_rx: tokio::sync::mpsc::Receiver<(RKey, Arc<tokio::sync::Mutex<RNode>>)>,
    mut client_query_rx: tokio::sync::mpsc::Receiver<QueryMsg>,
    mut shutdown_ch: tokio::sync::broadcast::Receiver<u8>,
) -> task::JoinHandle<u32> {
    println!("evict service...started.");

    let table_name = table_name_.into();

    //let mut start = Instant::now();

    println!("start evict service: table [{}] ", table_name);

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
                //biased;         // removes random number generation - normal processing will determine order so select! can follow it.
                // note: recv() is cancellable, meaning select! can cancel a recv() without loosing data in the channel.
                // select! will be forced to cancel recv() if another branch event happens e.g. recv() on shutdown_channel.
                Some((rkey, arc_node)) = evict_submit_rx.recv() => {

                    pending.0.insert(rkey.clone());

                    tasks += 1;

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
                    pending.0.remove(&evict_rkey);
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

                    let result = match pending.0.contains(&query_msg.0) {
                        true => {
                            query_client.0.insert(query_msg.0.clone(), query_msg.1.clone());
                            true
                            },
                        false => false
                    };
                    if let Err(err) = query_msg.1.send(result).await {
                        panic!("Error in sending query_msg [{}]",err)
                    };
                },

                _ = shutdown_ch.recv() => {
                        println!("Shutdown of evict service started. Waiting for remaining evict tasks to complete...");
                        while tasks > 0 {
                        println!("...waiting on {} tasks",tasks);
                           let Some(evict_rkey) = evict_completed_rx.recv().await else {panic!("none...")};
                            tasks-=1;
                            // send to client if one is waiting on query channel. Does not block as buffer is 1.
                            if let Some(client_ch) = query_client.0.get(&evict_rkey) {
                                // send ack of completed eviction to waiting client
                                if let Err(err) = client_ch.send(true).await {
                                    panic!("Error in sending to waiting client that rkey is evicited [{}]",err)
                                }
                                //
                                query_client.0.remove(&evict_rkey);
                            }  
                        }
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
    // ASSUMPTION: most nodes have less than embedded edges so update with list_append
    // remember that Dynamodb will scan the entire List attribute before appending, so List should be relatively small < 10000.
    let mut node = arc_node.lock().await;
    let table_name: String = table_name_.into();
    let mut target_uid: Vec<AttributeValue>;
    let mut target_bid: Vec<AttributeValue>;
    let mut target_id: Vec<AttributeValue>;
    let mut update_expression: &str;
    let edge_cnt = node.target_uid.len();
    let init_cnt = node.init_cnt as usize;

    if init_cnt <= crate::EMBEDDED_CHILD_NODES  {

        if node.target_uid.len() <= crate::EMBEDDED_CHILD_NODES - init_cnt {
            // consume all of node.target*
            target_uid = mem::take(&mut node.target_uid);
            target_bid = mem::take(&mut node.target_bid);
            target_id = mem::take(&mut node.target_id);
        } else {
            // consume portion of node.target*
            target_uid = node
                .target_uid
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_uid, &mut node.target_uid);
            target_bid = node
                .target_bid
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_bid, &mut node.target_bid);
            target_id = node
                .target_id
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_id, &mut node.target_id);
        }

        if init_cnt == 0 {
            // no data in db
            update_expression = "SET #target = :tuid, #bid = :bid, #id = id, #cnt = :cnt, #db_sourced : :db_sourced";
        } else {
            // append to existing data
            update_expression = "SET #target=LIST_APPEND(#target, :tuid), #bid=LIST_APPEND(#bid,:bid), #id=LIST_APPEND(#id,:id), #cnt = #cnt+:cnt";
        }
        // update edge item
        let result = dyn_client
            .update_item()
            .table_name(table_name.clone())
            .key(types::PK, AttributeValue::B(Blob::new(rkey.0.clone())))
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .update_expression(update_expression)
            // reverse edge
            .expression_attribute_names("#cnt", types::CNT)
            .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
            .expression_attribute_names("#target", types::OVB)
            .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
            .expression_attribute_names("#bid", types::OVB_BID)
            .expression_attribute_values(":bid", AttributeValue::L(target_bid))
            .expression_attribute_names("#id", types::OVB_ID)
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
                    } else {
                        target_uid = node
                            .target_uid
                            .split_off(crate::OV_MAX_BATCH_SIZE as usize - oblen as usize);
                        std::mem::swap(&mut target_uid, &mut node.target_uid);
                        target_bid = node.target_bid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_bid, &mut node.target_bid);
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

                update_expression = "SET #target = :tuid, #bid = :bid, #id = id";

                let result = dyn_client
                    .update_item()
                    .table_name(table_name.clone())
                    .key(types::PK, AttributeValue::B(Blob::new(rkey.0.clone())))
                    .key(types::SK, AttributeValue::S(rkey.1.clone()))
                    .update_expression(update_expression)
                    // reverse edge
                    .expression_attribute_names("#target", types::OVB)
                    .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
                    .expression_attribute_names("#bid", types::OVB_BID)
                    .expression_attribute_values(":bid", AttributeValue::L(target_bid))
                    .expression_attribute_names("#id", types::OVB_ID)
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
            .expression_attribute_names("#ovb", types::OVB_BID)
            .expression_attribute_values(":ovb", types::u32_to_av_ln(&node.obid))
            .expression_attribute_names("#ovb", types::OVB_CUR)
            .expression_attribute_values(":ovb", AttributeValue::N(node.ocur.unwrap().to_string()))
            //.return_values(ReturnValue::AllNew)
            .send()
            .await;

        handle_result(result);
    }
    // send evict completed msg to waiting client
    if let Err(err) = evict_completed_send_ch.send(rkey.clone()).await {
        println!(
            "Sending completed evict msg to waiting client failed: {}",
            err
        );
    }
}

fn  handle_result(result: Result<UpdateItemOutput, SdkError<UpdateItemError, HttpResponse>> ) {
    
    match result {
        Ok(_out) => {}
        Err(err) => {
            match err {
                SdkError::ConstructionFailure(_cf) => {},
                SdkError::TimeoutError(_te) => {},
                SdkError::DispatchFailure(_df) => {},
                SdkError::ResponseError(_re) => {},
                SdkError::ServiceError(_se) => {},
                _ => {},
            }
        }
    }
}