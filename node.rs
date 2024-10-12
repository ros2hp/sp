use super::*;

use crate::types;
use aws_sdk_dynamodb::types::AttributeValue;

use uuid::Uuid;

// NodeState is a redundant concept. Mutex lock will mean concurrent access will prevent others see the State while it is in transition.
// Maybe useful to debug purposes.
#[derive(Clone)]
pub enum NodeState {
    Evicting,
    Loading,
    Available,
}

#[derive(Clone)]
pub struct RNode {
    pub node: Uuid,     // child or associated OvB Uuid
    pub rvs_sk: String, // child or associated OvB batch SK
    //
    pub state: NodeState,
    // edge count at node initialisation (new or db sourced)
    pub init_cnt: u32,
    // accumlate edge data into these Vec's
    pub target_uid: Vec<AttributeValue>,
    pub target_bid: Vec<AttributeValue>,
    pub target_id: Vec<AttributeValue>,
    // metadata that describes how to populate target* into db attributes when persisted
    pub ovb: Vec<Uuid>,  // Uuid of OvB
    pub obid: Vec<u32>,  // current batch id in each OvB
    pub obcnt: Vec<u32>, // edge count in batch
    pub oblen: Vec<u32>, // count of itmes in current batch across OvBs
    pub oid: Vec<u32>,
    pub ocur: Option<u8>, // current Ovb in use
    //
    //
    //update_expression: String,
    // on_evict : Option<Box<dyn Fn()>>,
    pub next: Option<Arc<tokio::sync::Mutex<RNode>>>,
    pub prev: Option<Weak<tokio::sync::Mutex<RNode>>>,
}

impl RNode {
    pub fn new() -> RNode {
        RNode {
            node: Uuid::nil(),
            rvs_sk: String::new(), //
            state: NodeState::Loading, //
            init_cnt: 0,           // edge cnt at initialisation (e.g as read from database)
                                   //
            target_uid: vec![],
            target_bid: vec![],
            target_id: vec![], //
            ovb: vec![],
            obid: vec![],
            oblen: vec![],
            obcnt: vec![],
            oid: vec![],
            ocur: None, //
            next: None,
            prev: None,
        }
    }

    pub fn new_with_key(rkey: &RKey) -> RNode {
        RNode {
            node: rkey.0.clone(),
            rvs_sk: rkey.1.clone(), //
            state: NodeState::Loading, //
            init_cnt: 0,            //
            target_uid: vec![],     // target_uid.len() total edges added in current sp session
            target_bid: vec![],
            target_id: vec![], //
            ovb: vec![],
            obcnt: vec![],
            oblen: vec![],
            obid: vec![],
            oid: vec![],
            ocur: None, //
            next: None,
            prev: None,
        }
    }

    pub async fn load_from_db(
        &mut self,
        dyn_client: &DynamoClient,
        table_name: &str,
        rkey: &RKey,
    ) {
        let result = dyn_client
            .get_item()
            .table_name(table_name)
            .key(
                types::PK,
                AttributeValue::B(Blob::new(rkey.0.clone().as_bytes())),
            )
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .projection_expression((&*LOAD_PROJ).clone())
            .send()
            .await;

        if let Err(err) = result {
            panic!(
                "get node type: no item found: expected a type value for node. Error: {}",
                err
            )
        }
        let mut ri: RNode = match result.unwrap().item {
            None => RNode::new(),
            Some(v) => v.into(),
        };
        self.state = NodeState::Available;
        // update self with db data
        self.init_cnt = ri.init_cnt;
        self.target_uid = ri.target_uid;
        self.target_bid = ri.target_bid;
        self.target_id = ri.target_id;
        self.ovb = ri.ovb;   
        self.obid = ri.obid; 
        self.obcnt = ri.obcnt; 
        self.oblen = ri.oblen; 
        self.oid = ri.oid;
        self.ocur = ri.ocur;
    }

    // fn update(&mut self, e : Self) {
    //     self.node = e.node;
    //     self.rvs_sk = e.rvs_sk;
    //     //
    //     self.cnt = e.cnt;
    //     self.ovb = e.ovb;
    //     self.obid = e.obid;
    //     self.ocur = e.ocur;
    // }

    pub fn add_reverse_edge(&mut self, target_uid: Uuid, target_bid: u32, target_id: u32) {
        //self.cnt += 1; // redundant, use container_uuid.len() and add it to db cnt attribute.
        // accumulate edges into these Vec's. Distribute the data across Dynamodb attributes (aka OvB batches) when persisting to database.
        self.target_uid
            .push(AttributeValue::B(Blob::new(target_uid.as_bytes())));
        self.target_bid
            .push(AttributeValue::N(target_bid.to_string()));
        self.target_id
            .push(AttributeValue::N(target_id.to_string()));
        
    }

    pub fn set_prev(&mut self, v: Weak<tokio::sync::Mutex<RNode>>) {
        self.prev = Some(v)
    }

    pub fn set_next(&mut self, v: Arc<tokio::sync::Mutex<RNode>>) {
        self.next = Some(v)
    }

    //
}

// Populate reverse cache with return values from Dynamodb.
// note: not interested in TARGET* attributes only OvB about TARGET*
impl From<HashMap<String, AttributeValue>> for RNode {
    //    HashMap.into() -> RNode

    fn from(mut value: HashMap<String, AttributeValue>) -> Self {
        let mut edge = RNode::new();

        for (k, v) in value.drain() {
            match k.as_str() {
                types::PK => edge.node = types::as_uuid(v).unwrap(),
                types::SK => edge.rvs_sk = types::as_string(v).unwrap(),
                //
                types::CNT => edge.init_cnt = types::as_u32_2(v).unwrap(),
                //
                types::OVB => edge.ovb = types::as_luuid(v).unwrap(),
                types::OVB_CNT => edge.obcnt = types::as_lu32(v).unwrap(),
                types::OVB_BID => edge.obid = types::as_lu32(v).unwrap(),
                types::OVB_ID => edge.oid = types::as_lu32(v).unwrap(),
                types::OVB_CUR => edge.ocur = types::as_u8_2(v),
                _ => panic!(
                    "unexpected attribute in HashMap for RNode: [{}]",
                    k.as_str()
                ),
            }
        }
        edge
    }
}
