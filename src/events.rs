use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::model::AttributeValue;
use aws_sdk_dynamodb::types::Blob;
use anyhow::Result;

pub type PK = String;
pub type SK = String;

#[derive(Debug, Clone, PartialEq)]
pub struct EventKey {
    pub partition_key: PK,
    pub sort_key: SK,
}

impl EventKey {
    pub fn new(partition_key: PK, sort_key: SK) -> Self {
        Self { partition_key, sort_key }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub primary_key: EventKey,
    pub name: String,
    pub payload: Vec<u8>,
    pub created_at: u64,
}

impl Event {
    pub fn new(primary_key: EventKey, name: &str, payload: Vec<u8>, created_at: u64) -> Self {
        Self { primary_key, name: name.to_string(), payload, created_at }
    }

    pub fn primary_key(&self) -> &EventKey {
        &self.primary_key
    }
}

pub struct EventIO {
    client: Client,
    table_name: String,
}

impl EventIO {
    pub fn new(client: Client, table_name: &str) -> Self {
        Self { client , table_name: table_name.to_string() }
    }

    pub async fn write(&self, event: Event) -> Result<()> {
        self.client.put_item()
            .table_name(self.table_name.clone())
            .item("pkey", AttributeValue::S(event.primary_key.partition_key))
            .item("skey", AttributeValue::S(event.primary_key.sort_key))
            .item("name", AttributeValue::S(event.name))
            .item("payload", AttributeValue::B(Blob::new(event.payload)))
            .item("created_at", AttributeValue::N(event.created_at.to_string()))
            .send().await?;
        Ok(())
    }

    pub async fn read(&self, id: &EventKey) -> Result<Event> {
        let result = self.client.get_item()
            .table_name(self.table_name.clone())
            .key("pkey", AttributeValue::S(id.partition_key.clone()))
            .key("skey", AttributeValue::S(id.sort_key.clone()))
            .send().await?;
        let item = result.item.unwrap();
        let pkey = item.get("pkey").unwrap().as_s().as_ref().unwrap().to_string();
        let skey = item.get("skey").unwrap().as_s().as_ref().unwrap().to_string();
        let name = item.get("name").unwrap().as_s().as_ref().unwrap().to_string();
        let payload = item.get("payload").unwrap().as_b().unwrap().as_ref().to_vec();
        let created_at = item.get("created_at").unwrap().as_n().as_ref().unwrap().parse::<u64>().unwrap();
        Ok(Event {
            primary_key: EventKey { partition_key: pkey, sort_key: skey },
            name,
            payload,
            created_at,
        })
    }
}