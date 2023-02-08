use aws_sdk_dynamodb::Client;
use crate::events::Event;
use anyhow::Result;
use aws_sdk_dynamodb::model::AttributeValue;
use aws_sdk_dynamodb::types::Blob;

pub type AggregateID = String;

pub struct Aggregate {
    id: AggregateID,
    events: Vec<Event>,
    created_at: u64,
    version: u64,
}

pub struct AggregateIO {
    client: Client,
    table_name: String,
}

impl AggregateIO {
    pub fn new(client: Client, table_name: &str) -> Self {
        Self { client, table_name: table_name.to_string() }
    }

    pub async fn write(&self, aggregate: Aggregate) -> Result<()> {
        let events = aggregate.events.iter().map(|event| {
            AttributeValue::L(vec![AttributeValue::S(event.primary_key.partition_key.clone()),
                                   AttributeValue::S(event.primary_key.sort_key.clone()),
                                   AttributeValue::S(event.name.clone()),
                                   AttributeValue::B(Blob::new(event.payload.clone())),
                                   AttributeValue::N(event.created_at.to_string())])
        }).collect::<Vec<_>>();

        self.client.put_item()
            .table_name(self.table_name.clone())
            .item("pkey", AttributeValue::S(aggregate.id))
            .item("events", AttributeValue::L(events))
            .item("created_at", AttributeValue::N(aggregate.created_at.to_string()))
            .item("version", AttributeValue::N(aggregate.version.to_string()))
            .send().await?;
        Ok(())
    }
}