use crate::events::{Event, EventKey};
use anyhow::Result;
use aws_sdk_dynamodb::model::AttributeValue;
use aws_sdk_dynamodb::types::Blob;
use aws_sdk_dynamodb::Client;

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
        Self {
            client,
            table_name: table_name.to_string(),
        }
    }

    pub async fn write(&self, aggregate: Aggregate) -> Result<()> {
        let events = aggregate
            .events
            .iter()
            .map(|event| {
                AttributeValue::L(vec![
                    AttributeValue::S(event.primary_key.partition_key.clone()),
                    AttributeValue::S(event.primary_key.sort_key.clone()),
                    AttributeValue::S(event.name.clone()),
                    AttributeValue::B(Blob::new(event.payload.clone())),
                    AttributeValue::N(event.created_at.to_string()),
                ])
            })
            .collect::<Vec<_>>();

        self.client
            .put_item()
            .table_name(self.table_name.clone())
            .item("pkey", AttributeValue::S(aggregate.id))
            .item("events", AttributeValue::L(events))
            .item(
                "created_at",
                AttributeValue::N(aggregate.created_at.to_string()),
            )
            .item("version", AttributeValue::N(aggregate.version.to_string()))
            .send()
            .await?;
        Ok(())
    }

    pub async fn read(&self, id: &AggregateID) -> Result<Aggregate> {
        let response = self
            .client
            .get_item()
            .table_name(self.table_name.clone())
            .key("pkey", AttributeValue::S(id.clone()))
            .send()
            .await?;

        let item = response.item.unwrap();
        let events = item
            .get("events")
            .unwrap()
            .as_l()
            .unwrap()
            .iter()
            .map(|event| {
                let values = event.as_l().unwrap();
                Event::new(
                    EventKey::new(
                        values[0].as_s().unwrap().clone(),
                        values[1].as_s().unwrap().clone(),
                    ),
                    values[2].as_s().unwrap(),
                    values[3].as_b().unwrap().as_ref().to_vec(),
                    values[4].as_n().unwrap().parse::<u64>().unwrap(),
                )
            })
            .collect::<Vec<_>>();

        Ok(Aggregate {
            id: id.clone(),
            events,
            created_at: item
                .get("created_at")
                .unwrap()
                .as_n()
                .unwrap()
                .parse::<u64>()
                .unwrap(),
            version: item
                .get("version")
                .unwrap()
                .as_n()
                .unwrap()
                .parse::<u64>()
                .unwrap(),
        })
    }
}
