mod dynamodb_local;

use aws_sdk_dynamodb::Client;
use dynamodb_local::*;

use aws_sdk_dynamodb::model::{AttributeDefinition, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType};
use testcontainers::{clients, images};
use event_store_rs::events::{Event, EventIO, EventKey};

async fn create_table(dynamodb: &Client, table_name: &str) {
    let pkey_key_schema = KeySchemaElement::builder()
        .attribute_name("pkey".to_string())
        .key_type(KeyType::Hash)
        .build();

    let skey_key_schema = KeySchemaElement::builder()
        .attribute_name("skey".to_string())
        .key_type(KeyType::Range)
        .build();

    let pkey_attr_def = AttributeDefinition::builder()
        .attribute_name("pkey".to_string())
        .attribute_type(ScalarAttributeType::S)
        .build();

    let skey_attr_def = AttributeDefinition::builder()
        .attribute_name("skey".to_string())
        .attribute_type(ScalarAttributeType::S)
        .build();

    let provisioned_throughput = ProvisionedThroughput::builder()
        .read_capacity_units(10)
        .write_capacity_units(5)
        .build();
    let create_table_result = dynamodb
        .create_table()
        .table_name(table_name.clone())
        .set_key_schema(Some(vec![pkey_key_schema, skey_key_schema]))
        .set_attribute_definitions(Some(vec![pkey_attr_def, skey_attr_def]))
        .provisioned_throughput(provisioned_throughput)
        .send()
        .await;
    assert!(create_table_result.is_ok());

    let req = dynamodb.list_tables().limit(10);
    let list_tables_result = req.send().await.unwrap();

    assert_eq!(list_tables_result.table_names().unwrap().len(), 1);
}



#[tokio::test]
async fn test_dynamodb_local() {
    let docker = clients::Cli::default();
    let node = docker.run(images::dynamodb_local::DynamoDb::default());
    let host_port = node.get_host_port_ipv4(8000);
    let client = build_dynamodb_client(host_port).await;
    let table_name = "events";

    create_table(&client, table_name).await;

    let event_io = EventIO::new(client, table_name);

    let event = Event::new(EventKey::new("1".to_string(), "1".to_string()), "test", vec![1u8, 2, 3], 1);
    event_io.write(event.clone()).await.unwrap();

    let id = event.primary_key().clone();
    let actual = event_io.read(&id).await.unwrap();
    assert_eq!(actual, event);
}