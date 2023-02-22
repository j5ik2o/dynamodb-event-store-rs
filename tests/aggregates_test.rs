use aws_sdk_dynamodb::model::{
    AttributeDefinition, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;

async fn create_table(dynamodb: &Client, table_name: &str) {
    let pkey_key_schema = KeySchemaElement::builder()
        .attribute_name("pkey".to_string())
        .key_type(KeyType::Hash)
        .build();

    let pkey_attr_def = AttributeDefinition::builder()
        .attribute_name("pkey".to_string())
        .attribute_type(ScalarAttributeType::S)
        .build();

    let provisioned_throughput = ProvisionedThroughput::builder()
        .read_capacity_units(10)
        .write_capacity_units(5)
        .build();
    let create_table_result = dynamodb
        .create_table()
        .table_name(table_name.clone())
        .set_key_schema(Some(vec![pkey_key_schema]))
        .set_attribute_definitions(Some(vec![pkey_attr_def]))
        .provisioned_throughput(provisioned_throughput)
        .send()
        .await;
    assert!(create_table_result.is_ok());

    let req = dynamodb.list_tables().limit(10);
    let list_tables_result = req.send().await.unwrap();

    assert_eq!(list_tables_result.table_names().unwrap().len(), 1);
}

#[tokio::test]
async fn test_dynamodb_local() {}
