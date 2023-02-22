use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::{Client, Credentials};

pub async fn build_dynamodb_client(host_port: u16) -> Client {
    let endpoint_uri = format!("http://127.0.0.1:{}", host_port);
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let creds = Credentials::new("fakeKey", "fakeSecret", None, None, "test");

    let shared_config = aws_config::from_env()
        .region(region_provider)
        .endpoint_url(endpoint_uri)
        .credentials_provider(creds)
        .load()
        .await;

    Client::new(&shared_config)
}
