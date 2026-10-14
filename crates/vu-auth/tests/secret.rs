//! `ClientCredentials::from_secret` against a mocked Secrets Manager (aws-smithy-mocks).

use aws_sdk_secretsmanager::Client;
use aws_sdk_secretsmanager::operation::get_secret_value::GetSecretValueOutput;
use aws_smithy_mocks::{RuleMode, mock, mock_client};
use vu_auth::ClientCredentials;

#[tokio::test]
async fn loads_client_credentials_from_json_secret() {
    let rule = mock!(Client::get_secret_value)
        .match_requests(|r| r.secret_id() == Some("arn:aws:secretsmanager:::secret/vu-oauth"))
        .then_output(|| {
            GetSecretValueOutput::builder()
                .secret_string(r#"{"client_id":"cid","client_secret":"shh"}"#)
                .build()
        });
    let client = mock_client!(aws_sdk_secretsmanager, RuleMode::Sequential, &[&rule]);

    let creds = ClientCredentials::from_secret(
        &client,
        "arn:aws:secretsmanager:::secret/vu-oauth",
        "https://idp.example/token".into(),
        Some("tads-api/write".into()),
    )
    .await
    .expect("secret should load");

    assert_eq!(creds.client_id, "cid");
    assert_eq!(creds.client_secret, "shh");
    assert_eq!(creds.token_url, "https://idp.example/token");
    assert_eq!(creds.scope.as_deref(), Some("tads-api/write"));
}

#[tokio::test]
async fn rejects_secret_without_string_value() {
    let rule =
        mock!(Client::get_secret_value).then_output(|| GetSecretValueOutput::builder().build());
    let client = mock_client!(aws_sdk_secretsmanager, RuleMode::Sequential, &[&rule]);

    let err = ClientCredentials::from_secret(&client, "arn", "https://idp/token".into(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, vu_auth::VuAuthError::SecretParse(_)));
}
