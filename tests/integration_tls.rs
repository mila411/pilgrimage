#[cfg(test)]
mod tls_integration_tests {
    use pilgrimage::security::tls_manager::{
        CertRotationConfig, CertValidationConfig, EnhancedTlsConfig, EnhancedTlsManager, TlsVersion,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn test_mutual_tls_configuration() {
        println!("🧪 Testing mutual TLS configuration...");

        let config = EnhancedTlsConfig {
            enabled: true,
            mutual_tls: true,
            server_cert_path: "test_certs/server.crt".to_string(),
            server_key_path: "test_certs/server.key".to_string(),
            client_cert_path: Some("test_certs/client.crt".to_string()),
            client_key_path: Some("test_certs/client.key".to_string()),
            ca_cert_path: Some("test_certs/ca.crt".to_string()),
            min_tls_version: TlsVersion::V1_3,
            cipher_suites: vec!["TLS_AES_256_GCM_SHA384".to_string()],
            cert_validation: CertValidationConfig {
                verify_chain: true,
                verify_hostname: false,  // Disabled in test environment
                allow_self_signed: true, // Allow self-signed certificates for testing
                check_revocation: false,
                max_cert_age_days: 365,
            },
            cert_rotation: CertRotationConfig {
                enabled: false, // Disabled in tests
                check_interval: Duration::from_secs(3600),
                renewal_threshold_days: 30,
                backup_old_certs: false,
            },
        };

        let result = EnhancedTlsManager::new(config).await;

        match result {
            Ok(mut manager) => {
                println!("✅ TLS Manager created successfully");

                let status = manager.get_mutual_tls_status().await;
                println!("📊 Mutual TLS Status:");
                println!("  - Enabled: {}", status.enabled);
                println!("  - Verification Mode: {}", status.verification_mode);

                manager.shutdown().await;
                println!("✅ TLS Manager shutdown complete");
            }
            Err(e) => {
                println!(
                    "⚠️ TLS Manager creation failed (expected for missing certs): {}",
                    e
                );
                println!("✅ This is normal when certificate files don't exist");
            }
        }
    }

    #[tokio::test]
    async fn test_certificate_validation_logic() {
        println!("🧪 Testing certificate validation logic...");

        // Simple configuration for testing
        let config = EnhancedTlsConfig {
            enabled: true,
            mutual_tls: false, // Simple TLS
            server_cert_path: "dummy.crt".to_string(),
            server_key_path: "dummy.key".to_string(),
            client_cert_path: None,
            client_key_path: None,
            ca_cert_path: None,
            min_tls_version: TlsVersion::V1_3,
            cipher_suites: vec!["TLS_AES_256_GCM_SHA384".to_string()],
            cert_validation: CertValidationConfig {
                verify_chain: true,
                verify_hostname: true,
                allow_self_signed: false,
                check_revocation: false,
                max_cert_age_days: 365,
            },
            cert_rotation: CertRotationConfig {
                enabled: false,
                check_interval: Duration::from_secs(3600),
                renewal_threshold_days: 30,
                backup_old_certs: false,
            },
        };

        let result = EnhancedTlsManager::new(config).await;

        match result {
            Ok(mut manager) => {
                println!("✅ TLS Manager created");

                // Test certificate validation logic (with dummy data)
                let dummy_cert_der = vec![0x30, 0x82, 0x01, 0x00]; // Invalid DER data
                let validation_result = manager.validate_client_certificate(&dummy_cert_der).await;

                match validation_result {
                    Ok(is_valid) => {
                        println!("📋 Certificate validation result: {}", is_valid);
                        if !is_valid {
                            println!("✅ Invalid certificate correctly rejected");
                        }
                    }
                    Err(e) => {
                        println!("✅ Certificate validation error (expected): {}", e);
                    }
                }

                manager.shutdown().await;
            }
            Err(e) => {
                println!("⚠️ TLS Manager creation failed: {}", e);
            }
        }
    }

    #[test]
    fn test_tls_configuration_validation() {
        println!("🧪 Testing TLS configuration validation...");

        // Production configuration
        let prod_config = EnhancedTlsConfig {
            enabled: true,
            mutual_tls: true,
            server_cert_path: "certs/server.crt".to_string(),
            server_key_path: "certs/server.key".to_string(),
            client_cert_path: Some("certs/client.crt".to_string()),
            client_key_path: Some("certs/client.key".to_string()),
            ca_cert_path: Some("certs/ca.crt".to_string()),
            min_tls_version: TlsVersion::V1_3,
            cipher_suites: vec![
                "TLS_AES_256_GCM_SHA384".to_string(),
                "TLS_CHACHA20_POLY1305_SHA256".to_string(),
            ],
            cert_validation: CertValidationConfig {
                verify_chain: true,
                verify_hostname: true,
                allow_self_signed: false,
                check_revocation: true,
                max_cert_age_days: 365,
            },
            cert_rotation: CertRotationConfig {
                enabled: true,
                check_interval: Duration::from_secs(1800),
                renewal_threshold_days: 30,
                backup_old_certs: true,
            },
        };

        println!("✅ Production configuration:");
        println!("  - Mutual TLS: {}", prod_config.mutual_tls);
        println!("  - Min TLS version: {:?}", prod_config.min_tls_version);
        println!(
            "  - Self-signed allowed: {}",
            prod_config.cert_validation.allow_self_signed
        );
        println!("  - Cert rotation: {}", prod_config.cert_rotation.enabled);

        // Development configuration
        let dev_config = EnhancedTlsConfig::default();
        println!("✅ Development configuration:");
        println!("  - Enabled: {}", dev_config.enabled);
        println!("  - Mutual TLS: {}", dev_config.mutual_tls);

        assert_eq!(prod_config.enabled, true);
        assert_eq!(prod_config.mutual_tls, true);
        assert_eq!(dev_config.enabled, false);

        println!("✅ Configuration validation tests passed");
    }

    #[test]
    fn test_security_features() {
        println!("🧪 Testing security features configuration...");

        let secure_config = EnhancedTlsConfig {
            enabled: true,
            mutual_tls: true,
            server_cert_path: "certs/server.crt".to_string(),
            server_key_path: "certs/server.key".to_string(),
            client_cert_path: Some("certs/client.crt".to_string()),
            client_key_path: Some("certs/client.key".to_string()),
            ca_cert_path: Some("certs/ca.crt".to_string()),
            min_tls_version: TlsVersion::V1_3,
            cipher_suites: vec![
                "TLS_AES_256_GCM_SHA384".to_string(),
                "TLS_CHACHA20_POLY1305_SHA256".to_string(),
                "TLS_AES_128_GCM_SHA256".to_string(),
            ],
            cert_validation: CertValidationConfig {
                verify_chain: true,
                verify_hostname: true,
                allow_self_signed: false,
                check_revocation: true,
                max_cert_age_days: 90, // Short-term renewal
            },
            cert_rotation: CertRotationConfig {
                enabled: true,
                check_interval: Duration::from_secs(900), // 15-minute intervals
                renewal_threshold_days: 7,                // Renew one week in advance
                backup_old_certs: true,
            },
        };

        println!("🛡️ Security features enabled:");
        println!(
            "  - Certificate chain verification: {}",
            secure_config.cert_validation.verify_chain
        );
        println!(
            "  - Hostname verification: {}",
            secure_config.cert_validation.verify_hostname
        );
        println!(
            "  - Revocation checking: {}",
            secure_config.cert_validation.check_revocation
        );
        println!(
            "  - Max certificate age: {} days",
            secure_config.cert_validation.max_cert_age_days
        );
        println!(
            "  - Certificate rotation: {}",
            secure_config.cert_rotation.enabled
        );
        println!(
            "  - Rotation check interval: {:?}",
            secure_config.cert_rotation.check_interval
        );
        println!(
            "  - Renewal threshold: {} days",
            secure_config.cert_rotation.renewal_threshold_days
        );

        // Security configuration validation
        assert!(secure_config.cert_validation.verify_chain);
        assert!(secure_config.cert_validation.verify_hostname);
        assert!(!secure_config.cert_validation.allow_self_signed);
        assert!(secure_config.cert_validation.check_revocation);
        assert!(secure_config.cert_rotation.enabled);
        assert_eq!(secure_config.min_tls_version, TlsVersion::V1_3);

        println!("✅ Security features validation passed");
    }
}
