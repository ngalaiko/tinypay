mod events {
    mod reduce {
        #[test]
        fn empty() {
            let accounts = tinypay::events::reduce(&[]);
            assert!(accounts.is_empty())
        }

        mod transaction {

            mod deposit {
                #[test]
                fn success() {
                    let accounts = tinypay::events::reduce(&[tinypay::events::Event::Transaction(
                        tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        },
                    )]);
                    assert_eq!(accounts.len(), 1);

                    assert_eq!(accounts[0].client, 1);
                    assert_eq!(accounts[0].available, 100.0);
                    assert_eq!(accounts[0].held, 0.0);
                    assert_eq!(accounts[0].total, 100.0);
                    assert!(!accounts[0].locked);
                }

                #[test]
                fn duplicate() {
                    let accounts = tinypay::events::reduce(&[
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        }),
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        }),
                    ]);
                    assert_eq!(accounts.len(), 1);

                    assert_eq!(accounts[0].client, 1);
                    assert_eq!(accounts[0].available, 100.0);
                    assert_eq!(accounts[0].held, 0.0);
                    assert_eq!(accounts[0].total, 100.0);
                    assert!(!accounts[0].locked);
                }
            }

            mod withdrawal {
                #[test]
                fn sufficient() {
                    let accounts = tinypay::events::reduce(&[
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        }),
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 2,
                            client_id: 1,
                            amount: -50.0,
                        }),
                    ]);
                    assert_eq!(accounts.len(), 1);

                    assert_eq!(accounts[0].client, 1);
                    assert_eq!(accounts[0].available, 50.0);
                    assert_eq!(accounts[0].held, 0.0);
                    assert_eq!(accounts[0].total, 50.0);
                    assert!(!accounts[0].locked);
                }

                #[test]
                fn not_enough() {
                    let accounts = tinypay::events::reduce(&[
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        }),
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 2,
                            client_id: 1,
                            amount: -150.0,
                        }),
                    ]);

                    assert_eq!(accounts.len(), 1);

                    assert_eq!(accounts[0].client, 1);
                    assert_eq!(accounts[0].available, 100.0);
                    assert_eq!(accounts[0].held, 0.0);
                    assert_eq!(accounts[0].total, 100.0);
                    assert!(!accounts[0].locked);
                }
            }

            #[test]
            fn duplicate() {
                let accounts = tinypay::events::reduce(&[
                    tinypay::events::Event::Transaction(tinypay::events::Transaction {
                        id: 1,
                        client_id: 1,
                        amount: 100.0,
                    }),
                    tinypay::events::Event::Transaction(tinypay::events::Transaction {
                        id: 1,
                        client_id: 1,
                        amount: 100.0,
                    }),
                ]);

                assert_eq!(accounts.len(), 1);

                assert_eq!(accounts[0].client, 1);
                assert_eq!(accounts[0].available, 100.0);
                assert_eq!(accounts[0].held, 0.0);
                assert_eq!(accounts[0].total, 100.0);
                assert!(!accounts[0].locked);
            }
        }

        mod dispute {
            #[test]
            fn open() {
                let accounts = tinypay::events::reduce(&[
                    tinypay::events::Event::Transaction(tinypay::events::Transaction {
                        id: 1,
                        client_id: 1,
                        amount: 100.0,
                    }),
                    tinypay::events::Event::Dispute(tinypay::events::Dispute {
                        transaction_id: 1,
                        client_id: 1,
                    }),
                ]);

                assert_eq!(accounts.len(), 1);

                assert_eq!(accounts[0].client, 1);
                assert_eq!(accounts[0].available, 0.0);
                assert_eq!(accounts[0].held, 100.0);
                assert_eq!(accounts[0].total, 100.0);
                assert!(!accounts[0].locked);
            }

            #[test]
            fn ignored() {
                let accounts = tinypay::events::reduce(&[
                    tinypay::events::Event::Transaction(tinypay::events::Transaction {
                        id: 1,
                        client_id: 1,
                        amount: 100.0,
                    }),
                    tinypay::events::Event::Dispute(tinypay::events::Dispute {
                        transaction_id: 2,
                        client_id: 1,
                    }),
                ]);

                assert_eq!(accounts.len(), 1);

                assert_eq!(accounts[0].client, 1);
                assert_eq!(accounts[0].available, 100.0);
                assert_eq!(accounts[0].held, 0.0);
                assert_eq!(accounts[0].total, 100.0);
                assert!(!accounts[0].locked);
            }

            mod resolve {
                #[test]
                fn ignored() {
                    let accounts = tinypay::events::reduce(&[
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        }),
                        tinypay::events::Event::Dispute(tinypay::events::Dispute {
                            transaction_id: 1,
                            client_id: 1,
                        }),
                        tinypay::events::Event::Resolve(tinypay::events::Resolve {
                            transaction_id: 2,
                            client_id: 1,
                        }),
                    ]);

                    assert_eq!(accounts.len(), 1);

                    assert_eq!(accounts[0].client, 1);
                    assert_eq!(accounts[0].available, 0.0);
                    assert_eq!(accounts[0].held, 100.0);
                    assert_eq!(accounts[0].total, 100.0);
                    assert!(!accounts[0].locked);
                }

                #[test]
                fn success() {
                    let accounts = tinypay::events::reduce(&[
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        }),
                        tinypay::events::Event::Dispute(tinypay::events::Dispute {
                            transaction_id: 1,
                            client_id: 1,
                        }),
                        tinypay::events::Event::Resolve(tinypay::events::Resolve {
                            transaction_id: 1,
                            client_id: 1,
                        }),
                    ]);

                    assert_eq!(accounts.len(), 1);

                    assert_eq!(accounts[0].client, 1);
                    assert_eq!(accounts[0].available, 100.0);
                    assert_eq!(accounts[0].held, 0.0);
                    assert_eq!(accounts[0].total, 100.0);
                    assert!(!accounts[0].locked);
                }
            }

            mod chargeback {
                #[test]
                fn illegal() {
                    let accounts = tinypay::events::reduce(&[
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        }),
                        tinypay::events::Event::Dispute(tinypay::events::Dispute {
                            transaction_id: 1,
                            client_id: 1,
                        }),
                        tinypay::events::Event::Chargeback(tinypay::events::Chargeback {
                            transaction_id: 1,
                            client_id: 1,
                        }),
                    ]);

                    assert_eq!(accounts.len(), 1);

                    assert_eq!(accounts[0].client, 1);
                    assert_eq!(accounts[0].available, 0.0);
                    assert_eq!(accounts[0].held, 0.0);
                    assert_eq!(accounts[0].total, 0.0);
                    assert!(!accounts[0].locked);
                }

                #[test]
                fn legal() {
                    let accounts = tinypay::events::reduce(&[
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        }),
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 2,
                            client_id: 1,
                            amount: -50.0,
                        }),
                        tinypay::events::Event::Dispute(tinypay::events::Dispute {
                            transaction_id: 1,
                            client_id: 1,
                        }),
                        tinypay::events::Event::Chargeback(tinypay::events::Chargeback {
                            transaction_id: 1,
                            client_id: 1,
                        }),
                    ]);

                    assert_eq!(accounts.len(), 1);

                    assert_eq!(accounts[0].client, 1);
                    assert_eq!(accounts[0].available, -50.0);
                    assert_eq!(accounts[0].held, 100.0);
                    assert_eq!(accounts[0].total, 50.0);
                    assert!(accounts[0].locked);
                }

                #[test]
                fn ignored() {
                    let accounts = tinypay::events::reduce(&[
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 1,
                            client_id: 1,
                            amount: 100.0,
                        }),
                        tinypay::events::Event::Transaction(tinypay::events::Transaction {
                            id: 2,
                            client_id: 1,
                            amount: -50.0,
                        }),
                        tinypay::events::Event::Dispute(tinypay::events::Dispute {
                            transaction_id: 1,
                            client_id: 1,
                        }),
                        tinypay::events::Event::Chargeback(tinypay::events::Chargeback {
                            transaction_id: 2,
                            client_id: 1,
                        }),
                    ]);

                    assert_eq!(accounts.len(), 1);

                    assert_eq!(accounts[0].client, 1);
                    assert_eq!(accounts[0].available, -50.0);
                    assert_eq!(accounts[0].held, 100.0);
                    assert_eq!(accounts[0].total, 50.0);
                    assert!(!accounts[0].locked);
                }
            }
        }
    }
}
