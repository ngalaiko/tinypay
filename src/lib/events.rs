pub mod csv;

use std::collections::{HashMap, HashSet};

use crate::accounts;

#[derive(Debug)]
pub struct Transaction {
    pub id: u32,
    pub client_id: u16,
    pub amount: f64,
}

#[derive(Debug)]
pub struct Dispute {
    pub transaction_id: u32,
    pub client_id: u16,
}

#[derive(Debug)]
pub struct Resolve {
    pub transaction_id: u32,
    pub client_id: u16,
}

#[derive(Debug)]
pub struct Chargeback {
    pub transaction_id: u32,
    pub client_id: u16,
}

#[derive(Debug)]
pub enum Event {
    Transaction(Transaction),
    Dispute(Dispute),
    Resolve(Resolve),
    Chargeback(Chargeback),
}

impl Event {
    pub fn client_id(&self) -> u16 {
        match self {
            Event::Transaction(transaction) => transaction.client_id,
            Event::Dispute(dispute) => dispute.client_id,
            Event::Resolve(resolve) => resolve.client_id,
            Event::Chargeback(chargeback) => chargeback.client_id,
        }
    }
}

pub fn reduce(events: &[Event]) -> Vec<accounts::Account> {
    let mut accounts = std::collections::HashMap::<u16, accounts::Account>::new();
    let mut transactions = std::collections::HashMap::<u16, HashMap<u32, f64>>::new();
    let mut disputes = std::collections::HashMap::<u16, HashSet<u32>>::new();
    for event in events {
        let client_id = event.client_id();

        let account = accounts
            .entry(client_id)
            .or_insert_with(|| accounts::Account::new(client_id));

        if account.locked {
            // Ignore events for locked accounts
            continue;
        }

        let account_transactions = transactions.entry(client_id).or_default();
        let account_disputes = disputes.entry(client_id).or_default();

        match event {
            Event::Transaction(transaction) => {
                if account_transactions.contains_key(&transaction.id) {
                    // Ignore transactions that have already been processed
                    continue;
                }

                if account.available + transaction.amount < 0.0 {
                    // Ignore withdrawal transactions that would result in a negative available balance
                    continue;
                }

                account.available += transaction.amount;
                account.total += transaction.amount;
                account_transactions.insert(transaction.id, transaction.amount);
            }
            Event::Dispute(dispute) => {
                let Some(transaction_amount) = account_transactions.get(&dispute.transaction_id)
                else {
                    // Ignore disputes for transactions that do not exist
                    continue;
                };
                account.available -= transaction_amount;
                account.held += transaction_amount;
                account_disputes.insert(dispute.transaction_id);
            }
            Event::Resolve(resolve) => {
                if !account_disputes.contains(&resolve.transaction_id) {
                    // Ignore resolves for transactions that are not in dispute
                    continue;
                }

                let Some(transaction_amount) = account_transactions.get(&resolve.transaction_id)
                else {
                    // Ignore resolves for transactions that do not exist
                    continue;
                };

                account.available += transaction_amount;
                account.held -= transaction_amount;
                account_disputes.remove(&resolve.transaction_id);
            }
            Event::Chargeback(chargeback) => {
                let Some(transaction_amount) = account_transactions.get(&chargeback.transaction_id)
                else {
                    // Ignore chargebacks for transactions that do not exist
                    continue;
                };

                if !account_disputes.contains(&chargeback.transaction_id) {
                    // Chargeback for transactions that are not in dispute are ignored
                    continue;
                }

                if account.total - transaction_amount < 0.0 {
                    // If a chargeback leads to negative balance, lock the account
                    account.locked = true;
                    continue;
                }

                account.held -= transaction_amount;
                account.total -= transaction_amount;
                account_disputes.remove(&chargeback.transaction_id);
                account_transactions.remove(&chargeback.transaction_id);
            }
        }
    }
    accounts.into_values().collect()
}
