use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

#[tokio::main]
async fn main() {
    let Some(input) = std::env::args().nth(1) else {
        eprintln!("Usage: {} <input>", std::env::args().next().unwrap());
        std::process::exit(1);
    };

    match run(input).await {
        Err(error) => {
            eprintln!("{}", error);
            std::process::exit(1);
        }
        Ok(accounts) => {
            println!("client,available,held,total,locked");
            for account in accounts {
                println!(
                    "{},{:.4},{:.4},{:.4},{}",
                    account.client, account.available, account.held, account.total, account.locked
                );
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error("{0}: {1}")]
    IO(std::path::PathBuf, tokio::io::Error),
}

async fn run<P: AsRef<std::path::Path>>(input: P) -> Result<Vec<Account>, RunError> {
    let input = input.as_ref();
    let mut events = read_file(input).await.map_err(|error| RunError::IO(input.to_path_buf(), error))?;

    // Initialize the number of workers to the number of logical CPUs
    let workers_count = num_cpus::get();
    let mut senders = Vec::with_capacity(workers_count);
    let mut handles = Vec::with_capacity(workers_count);
    for _ in 0..workers_count {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        senders.push(tx);
        handles.push(tokio::spawn(reduce(rx)));
    }

    // Distribute events to workers in a round-robin fashion
    // making sure that events from the same client are processed by the same worker
    while let Some(event) = events.recv().await {
        let worker_index = event.client_id() as usize % workers_count;
        senders[worker_index]
            .send(event)
            .await
            .expect("receiver is not dropped");
    }

    // Drop the senders to signal the workers to stop
    for tx in senders {
        drop(tx);
    }

    let accounts = futures::future::try_join_all(handles)
        .await
        .expect("all workers are running")
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(accounts)
}


async fn read_file<P: AsRef<std::path::Path>>(
    input: P,
) -> Result<tokio::sync::mpsc::Receiver<Event>, tokio::io::Error> {
    use tokio::io::AsyncBufReadExt;

    const BUFFER_SIZE: usize = 8 * 1024;
    let file = tokio::fs::File::open(input.as_ref()).await?;
    let reader = tokio::io::BufReader::with_capacity(BUFFER_SIZE, file);

    let (tx, rx) = tokio::sync::mpsc::channel(BUFFER_SIZE);

    tokio::spawn(async move {
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await.expect("read line") {
            if let Ok(transaction) = line.parse() {
                tx.send(transaction).await.expect("receiver is not dropped");
            }
        }
    });

    Ok(rx)
}

async fn reduce(mut events: tokio::sync::mpsc::Receiver<Event>) -> Vec<Account> {
    let mut accounts = std::collections::HashMap::<u16, Account>::new();
    let mut transactions = std::collections::HashMap::<u16, HashMap<u32, f64>>::new();
    let mut disputes = std::collections::HashMap::<u16, HashSet<u32>>::new();
    while let Some(event) = events.recv().await {
        let client_id = event.client_id();

        let account = accounts
            .entry(client_id)
            .or_insert_with(|| Account::new(client_id));

        if account.locked {
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
                    if transaction.amount < 0.0  {
                        // Ignore withdrawal transactions that would result in a negative available balance
                        continue;
                    } else {
                        // Repaying debts is allowed 
                    }
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
                if !account_disputes.contains(&chargeback.transaction_id) {
                    // Chargeback for transactions that are not in dispute locks the account
                    account.locked = true;
                    continue;
                }

                let Some(transaction_amount) = account_transactions.get(&chargeback.transaction_id)
                else {
                    // Ignore chargebacks for transactions that do not exist
                    continue;
                };

                account.held -= transaction_amount;
                account.total -= transaction_amount;
                account_disputes.remove(&chargeback.transaction_id);
                account_transactions.remove(&chargeback.transaction_id);
            }
        }
    }
    accounts.into_values().collect()
}

#[derive(Debug)]
struct Account {
    client: u16,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

impl Account {
    fn new(client: u16) -> Self {
        Self {
            client,
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: false,
        }
    }
}

#[derive(Debug)]
struct Transaction {
    id: u32,
    client_id: u16,
    /// Negative for withdrawals
    amount: f64,
}

#[derive(Debug)]
struct Dispute {
    transaction_id: u32,
    client_id: u16,
}

#[derive(Debug)]
struct Resolve {
    transaction_id: u32,
    client_id: u16,
}

#[derive(Debug)]
struct Chargeback {
    transaction_id: u32,
    client_id: u16,
}

#[derive(Debug)]
enum Event {
    Transaction(Transaction),
    Dispute(Dispute),
    Resolve(Resolve),
    Chargeback(Chargeback),
}

impl Event {
    fn client_id(&self) -> u16 {
        match self {
            Event::Transaction(transaction) => transaction.client_id,
            Event::Dispute(dispute) => dispute.client_id,
            Event::Resolve(resolve) => resolve.client_id,
            Event::Chargeback(chargeback) => chargeback.client_id,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ParseError {
    #[error("invalid transaction type: {0}")]
    InvalidEventType(String),
    #[error("missing transaction type")]
    MissingTransactionType,
    #[error("invalid client ID: {0}")]
    InvalidClientId(String),
    #[error("missing client ID")]
    MissonClientId,
    #[error("invalid transaction ID: {0}")]
    InvalidTransactionId(String),
    #[error("missing transaction ID")]
    MissingTransactionId,
    #[error("invalid amount: {0}")]
    InvalidAmount(String),
    #[error("missing amount")]
    MissingAmount,
}

impl FromStr for Event {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(',');
        let transaction_type = parts.next().ok_or(ParseError::MissingTransactionType)?;
        match transaction_type.trim() {
            "deposit" => {
                let client_id = parts.next().ok_or(ParseError::MissonClientId)?;
                let client_id = client_id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidClientId(client_id.to_string()))?;
                let id = parts.next().ok_or(ParseError::MissingTransactionId)?;
                let id = id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidTransactionId(id.to_string()))?;
                let amount = parts.next().ok_or(ParseError::MissingAmount)?;
                let amount = amount
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| ParseError::InvalidAmount(amount.to_string()))?;
                Ok(Event::Transaction(Transaction {
                    id,
                    client_id,
                    amount,
                }))
            }
            "withdrawal" => {
                let client_id = parts.next().ok_or(ParseError::MissonClientId)?;
                let client_id = client_id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidClientId(client_id.to_string()))?;
                let id = parts.next().ok_or(ParseError::MissingTransactionId)?;
                let id = id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidTransactionId(id.to_string()))?;
                let amount = parts.next().ok_or(ParseError::MissingAmount)?;
                let amount = amount
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| ParseError::InvalidAmount(amount.to_string()))?;
                Ok(Event::Transaction(Transaction {
                    id,
                    client_id,
                    amount: -amount,
                }))
            }
            "dispute" => {
                let client_id = parts.next().ok_or(ParseError::MissonClientId)?;
                let client_id = client_id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidClientId(client_id.to_string()))?;
                let id = parts.next().ok_or(ParseError::MissingTransactionId)?;
                let id = id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidTransactionId(id.to_string()))?;
                Ok(Event::Dispute(Dispute {
                    transaction_id: id,
                    client_id,
                }))
            }
            "resolve" => {
                let client_id = parts.next().ok_or(ParseError::MissonClientId)?;
                let client_id = client_id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidClientId(client_id.to_string()))?;
                let id = parts.next().ok_or(ParseError::MissingTransactionId)?;
                let id = id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidTransactionId(id.to_string()))?;
                Ok(Event::Resolve(Resolve {
                    transaction_id: id,
                    client_id,
                }))
            }
            "chargeback" => {
                let client_id = parts.next().ok_or(ParseError::MissonClientId)?;
                let client_id = client_id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidClientId(client_id.to_string()))?;
                let id = parts.next().ok_or(ParseError::MissingTransactionId)?;
                let id = id
                    .trim()
                    .parse()
                    .map_err(|_| ParseError::InvalidTransactionId(id.to_string()))?;
                Ok(Event::Chargeback(Chargeback {
                    transaction_id: id,
                    client_id,
                }))
            }
            _ => Err(ParseError::InvalidEventType(transaction_type.to_string())),
        }
    }
}
