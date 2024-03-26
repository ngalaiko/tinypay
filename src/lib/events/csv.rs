use tokio::io::AsyncBufReadExt;

use crate::events;

pub struct CsvEventsReader<R: tokio::io::AsyncRead + std::marker::Unpin + Send + 'static> {
    lines: tokio::io::Lines<tokio::io::BufReader<R>>,
}

impl<R: tokio::io::AsyncRead + std::marker::Unpin + Send + 'static> CsvEventsReader<R> {
    pub fn from_reader(reader: R) -> Self {
        const BUFFER_SIZE: usize = 8 * 1024;
        let reader = tokio::io::BufReader::with_capacity(BUFFER_SIZE, reader);
        Self {
            lines: reader.lines(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NextError {
    #[error("failed to parse event: {0}")]
    Parse(FromCsvRowError),
    #[error("failed to read event: {0}")]
    IO(std::io::Error),
}

impl<R: tokio::io::AsyncRead + std::marker::Unpin + Send + 'static> CsvEventsReader<R> {
    pub async fn next_event(&mut self) -> Option<Result<events::Event, NextError>> {
        match self.lines.next_line().await {
            Ok(Some(line)) => match from_csv_row(&line) {
                Ok(event) => Some(Ok(event)),
                Err(error) => Some(Err(NextError::Parse(error))),
            },
            Ok(None) => None,
            Err(error) => Some(Err(NextError::IO(error))),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FromCsvRowError {
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

fn from_csv_row(s: &str) -> Result<events::Event, FromCsvRowError> {
    let mut parts = s.split(',');
    let transaction_type = parts
        .next()
        .ok_or(FromCsvRowError::MissingTransactionType)?;
    match transaction_type.trim() {
        "deposit" => {
            let client_id = parts.next().ok_or(FromCsvRowError::MissonClientId)?;
            let client_id = client_id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidClientId(client_id.to_string()))?;
            let id = parts.next().ok_or(FromCsvRowError::MissingTransactionId)?;
            let id = id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidTransactionId(id.to_string()))?;
            let amount = parts.next().ok_or(FromCsvRowError::MissingAmount)?;
            let amount = amount
                .trim()
                .parse::<f64>()
                .map_err(|_| FromCsvRowError::InvalidAmount(amount.to_string()))?;
            Ok(events::Event::Transaction(events::Transaction {
                id,
                client_id,
                amount,
            }))
        }
        "withdrawal" => {
            let client_id = parts.next().ok_or(FromCsvRowError::MissonClientId)?;
            let client_id = client_id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidClientId(client_id.to_string()))?;
            let id = parts.next().ok_or(FromCsvRowError::MissingTransactionId)?;
            let id = id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidTransactionId(id.to_string()))?;
            let amount = parts.next().ok_or(FromCsvRowError::MissingAmount)?;
            let amount = amount
                .trim()
                .parse::<f64>()
                .map_err(|_| FromCsvRowError::InvalidAmount(amount.to_string()))?;
            Ok(events::Event::Transaction(events::Transaction {
                id,
                client_id,
                amount: -amount,
            }))
        }
        "dispute" => {
            let client_id = parts.next().ok_or(FromCsvRowError::MissonClientId)?;
            let client_id = client_id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidClientId(client_id.to_string()))?;
            let id = parts.next().ok_or(FromCsvRowError::MissingTransactionId)?;
            let id = id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidTransactionId(id.to_string()))?;
            Ok(events::Event::Dispute(events::Dispute {
                transaction_id: id,
                client_id,
            }))
        }
        "resolve" => {
            let client_id = parts.next().ok_or(FromCsvRowError::MissonClientId)?;
            let client_id = client_id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidClientId(client_id.to_string()))?;
            let id = parts.next().ok_or(FromCsvRowError::MissingTransactionId)?;
            let id = id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidTransactionId(id.to_string()))?;
            Ok(events::Event::Resolve(events::Resolve {
                transaction_id: id,
                client_id,
            }))
        }
        "chargeback" => {
            let client_id = parts.next().ok_or(FromCsvRowError::MissonClientId)?;
            let client_id = client_id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidClientId(client_id.to_string()))?;
            let id = parts.next().ok_or(FromCsvRowError::MissingTransactionId)?;
            let id = id
                .trim()
                .parse()
                .map_err(|_| FromCsvRowError::InvalidTransactionId(id.to_string()))?;
            Ok(events::Event::Chargeback(events::Chargeback {
                transaction_id: id,
                client_id,
            }))
        }
        _ => Err(FromCsvRowError::InvalidEventType(
            transaction_type.to_string(),
        )),
    }
}
