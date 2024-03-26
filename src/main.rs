use tinypay::accounts;
use tinypay::events;

#[tokio::main]
async fn main() {
    let Some(input) = std::env::args().nth(1) else {
        eprintln!("Usage: {} <input>", std::env::args().next().unwrap());
        std::process::exit(1);
    };

    let Ok(file) = tokio::fs::File::open(&input).await else {
        eprintln!("Failed to open input file: {}", input);
        std::process::exit(1);
    };

    let mut events_reader = events::csv::CsvEventsReader::from_reader(file);

    // Initialize the number of workers to the number of logical CPUs
    let workers_count = num_cpus::get();
    let mut senders = Vec::with_capacity(workers_count);
    let mut handles = Vec::with_capacity(workers_count);
    for _ in 0..workers_count {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
        senders.push(tx);
        handles.push(tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(event) = rx.recv().await {
                events.push(event);
            }
            events::reduce(&events)
        }));
    }

    // Distribute events to workers making sure that events from the same client are processed by the same worker
    while let Some(event) = events_reader.next_event().await {
        let Ok(event) = event else { continue };
        let worker_index = event.client_id() as usize % workers_count;
        senders[worker_index]
            .send(event)
            .await
            .expect("receiver is not dropped");
    }

    // Drop the senders to signal workers to stop
    for tx in senders {
        drop(tx);
    }

    let mut accounts_writer = accounts::csv::AccountsCsvWriter::from_writer(std::io::stdout());

    futures::future::try_join_all(handles)
        .await
        .expect("all workers are running")
        .into_iter()
        .flatten()
        .for_each(|account| {
            accounts_writer
                .write_account(&account)
                .expect("failed to write account")
        });
}
