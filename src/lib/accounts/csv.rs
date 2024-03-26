use crate::accounts;

pub struct AccountsCsvWriter<R: std::io::Write> {
    writer: R,
    header_written: bool,
}

impl<R: std::io::Write> AccountsCsvWriter<R> {
    pub fn from_writer(writer: R) -> Self {
        Self {
            writer,
            header_written: false,
        }
    }

    pub fn write_account(&mut self, account: &accounts::Account) -> std::io::Result<()> {
        if !self.header_written {
            writeln!(self.writer, "client,available,held,total,locked")?;
            self.header_written = true;
        }
        writeln!(
            self.writer,
            "{},{:.4},{:.4},{:.4},{}",
            account.client, account.available, account.held, account.total, account.locked
        )
    }
}
