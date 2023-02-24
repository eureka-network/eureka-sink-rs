use crate::{db_loader::Loader, error::DBError};

#[allow(dead_code)]
impl Loader {
    fn flush(&mut self) -> Result<(), DBError> {
        let tx = self.connection().build_transaction();
        Ok(())
    }
}
