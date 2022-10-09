use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::Type;
use postgres_protocol::message::frontend;
use std::{
    fmt,
    sync::{Arc, Weak},
};

struct StatementInner {
    client: Weak<InnerClient>,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        log::info!("StatementInner::drop()");
        if let Some(client) = self.client.upgrade() {
            log::info!("StatementInner::drop() Upgraded to Arc");
            let buf = client.with_buf(|buf| {
                log::info!("StatementInner::drop() frontend::close()");
                frontend::close(b'S', &self.name, buf).unwrap();
                log::info!("StatementInner::drop() frontend::sync()");
                frontend::sync(buf);
                log::info!("StatementInner::drop() frontend::freeze()");
                buf.split().freeze()
            });
            log::info!("StatementInner::drop() client.send()");
            let _ = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)));
            log::info!("StatementInner::drop() complete");
        } else {
            log::info!("StatementInner::drop() Couldn't upgrade to Arc");
        }
    }
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn new(
        inner: &Arc<InnerClient>,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement {
        Statement(Arc::new(StatementInner {
            client: Arc::downgrade(inner),
            name,
            params,
            columns,
        }))
    }

    pub(crate) fn name(&self) -> &str {
        &self.0.name
    }

    /// Returns the expected types of the statement's parameters.
    pub fn params(&self) -> &[Type] {
        &self.0.params
    }

    /// Returns information about the columns returned when the statement is queried.
    pub fn columns(&self) -> &[Column] {
        &self.0.columns
    }
}

/// Information about a column of a query.
pub struct Column {
    name: String,
    type_: Type,
}

impl Column {
    pub(crate) fn new(name: String, type_: Type) -> Column {
        Column { name, type_ }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the column.
    pub fn type_(&self) -> &Type {
        &self.type_
    }
}

impl fmt::Debug for Column {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Column")
            .field("name", &self.name)
            .field("type", &self.type_)
            .finish()
    }
}
