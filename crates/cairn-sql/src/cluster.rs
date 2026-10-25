//! SQL transaction coordination around the independent native client.
pub use cairn_common::{Column, Schema};
use std::{ops::Deref, sync::Arc};

pub struct Cluster(pub(crate) Arc<cairn_iproto::Cluster>);

impl Deref for Cluster {
    type Target = Arc<cairn_iproto::Cluster>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
