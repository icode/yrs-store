use bytes::Bytes;
use std::collections::HashMap;
use yrs::error::{Error, UpdateError};
use yrs::updates::decoder::Decode;
use yrs::{
    Doc, Out, ReadTxn, StateVector, Subscription, Transact, Transaction, TransactionAcqError,
    TransactionCleanupEvent, TransactionMut, Update,
};

pub trait ForStore {
    fn get_update(&self) -> Bytes;
    fn diff_state_update(&self, state: &Bytes) -> Result<Bytes, Error>;
    fn apply_update(&self, txn: &mut TransactionMut, update: &Bytes) -> Result<(), UpdateError>;
    fn roots(&self, txn: &Transaction<'static>) -> HashMap<String, Out>;
    fn observe<F>(&self, f: F) -> Result<Subscription, TransactionAcqError>
    where
        F: Fn(&TransactionMut, &TransactionCleanupEvent) + Send + Sync + 'static;
}

impl ForStore for Doc {
    fn get_update(&self) -> Bytes {
        self.transact()
            .encode_state_as_update_v1(&StateVector::default())
            .into()
    }

    fn diff_state_update(&self, state: &Bytes) -> Result<Bytes, Error> {
        let state_vector = StateVector::decode_v1(&state.as_ref())?;
        let update = self.transact().encode_state_as_update_v1(&state_vector);
        Ok(update.into())
    }

    fn apply_update(&self, txn: &mut TransactionMut, update: &Bytes) -> Result<(), UpdateError> {
        let u = Update::decode_v1(update.as_ref()).unwrap();
        txn.apply_update(u)
    }

    fn roots(&self, txn: &Transaction<'static>) -> HashMap<String, Out> {
        txn.root_refs()
            .map(|(key_slice, value)| (key_slice.to_string(), value))
            .collect::<HashMap<String, Out>>()
    }

    fn observe<F>(&self, f: F) -> Result<Subscription, TransactionAcqError>
    where
        F: Fn(&TransactionMut, &TransactionCleanupEvent) + Send + Sync + 'static,
    {
        self.observe_transaction_cleanup(move |txn, event| {
            if !event.delete_set.is_empty() || event.before_state != event.after_state {
                f(txn, event);
            }
        })
    }
}
