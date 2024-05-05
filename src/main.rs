use std::io::Read;
use std::sync::{Arc, Mutex, MutexGuard};

use alloy_consensus::{SidecarCoder, SimpleCoder};
use alloy_rlp::Decodable as _;
use alloy_sol_types::{sol, SolEventInterface};
use brotli2::read::{BrotliDecoder, BrotliEncoder};
use reth::transaction_pool::TransactionPool;
use reth_exex::{ExExContext, ExExEvent};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_primitives::revm_primitives::FixedBytes;
use reth_primitives::ruint::Uint;
use reth_primitives::{
    address, eip4844::kzg_to_versioned_hash, Address, Bytes, SealedBlockWithSenders,
    TransactionSigned, TxType, B256,
};
use reth_provider::Chain;
use reth_tracing::tracing::info;
use rusqlite::Connection;

sol!(SequencerInbox, "sequencer_inbox.abi");
use SequencerInbox::SequencerInboxEvents;

const DATABASE_PATH: &'static str = "sequencer_inbox.db";
const ARBITRUM_SEPOLIA_SEQUENCER_INBOX: Address =
    address!("6c97864CE4bEf387dE0b3310A44230f7E3F1be0D");

pub struct SequencerInboxReader<Node: FullNodeComponents> {
    ctx: ExExContext<Node>,
    db: Database,
}

impl<Node: FullNodeComponents> SequencerInboxReader<Node> {
    fn new(ctx: ExExContext<Node>, db: Database) -> eyre::Result<Self> {
        Ok(Self { ctx, db })
    }

    async fn run(mut self) -> eyre::Result<()> {
        while let Some(notif) = self.ctx.notifications.recv().await {
            if let Some(chain) = notif.committed_chain() {
                info!("Committed chain received");
                let events = decode_chain_into_events(&chain);
                for (block, tx, event) in events {
                    self.handle_sequencer_event(block, tx, event).await?;
                }
                self.ctx
                    .events
                    .send(ExExEvent::FinishedHeight(chain.tip().number))?;
            }
            // TODO: Handle reorg events.
        }
        Ok(())
    }

    async fn handle_sequencer_event(
        &mut self,
        block: &SealedBlockWithSenders,
        tx: &TransactionSigned,
        event: SequencerInboxEvents,
    ) -> eyre::Result<()> {
        use SequencerInboxEvents::*;
        match event {
            SequencerBatchDelivered(_batch) => {
                info!("Received batch delivery from sequencer inbox");
            }
            SequencerBatchData(_batch_data) => {
                info!("Received batch data from sequencer inbox");
            }
            InboxMessageDelivered(message) => {
                info!("Inbox mesage delivered");
                let num = message.messageNum;
                let batch =
                    decode_transactions(self.ctx.pool(), num, block, tx, tx.input().clone())
                        .await?;
                self.db.insert_batch(batch)?;
                info!("Saved batch to database");
            }
            InboxMessageDeliveredFromOrigin(_message) => {
                info!("Inbox mesage delivered from origin");
            }
            _ => {
                info!(tx = ?tx, "Unknown event");
            }
        }
        Ok(())
    }
}

async fn decode_transactions<Pool: TransactionPool>(
    pool: &Pool,
    message_num: Uint<256, 4>,
    block: &SealedBlockWithSenders,
    tx: &TransactionSigned,
    tx_input: Bytes,
) -> eyre::Result<Batch> {
    // Get raw transactions either from the blobs, or directly from the block data
    let compressed_batch_data = if matches!(tx.tx_type(), TxType::Eip4844) {
        info!("Received batch data as blob");
        let blobs: Vec<_> = if let Some(sidecar) = pool.get_blob(tx.hash)? {
            // Try to get blobs from the transaction pool
            sidecar.blobs.into_iter().zip(sidecar.commitments).collect()
        } else {
            // If transaction is not found in the pool, try to get blobs from Blobscan
            let blobscan_client = foundry_blob_explorers::Client::holesky();
            let sidecar = blobscan_client.transaction(tx.hash).await?.blob_sidecar();
            sidecar
                .blobs
                .into_iter()
                .map(|blob| (*blob).into())
                .zip(
                    sidecar
                        .commitments
                        .into_iter()
                        .map(|commitment| (*commitment).into()),
                )
                .collect()
        };

        // Decode blob hashes from block data
        let blob_hashes = Vec::<B256>::decode(&mut tx_input.as_ref())?;

        // Filter blobs that are present in the block data
        let blobs = blobs
            .into_iter()
            // Convert blob KZG commitments to versioned hashes
            .map(|(blob, commitment)| (blob, kzg_to_versioned_hash((*commitment).into())))
            // Filter only blobs that are present in the block data
            .filter(|(_, hash)| blob_hashes.contains(hash))
            .map(|(blob, _)| blob)
            .collect::<Vec<_>>();
        if blobs.len() != blob_hashes.len() {
            eyre::bail!("some blobs not found")
        }

        // Decode blobs and concatenate them to get the raw, brotli compressed payload of transactions.
        let data = SimpleCoder::default()
            .decode_all(&blobs)
            .ok_or(eyre::eyre!("failed to decode blobs"))?
            .concat();

        data.into()
    } else {
        info!("Received batch as calldata");
        tx_input
    };

    let compressor = BrotliEncoder::new(compressed_batch_data.as_ref(), 11);
    let mut decompressor = BrotliDecoder::new(compressor);
    let mut raw_transactions = Vec::new();
    decompressor.read_to_end(&mut raw_transactions)?;

    info!(
        "Decompressed brotli batch successfully of len {}",
        raw_transactions.len()
    );
    Ok(Batch {
        seq_num: message_num,
        tx_hash: tx.hash(),
        data: Bytes::from(raw_transactions),
        block_number: block.block.number,
    })
}

/// Decode chain of blocks into a flattened list of receipt logs, filter only
/// transactions to the sequencer inbox contract and extract events.
fn decode_chain_into_events(
    chain: &Chain,
) -> Vec<(
    &SealedBlockWithSenders,
    &TransactionSigned,
    SequencerInboxEvents,
)> {
    chain
        // Get all blocks and receipts
        .blocks_and_receipts()
        // Get all receipts
        .flat_map(|(block, receipts)| {
            block
                .body
                .iter()
                .zip(receipts.iter().flatten())
                .map(move |(tx, receipt)| (block, tx, receipt))
        })
        // Filter only transactions to the rollup contract
        .filter(|(_, tx, _)| tx.to() == Some(ARBITRUM_SEPOLIA_SEQUENCER_INBOX))
        // Get all logs
        .flat_map(|(block, tx, receipt)| receipt.logs.iter().map(move |log| (block, tx, log)))
        // Decode and filter events
        .filter_map(|(block, tx, log)| {
            SequencerInboxEvents::decode_raw_log(log.topics(), &log.data.data, true)
                .ok()
                .map(|event| (block, tx, event))
        })
        .collect()
}

pub struct Database {
    connection: Arc<Mutex<Connection>>,
}

impl Database {
    pub fn new(connection: Connection) -> eyre::Result<Self> {
        let database = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        database.create_tables()?;
        Ok(database)
    }

    fn connection(&self) -> MutexGuard<'_, Connection> {
        self.connection
            .lock()
            .expect("failed to acquire database lock")
    }

    fn create_tables(&self) -> eyre::Result<()> {
        self.connection().execute_batch(
            "CREATE TABLE IF NOT EXISTS batch (
                seq_num      TEXT PRIMARY KEY,
                tx_hash      TEXT UNIQUE,
                data         TEXT,
                block_number INTEGER
            );",
        )?;
        Ok(())
    }

    fn insert_batch(&mut self, batch: Batch) -> eyre::Result<()> {
        let mut connection = self.connection();
        let tx = connection.transaction()?;
        tx.execute(
            "INSERT INTO batch (seq_num, tx_hash, data, block_number) VALUES (?, ?, ?, ?)",
            (
                batch.seq_num.to_string(),
                batch.tx_hash.to_string(),
                batch.data.to_string(),
                batch.block_number,
            ),
        )?;
        tx.commit()?;
        Ok(())
    }
}

struct Batch {
    seq_num: Uint<256, 4>,
    tx_hash: FixedBytes<32>,
    data: Bytes,
    block_number: u64,
}

fn main() -> eyre::Result<()> {
    reth::cli::Cli::parse_args().run(|builder, _| async move {
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("SequencerInboxReader", move |ctx| async {
                let connection = Connection::open(DATABASE_PATH)?;
                let db = Database::new(connection)?;
                Ok(SequencerInboxReader::new(ctx, db)?.run())
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}
