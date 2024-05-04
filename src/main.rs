use alloy_consensus::{SidecarCoder, SimpleCoder};
use alloy_rlp::Decodable as _;
use alloy_sol_types::{sol, SolEventInterface, SolInterface};
use reth::transaction_pool::TransactionPool;
use reth_exex::{ExExContext, ExExEvent};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_primitives::{
    address, eip4844::kzg_to_versioned_hash, Address, Bytes, SealedBlockWithSenders,
    TransactionSigned, TxType, B256,
};
use reth_provider::Chain;
use reth_tracing::tracing::info;

sol!(SequencerInbox, "sequencer_inbox.abi");
use SequencerInbox::{SequencerInboxCalls, SequencerInboxEvents};

pub struct SequencerInboxReader<Node: FullNodeComponents> {
    ctx: ExExContext<Node>,
}

impl<Node: FullNodeComponents> SequencerInboxReader<Node> {
    fn new(ctx: ExExContext<Node>) -> eyre::Result<Self> {
        Ok(Self { ctx })
    }

    async fn run(mut self) -> eyre::Result<()> {
        while let Some(notif) = self.ctx.notifications.recv().await {
            if let Some(chain) = notif.committed_chain() {
                let events = decode_chain_into_events(&chain);
                for (_, tx, event) in events {
                    self.handle_sequencer_event(tx, event).await?;
                }
                self.ctx
                    .events
                    .send(ExExEvent::FinishedHeight(chain.tip().number))?;
            }
        }
        Ok(())
    }

    async fn handle_sequencer_event(
        &mut self,
        tx: &TransactionSigned,
        event: SequencerInboxEvents,
    ) -> eyre::Result<()> {
        use SequencerInboxEvents::*;
        match event {
            SequencerBatchDelivered(batch) => {
                let seq = batch.batchSequenceNumber;
                info!(batch_sequencer_number = ?seq, "Received batch");
                let call = SequencerInboxCalls::abi_decode(tx.input(), true)?;
                if let SequencerInboxCalls::addSequencerL2Batch(
                    SequencerInbox::addSequencerL2BatchCall { data, .. },
                ) = call
                {
                    // let batch = batch.into_iter().map(|tx| tx.into()).collect::<Vec<_>>();
                    decode_transactions(self.ctx.pool(), tx, data).await?;
                }
                // let call = RollupContractCalls::abi_decode(tx.input(), true)?;

                //     if let RollupContractCalls::submitBlock(RollupContract::submitBlockCall {
                //         header,
                //         blockData,
                //         ..
                //     }) = call
                // decode_transactions(self.ctx.pool(), tx, block_data, block_data_hash)
            }
            _ => {
                info!(tx = ?tx, "Received unknown event");
            }
        }
        Ok(())
    }
}

async fn decode_transactions<Pool: TransactionPool>(
    pool: &Pool,
    tx: &TransactionSigned,
    block_data: Bytes,
    // block_data_hash: B256,
) -> eyre::Result<()> {
    // Get raw transactions either from the blobs, or directly from the block data
    let raw_transactions = if matches!(tx.tx_type(), TxType::Eip4844) {
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
        let blob_hashes = Vec::<B256>::decode(&mut block_data.as_ref())?;

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

        info!("Received blob data");
        data.into()
    } else {
        block_data
    };

    info!("Block data received");

    // TODO: Brotli decode the input blob.
    Ok(())

    // let raw_transaction_hash = keccak256(&raw_transactions);
    // if raw_transaction_hash != block_data_hash {
    //     eyre::bail!("block data hash mismatch")
    // }

    // // Decode block data, filter only transactions with the correct chain ID and recover senders
    // let transactions = Vec::<TransactionSigned>::decode(&mut raw_transactions.as_ref())?
    //     .into_iter()
    //     // .filter(|tx| tx.chain_id() == Some(CHAIN_ID))
    //     .map(|tx| {
    //         let sender = tx
    //             .recover_signer()
    //             .ok_or(eyre::eyre!("failed to recover signer"))?;
    //         Ok((tx, sender))
    //     })
    //     .collect::<eyre::Result<_>>()?;

    // Ok(transactions)
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
    let inbox_address: Address = address!("6c97864CE4bEf387dE0b3310A44230f7E3F1be0D");
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
        .filter(|(_, tx, _)| tx.to() == Some(inbox_address))
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

fn main() -> eyre::Result<()> {
    reth::cli::Cli::parse_args().run(|builder, _| async move {
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("SequencerInboxReader", move |ctx| async {
                Ok(SequencerInboxReader::new(ctx)?.run())
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}
