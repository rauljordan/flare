# Flare

Arbitrum Sepolia sequencer inbox reader in Rust using Reth's [execution extensions](https://www.paradigm.xyz/2024/05/reth-exex) (ExEx). Primitive implementation, reads batch data, brotli decompresses it, and saves it to a sqlite database. Supports reading batches that are posted either as calldata or blobs.

## Running

- Install Rust stable 1.77.0
- Run an Ethereum sepolia consensus node (see [Prysm](https://docs.prylabs.network/docs/install/install-with-script))
- Run Flare

```
cargo build --release
./target/release/flare node --http --http.api=debug,eth,reth --ws --full --chain=sepolia
```