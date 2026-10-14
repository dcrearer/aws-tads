// Deep reqwest + AWS SDK future nesting exceeds the default type-layout limit.
#![recursion_limit = "512"]
//! Local runner for the analysis worker: feed a TAMS `flows/segments_added` JSON payload through
//! the real TAMS + Pegasus path and print the derived key-moment Track Events.
//!
//! By default it is a **dry run** (no TADS writes). Pass `--write` to actually resolve the schema,
//! ensure the Track, and POST the Events to TADS.
//!
//! Reads config from `VU_CONFIG` (default `config/vu.yaml`) + `VU__*` env. Needs the TAMS (and, for
//! `--write`, TADS) OAuth creds in config and AWS credentials in the environment (for Bedrock).
//!
//! Usage: `cargo run -p tads-lambda-vu-analysis-worker --bin vu-dry-run -- [--write] event.json`

use std::path::PathBuf;

use clap::Parser;
use tads_lambda_vu_analysis_worker::{WorkerConfig, build_worker};

#[derive(Parser)]
#[command(
    about = "Analyse a TAMS segments_added payload and print derived key-moment Events; optionally write them to TADS."
)]
struct Args {
    /// Path to a JSON file containing a `flows/segments_added` event.
    event: PathBuf,
    /// Actually write to TADS (resolve schema, ensure Track, POST Events) instead of a dry run.
    #[arg(long)]
    write: bool,
    /// Replace existing Events: DELETE the derived ids, wait for the async delete, then recreate.
    /// Needed to update already-written Events, since `editable_events=false` forbids in-place POST.
    #[arg(long)]
    replace: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tads_telemetry::init();
    let args = Args::parse();
    let body = std::fs::read_to_string(&args.event)?;

    let writing = args.write || args.replace;
    let mut cfg = WorkerConfig::load()?;
    cfg.dry_run = !writing;
    let worker = build_worker(cfg).await.map_err(std::io::Error::other)?;

    // analyze() never writes — it resolves/windows/infers/maps so we can show the plan either way.
    let plan = worker.analyze(&body).await?;
    println!("track_id : {}", plan.track_id);
    println!("source   : {}", plan.source_entity_id);
    println!("events   : {}", plan.events.len());
    println!("{}", serde_json::to_string_pretty(&plan.events)?);

    if !writing || plan.events.is_empty() {
        if writing {
            println!("\nno events to write.");
        }
        return Ok(());
    }

    if args.replace {
        // DELETE the derived ids, wait for the async delete, then recreate with the current payload.
        println!("\n--replace: deleting existing Events, then recreating…");
        let ids: Vec<_> = plan.events.iter().map(|e| e.id).collect();
        worker.reanalyze(plan.track_id, &ids, &plan.events).await?;
        println!("replace complete");
        return Ok(());
    }

    // --write: create only (existing ids come back AlreadyPresent on a TAMS-compatible Track).
    println!("\n--write: posting to TADS…");
    let outcomes = worker.write(&plan).await?;
    for outcome in &outcomes {
        println!("  {outcome:?}");
    }
    let rejected = outcomes.iter().filter(|o| !o.is_success()).count();
    if rejected > 0 {
        return Err(format!("{rejected} event(s) rejected by TADS").into());
    }
    println!("writeback complete");
    Ok(())
}
