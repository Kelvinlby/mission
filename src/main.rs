use clap::Parser;

/// Script runner with resource monitor functionality.
#[derive(Parser)]
#[command(name = "mission", version, about)]
struct Cli {}

fn main() {
    Cli::parse();
}
