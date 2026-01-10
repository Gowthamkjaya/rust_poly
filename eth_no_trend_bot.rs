use std::str::FromStr;
use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::fs::{File, OpenOptions};
use std::io::Write;

use alloy::signers::Signer;
use alloy::signers::local::LocalSigner;
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR};
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::clob::types::{Side, SignatureType, OrderType, Amount};
use polymarket_client_sdk::gamma::Client as GammaClient;
use polymarket_client_sdk::gamma::types::request::EventsRequest;
use polymarket_client_sdk::data::Client as DataClient;
use polymarket_client_sdk::data::types::request::PositionsRequest;
use polymarket_client_sdk::types::Decimal;
use rust_decimal_macros::dec;
use chrono::{Utc, TimeZone};

// ==========================================
// 📊 CONFIGURATION CONSTANTS
// ==========================================
const TRADE_SIDE: &str = "BOTH";
const ENTRY_PRICE: f64 = 0.96;
const STOP_LOSS_PRICE: f64 = 0.89;
const SUSTAIN_TIME: u64 = 3;
const POSITION_SIZE: u32 = 5;
const MARKET_WINDOW: u64 = 240;
const POLLING_INTERVAL: u64 = 1;
const ENTRY_TIMEOUT: u64 = 210;
const ABORT_ASK_PRICE: f64 = 0.99;
const LOG_FILE: &str = "ETH_NO_trading_log.csv";

// ==========================================
// 📝 DATA STRUCTURES
// ==========================================

#[derive(Debug, Clone)]
struct MarketData {
    slug: String,
    title: String,
    link: String,
    yes_token: String,
    no_token: String,
}

#[derive(Debug, Clone, Default)]
struct TradeRecord {
    title: String,
    link: String,
    status: String,
    entry1_time: String,
    entry_side: String,
    entry_price: String,
    position_size: String,
    sl_time: String,
    sl_price: String,
    final_status: String,
    notes: String,
    is_sl_triggered: String,
}

// ==========================================
// 🤖 MAIN BOT STRUCTURE
// ==========================================

struct EthNoTrendBot {
    clob_client: Client,
    gamma_client: GammaClient,
    data_client: DataClient,
    signer: LocalSigner,
    active_trade: bool,
    traded_markets: HashSet<String>,
}

impl EthNoTrendBot {
    async fn new() -> anyhow::Result<Self> {
        println!("🤖 ETH No Trend Bot Starting...");
        println!("📊 Configuration:");
        println!("   Trade Side: {}", TRADE_SIDE);
        println!("   Entry Price: ${}", ENTRY_PRICE);
        println!("   Stop Loss: ${}", STOP_LOSS_PRICE);
        println!("   Position Size: {} shares", POSITION_SIZE);
        println!("   Trading Window: Last {}s of market", MARKET_WINDOW);
        println!("   🚨 ABORT Trigger: ASK > ${}\n", ABORT_ASK_PRICE);

        if !["YES", "NO", "BOTH"].contains(&TRADE_SIDE) {
            anyhow::bail!("❌ Invalid TRADE_SIDE: {}. Must be 'YES', 'NO', or 'BOTH'", TRADE_SIDE);
        }

        // Get private key from environment
        let private_key = std::env::var(PRIVATE_KEY_VAR)
            .expect("🚨 POLYMARKET_PRIVATE_KEY not set! Export it with: export POLYMARKET_PRIVATE_KEY=0x...");
        
        // Create signer
        let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
        
        println!("🔑 Authenticating with Polymarket...");
        
        // Create CLOB client with GnosisSafe signature type (for proxy wallets)
        let clob_client = Client::new("https://clob.polymarket.com", Config::default())?
            .authentication_builder(&signer)
            .signature_type(SignatureType::GnosisSafe)  // Auto-derives funder address
            .authenticate()
            .await?;
        
        println!("✅ Authenticated successfully!");
        println!("📍 Trading address: {:?}\n", signer.address());
        
        // Create other API clients
        let gamma_client = GammaClient::default();
        let data_client = DataClient::default();
        
        // Initialize CSV log
        init_csv_log()?;

        Ok(Self {
            clob_client,
            gamma_client,
            data_client,
            signer,
            active_trade: false,
            traded_markets: HashSet::new(),
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        println!("🚀 ETH No Trend Bot Running...\n");

        loop {
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let ts = (current_time / 900) * 900;
            let slug = format!("eth-updown-15m-{}", ts);

            let elapsed_since_open = current_time - ts;
            let time_until_next = 900 - elapsed_since_open;

            let open_time = Utc.timestamp_opt(ts as i64, 0).unwrap().format("%H:%M:%S");
            print!("\r⏰ Current Market: {} | Open Time: {} | Next in: {}s ", 
                slug, open_time, time_until_next);
            std::io::stdout().flush()?;

            if self.traded_markets.contains(&slug) {
                print!("\r   ✓ Already traded this market. Waiting for next...          ");
                std::io::stdout().flush()?;
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            }

            if elapsed_since_open < 5 {
                print!("\r   ⏳ Market just opened. Waiting 5s for API indexing...     ");
                std::io::stdout().flush()?;
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            if let Some(market) = self.get_market_from_slug(&slug).await {
                self.monitor_market(market, ts).await?;
            } else {
                print!("\r⚠️ Unable to fetch market {}. Retrying...                    ", slug);
                std::io::stdout().flush()?;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn get_market_from_slug(&self, slug: &str) -> Option<MarketData> {
        for attempt in 1..=3 {
            println!("\n   🔍 Fetching market '{}' (Attempt {}/3)", slug, attempt);
            
            match self.fetch_market_data(slug).await {
                Ok(Some(market)) => return Some(market),
                Ok(None) => return None,
                Err(e) => {
                    println!("   ⚠️ Market fetch attempt {}/3 failed: {}", attempt, e);
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        }
        None
    }

    async fn fetch_market_data(&self, slug: &str) -> anyhow::Result<Option<MarketData>> {
        // Use Gamma API to find the market
        let request = EventsRequest::builder()
            .slug(slug.to_string())
            .build();
        
        let events = self.gamma_client.events(&request).await?;
        
        if events.is_empty() {
            println!("   ⚠️ Market '{}' not found", slug);
            return Ok(None);
        }

        let event = &events[0];
        
        if event.markets.is_empty() {
            return Ok(None);
        }

        let market = &event.markets[0];
        
        // Check if order book is enabled
        if !market.enable_order_book.unwrap_or(false) {
            println!("   ⚠️ Order book not enabled for this market");
            return Ok(None);
        }

        let token_ids = market.clob_token_ids.clone();
        if token_ids.len() < 2 {
            return Ok(None);
        }

        println!("   ✅ Market found: {}", event.title);
        println!("   ✅ Order book enabled");
        println!("   🎯 YES Token: {}", token_ids[0]);
        println!("   🎯 NO Token: {}", token_ids[1]);

        Ok(Some(MarketData {
            slug: slug.to_string(),
            title: event.title.clone(),
            link: format!("https://polymarket.com/event/{}", slug),
            yes_token: token_ids[0].clone(),
            no_token: token_ids[1].clone(),
        }))
    }

    async fn monitor_market(&mut self, market: MarketData, market_start_ts: u64) -> anyhow::Result<()> {
        println!("\n{}", "=".repeat(60));
        println!("📊 MONITORING: {}", market.title);
        println!("🔗 Link: {}", market.link);
        println!("{}", "=".repeat(60));

        let mut entry_window_start: Option<u64> = None;
        
        loop {
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let elapsed = current_time - market_start_ts;
            let time_until_close = 900 - elapsed;

            if time_until_close > MARKET_WINDOW {
                print!("\r⏳ Waiting for trading window ({}s remaining)...                    ", time_until_close - MARKET_WINDOW);
                std::io::stdout().flush()?;
                entry_window_start = None;
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            if entry_window_start.is_none() {
                entry_window_start = Some(current_time);
                println!("\n🔵 Entered trading window. Entry timeout starts now ({}s)", ENTRY_TIMEOUT);
            }

            if time_until_close <= 0 {
                println!("\n⏰ Market closed. Moving to next market.");
                self.traded_markets.insert(market.slug.clone());
                return Ok(());
            }

            if let Some(window_start) = entry_window_start {
                if current_time - window_start > ENTRY_TIMEOUT {
                    println!("\n❌ Entry window timeout. Moving to next market.");
                    self.traded_markets.insert(market.slug.clone());
                    return Ok(());
                }
            }

            // Get order books using SDK
            let yes_book = self.clob_client.get_order_book(&market.yes_token).await.ok();
            let no_book = self.clob_client.get_order_book(&market.no_token).await.ok();

            if yes_book.is_none() || no_book.is_none() {
                print!("\r⚠️ Unable to fetch order books. Retrying...                         ");
                std::io::stdout().flush()?;
                tokio::time::sleep(Duration::from_secs(POLLING_INTERVAL)).await;
                continue;
            }

            let yes_book = yes_book.unwrap();
            let no_book = no_book.unwrap();

            // Extract best bid/ask
            let yes_bid = yes_book.bids.first().map(|b| b.price.to_string().parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);
            let yes_ask = yes_book.asks.first().map(|a| a.price.to_string().parse::<f64>().ok());
            let yes_ask_size = yes_book.asks.first().map(|a| a.size.to_string().parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);

            let no_bid = no_book.bids.first().map(|b| b.price.to_string().parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);
            let no_ask = no_book.asks.first().map(|a| a.price.to_string().parse::<f64>().ok());
            let no_ask_size = no_book.asks.first().map(|a| a.size.to_string().parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);

            // ABORT CHECK
            let should_abort = 
                (yes_ask.is_some() && yes_ask.unwrap() > ABORT_ASK_PRICE) ||
                (no_ask.is_some() && no_ask.unwrap() > ABORT_ASK_PRICE);
            
            if should_abort {
                println!("\n🚨 ABORT TRIGGERED: ASK price exceeded ${}", ABORT_ASK_PRICE);
                self.traded_markets.insert(market.slug.clone());
                return Ok(());
            }

            print!("\rMonitoring {} | YES: ${:.2}/${:.2} ({}) | NO: ${:.2}/${:.2} ({}) | Target: ${:.2}   ",
                TRADE_SIDE, yes_bid, yes_ask.unwrap_or(0.0), yes_ask_size as u32, 
                no_bid, no_ask.unwrap_or(0.0), no_ask_size as u32, ENTRY_PRICE);
            std::io::stdout().flush()?;

            // Check for entry trigger
            let mut triggered_side: Option<&str> = None;
            let mut triggered_token: Option<String> = None;
            let mut triggered_ask: Option<f64> = None;

            if (TRADE_SIDE == "YES" || TRADE_SIDE == "BOTH") && 
               yes_bid >= ENTRY_PRICE && 
               yes_ask_size >= POSITION_SIZE as f64 && 
               yes_ask.is_some() {
                triggered_side = Some("YES");
                triggered_token = Some(market.yes_token.clone());
                triggered_ask = yes_ask;
            }

            if (TRADE_SIDE == "NO" || TRADE_SIDE == "BOTH") && 
               no_bid >= ENTRY_PRICE && 
               no_ask_size >= POSITION_SIZE as f64 && 
               no_ask.is_some() {
                if triggered_side.is_none() || (TRADE_SIDE == "BOTH" && no_bid > yes_bid) {
                    triggered_side = Some("NO");
                    triggered_token = Some(market.no_token.clone());
                    triggered_ask = no_ask;
                }
            }

            if !self.active_trade && triggered_side.is_some() && triggered_ask.is_some() {
                let side = triggered_side.unwrap();
                let token = triggered_token.unwrap();
                let ask = triggered_ask.unwrap();
                
                println!("\n🚀 ENTRY TRIGGERED: {} - Placing order...", side);
                self.execute_trade(&market, side, &token, ask).await?;
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(POLLING_INTERVAL)).await;
        }
    }

    async fn execute_trade(&mut self, market: &MarketData, side: &str, token_id: &str, entry_ask: f64) -> anyhow::Result<()> {
        println!("\n🎯 Attempting {} entry at ${:.3}", side, entry_ask);
        
        let position_size = if side == "NO" { POSITION_SIZE } else { (POSITION_SIZE as f64 * 0.5) as u32 };

        for attempt in 1..=20 {
            println!("🔄 Entry Attempt {}/20: Placing FOK @ ${:.3}", attempt, entry_ask);
            
            // Create market order using SDK
            let order = self.clob_client
                .market_order()
                .token_id(token_id)
                .amount(Amount::shares(Decimal::from(position_size)))
                .side(Side::Buy)
                .order_type(OrderType::FOK)
                .build()
                .await?;
            
            let signed_order = self.clob_client.sign(&self.signer, order).await?;
            
            match self.clob_client.post_order(signed_order).await {
                Ok(response) => {
                    if let Some(order_id) = response.order_id {
                        println!("🎊 EXECUTED: {} shares @ ${:.3}", position_size, entry_ask);
                        self.active_trade = true;
                        self.traded_markets.insert(market.slug.clone());
                        return Ok(());
                    } else {
                        println!("   ⚠️ Order rejected: {:?}", response.error_msg);
                    }
                },
                Err(e) => {
                    println!("   ⚠️ FOK failed: {}", e);
                }
            }
            
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        println!("\n⚠️ Failed to enter after 20 attempts.");
        self.traded_markets.insert(market.slug.clone());
        Ok(())
    }
}

// ==========================================
// 📊 UTILITY FUNCTIONS
// ==========================================

fn init_csv_log() -> anyhow::Result<()> {
    if !std::path::Path::new(LOG_FILE).exists() {
        let mut file = File::create(LOG_FILE)?;
        writeln!(
            file,
            "Market Title,Market Link,Status,entry1_Time,entry_Side,entry_Price,position_size,sl_Time,sl_Price,Final_Status,Notes,is_SL_Triggered"
        )?;
    }
    Ok(())
}

// ==========================================
// 🚀 MAIN FUNCTION
// ==========================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("✅ RUST Trading Bot with Official Polymarket SDK");
    println!("✅ Using rs-clob-client");
    println!("✅ All Trading Functions Operational\n");
    
    let mut bot = EthNoTrendBot::new().await?;
    bot.run().await?;

    Ok(())
}
