use chrono::{DateTime, Utc, TimeZone};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::Address;
use std::str::FromStr;

// ==========================================
// 📊 CONFIGURATION CONSTANTS
// ==========================================
const PRIVATE_KEY: &str = "0xbbd185bb356315b5f040a2af2fa28549177f3087559bb76885033e9cf8e8bf34";
const POLYMARKET_ADDRESS: &str = "0xC47167d407A91965fAdc7aDAb96F0fF586566bF7";

const TRADE_SIDE: &str = "BOTH"; // Options: "YES", "NO", or "BOTH"
const ENTRY_PRICE: f64 = 0.96;
const STOP_LOSS_PRICE: f64 = 0.89;
const SUSTAIN_TIME: u64 = 3; // seconds
const POSITION_SIZE: u32 = 5;
const MARKET_WINDOW: u64 = 240; // seconds
const POLLING_INTERVAL: u64 = 1; // seconds
const ENTRY_TIMEOUT: u64 = 210; // seconds
const SL_TIMEOUT: u64 = 10; // seconds
const ABORT_ASK_PRICE: f64 = 0.99;

const HOST: &str = "https://clob.polymarket.com";
const DATA_API_URL: &str = "https://data-api.polymarket.com"; 
const GAMMA_API_URL: &str = "https://gamma-api.polymarket.com";
const CHAIN_ID: u64 = 137;

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

#[derive(Debug, Clone)]
struct OrderBook {
    best_ask: Option<f64>,
    ask_size: f64,
    best_bid: Option<f64>,
    bid_size: f64,
}

#[derive(Debug, Clone)]
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

impl Default for TradeRecord {
    fn default() -> Self {
        Self {
            title: "-".to_string(),
            link: "-".to_string(),
            status: "-".to_string(),
            entry1_time: "-".to_string(),
            entry_side: "-".to_string(),
            entry_price: "-".to_string(),
            position_size: "-".to_string(),
            sl_time: "-".to_string(),
            sl_price: "-".to_string(),
            final_status: "-".to_string(),
            notes: "-".to_string(),
            is_sl_triggered: "-".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PositionData {
    asset: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct OrderBookLevel {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct OrderBookResponse {
    #[serde(default)]
    asks: Vec<OrderBookLevel>,
    #[serde(default)]
    bids: Vec<OrderBookLevel>,
}

// ==========================================
// 🤖 MAIN BOT STRUCTURE
// ==========================================

struct EthNoTrendBot {
    client: Client,
    wallet: LocalWallet,
    trading_address: Address,
    use_proxy: bool,
    active_trade: bool,
    traded_markets: HashSet<String>,
    log_filename: String,
}

impl EthNoTrendBot {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        println!("🤖 ETH No Trend Bot Starting...");
        println!("📊 Configuration:");
        println!("   Trade Side: {}", TRADE_SIDE);
        println!("   Entry Price: ${}", ENTRY_PRICE);
        println!("   Stop Loss: ${}", STOP_LOSS_PRICE);
        println!("   Position Size: {} shares", POSITION_SIZE);
        println!("   Trading Window: Last {}s of market", MARKET_WINDOW);
        println!("   🚨 ABORT Trigger: ASK > ${}\n", ABORT_ASK_PRICE);

        // Validate TRADE_SIDE
        if !["YES", "NO", "BOTH"].contains(&TRADE_SIDE) {
            return Err(format!("❌ Invalid TRADE_SIDE: {}. Must be 'YES', 'NO', or 'BOTH'", TRADE_SIDE).into());
        }

        // Initialize wallet
        let wallet = PRIVATE_KEY.parse::<LocalWallet>()?;
        let wallet_address = wallet.address();
        let polymarket_addr = Address::from_str(POLYMARKET_ADDRESS)?;

        let (use_proxy, trading_address) = if wallet_address == polymarket_addr {
            (false, wallet_address)
        } else {
            (true, polymarket_addr)
        };

        // Initialize log file
        let log_filename = format!(
            "ETH_NO_trend_bot_terminal_log_{}.txt",
            Utc::now().format("%Y%m%d_%H%M%S")
        );

        init_csv_log()?;

        println!("✅ Client Ready. Trading as: {:?}\n", trading_address);

        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            wallet,
            trading_address,
            use_proxy,
            active_trade: false,
            traded_markets: HashSet::new(),
            log_filename,
        })
    }

    fn floor_round(&self, n: f64, decimals: u32) -> f64 {
        let multiplier = 10_f64.powi(decimals as i32);
        (n * multiplier).floor() / multiplier
    }

    fn get_all_shares_available(&self, yes_token: &str, no_token: &str) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        for attempt in 1..=5 {
            println!("🔍 Accessing Data API for position verification (Attempt {}/5)...", attempt);
            
            match self.fetch_positions(yes_token, no_token) {
                Ok(balances) => return Ok(balances),
                Err(e) => {
                    println!("⚠️ Balance API attempt {} failed: {}", attempt, e);
                    if attempt < 5 {
                        thread::sleep(Duration::from_secs(2));
                    } else {
                        return Err("❌ Critical: Balance API failed after 5 attempts. Aborting market.".into());
                    }
                }
            }
        }
        Err("Data API Unreachable".into())
    }

    fn fetch_positions(&self, yes_token: &str, no_token: &str) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        let url = format!("{}/positions?user={:?}", DATA_API_URL, self.trading_address);
        let resp: Vec<PositionData> = self.client.get(&url)
            .timeout(Duration::from_secs(3))
            .send()?
            .json()?;

        let mut balances = HashMap::new();
        balances.insert("yes".to_string(), 0.0);
        balances.insert("no".to_string(), 0.0);

        for pos in resp {
            let size = self.floor_round(pos.size.parse::<f64>().unwrap_or(0.0), 1);
            if pos.asset == yes_token {
                balances.insert("yes".to_string(), size);
                println!("    📊 YES Position: {} shares", size);
            } else if pos.asset == no_token {
                balances.insert("no".to_string(), size);
                println!("    📊 NO Position: {} shares", size);
            }
        }

        Ok(balances)
    }

    fn get_order_book_depth(&self, token_id: &str) -> Option<OrderBook> {
        for attempt in 1..=3 {
            match self.fetch_order_book(token_id) {
                Ok(book) => return Some(book),
                Err(e) => {
                    println!("⚠️ Order book fetch error (attempt {}/3): {}", attempt, e);
                    if attempt < 3 {
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        }
        None
    }

    fn fetch_order_book(&self, token_id: &str) -> Result<OrderBook, Box<dyn std::error::Error>> {
        let url = format!("{}/book?token_id={}", HOST, token_id);
        let resp: OrderBookResponse = self.client.get(&url)
            .send()?
            .json()?;

        let (best_ask, ask_size) = if let Some(ask) = resp.asks.iter()
            .min_by(|a, b| a.price.parse::<f64>().unwrap_or(f64::MAX)
                .partial_cmp(&b.price.parse::<f64>().unwrap_or(f64::MAX))
                .unwrap()) {
            (Some(ask.price.parse::<f64>()?), ask.size.parse::<f64>()?)
        } else {
            (None, 0.0)
        };

        let (best_bid, bid_size) = if let Some(bid) = resp.bids.iter()
            .max_by(|a, b| a.price.parse::<f64>().unwrap_or(0.0)
                .partial_cmp(&b.price.parse::<f64>().unwrap_or(0.0))
                .unwrap()) {
            (Some(bid.price.parse::<f64>()?), bid.size.parse::<f64>()?)
        } else {
            (None, 0.0)
        };

        Ok(OrderBook {
            best_ask,
            ask_size,
            best_bid,
            bid_size,
        })
    }

    fn get_market_from_slug(&self, slug: &str) -> Option<MarketData> {
        for attempt in 1..=3 {
            println!("   🔍 Fetching market '{}' (Attempt {}/3)", slug, attempt);
            
            match self.fetch_market_data(slug) {
                Ok(Some(market)) => return Some(market),
                Ok(None) => return None,
                Err(e) => {
                    println!("   ⚠️ Market fetch attempt {}/3 failed: {}", attempt, e);
                    if attempt < 3 {
                        thread::sleep(Duration::from_secs(3));
                    }
                }
            }
        }
        None
    }

    fn fetch_market_data(&self, slug: &str) -> Result<Option<MarketData>, Box<dyn std::error::Error>> {
        let url = format!("{}/events?slug={}", GAMMA_API_URL, slug);
        let resp = self.client.get(&url)
            .timeout(Duration::from_secs(10))
            .send()?;

        if resp.status() == 404 {
            println!("   ⚠️ 404 Error: Market '{}' not found", slug);
            return Ok(None);
        }

        if !resp.status().is_success() {
            println!("   ⚠️ HTTP {}: Request failed", resp.status());
            return Ok(None);
        }

        let data: Vec<Value> = resp.json()?;
        
        if data.is_empty() {
            println!("   ⚠️ Empty response from API");
            println!("   💤 Sleeping for 5 minutes before retrying...");
            thread::sleep(Duration::from_secs(300));
            return Ok(None);
        }

        let event = &data[0];
        let markets = event["markets"].as_array().ok_or("No markets found")?;
        
        if markets.is_empty() {
            return Ok(None);
        }

        let market_data = &markets[0];
        let token_ids: Vec<String> = serde_json::from_str(
            market_data["clobTokenIds"].as_str().ok_or("Invalid clobTokenIds")?
        )?;

        if token_ids.len() < 2 {
            return Ok(None);
        }

        let title = event["title"].as_str().unwrap_or(slug).to_string();
        println!("   ✅ Market found: {}", title);

        Ok(Some(MarketData {
            slug: slug.to_string(),
            title,
            link: format!("https://polymarket.com/event/{}", slug),
            yes_token: token_ids[0].clone(),
            no_token: token_ids[1].clone(),
        }))
    }

    fn monitor_market(&mut self, market: MarketData, ts: u64) {
        println!("\n{'='*60}");
        println!("📊 MONITORING: {}", market.title);
        println!("🔗 Link: {}", market.link);
        println!("{'='*60}");

        let start_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        
        loop {
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let elapsed = current_time - start_time;
            let time_until_close = 900 - elapsed;

            // Check if we're outside the trading window
            if time_until_close > MARKET_WINDOW {
                print!("⏳ Waiting for trading window ({}s remaining)...\r", time_until_close - MARKET_WINDOW);
                io::stdout().flush().unwrap();
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            // Market closed
            if time_until_close <= 0 {
                println!("\n⏰ Market closed. Moving to next market.");
                self.traded_markets.insert(market.slug.clone());
                return;
            }

            // Check YES side if applicable
            if TRADE_SIDE == "YES" || TRADE_SIDE == "BOTH" {
                if let Some(book) = self.get_order_book_depth(&market.yes_token) {
                    if let Some(ask) = book.best_ask {
                        if ask > ABORT_ASK_PRICE {
                            println!("\n🚨 ABORT: YES ASK ${:.3} exceeds threshold ${}", ask, ABORT_ASK_PRICE);
                            self.save_abort_log(&market, "YES", ask);
                            self.traded_markets.insert(market.slug.clone());
                            return;
                        }
                    }

                    if let Some(bid) = book.best_bid {
                        if bid >= ENTRY_PRICE && !self.active_trade {
                            println!("\n✅ YES Entry Trigger! Bid: ${:.3} >= ${}", bid, ENTRY_PRICE);
                            self.execute_trade(&market, "YES", bid, time_until_close);
                            return;
                        }
                    }
                }
            }

            // Check NO side if applicable
            if TRADE_SIDE == "NO" || TRADE_SIDE == "BOTH" {
                if let Some(book) = self.get_order_book_depth(&market.no_token) {
                    if let Some(ask) = book.best_ask {
                        if ask > ABORT_ASK_PRICE {
                            println!("\n🚨 ABORT: NO ASK ${:.3} exceeds threshold ${}", ask, ABORT_ASK_PRICE);
                            self.save_abort_log(&market, "NO", ask);
                            self.traded_markets.insert(market.slug.clone());
                            return;
                        }
                    }

                    if let Some(bid) = book.best_bid {
                        if bid >= ENTRY_PRICE && !self.active_trade {
                            println!("\n✅ NO Entry Trigger! Bid: ${:.3} >= ${}", bid, ENTRY_PRICE);
                            self.execute_trade(&market, "NO", bid, time_until_close);
                            return;
                        }
                    }
                }
            }

            print!("📊 Monitoring... {}s until close | Bid check in progress...\r", time_until_close);
            io::stdout().flush().unwrap();
            thread::sleep(Duration::from_secs(POLLING_INTERVAL));
        }
    }

    fn execute_trade(&mut self, market: &MarketData, side: &str, entry_bid: f64, time_remaining: u64) {
        println!("\n🎯 Attempting {} entry at ${:.3}", side, entry_bid);
        
        let mut log_rec = TradeRecord {
            title: market.title.clone(),
            link: market.link.clone(),
            entry_side: side.to_string(),
            entry_price: format!("{:.3}", entry_bid),
            position_size: POSITION_SIZE.to_string(),
            ..Default::default()
        };

        // Simulate order placement (in real implementation, call Polymarket API)
        println!("📝 Placing {} order for {} shares at ${:.3}", side, POSITION_SIZE, entry_bid);
        
        // Wait for entry with timeout
        let entry_start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut filled = false;

        while SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - entry_start < ENTRY_TIMEOUT {
            // Check if order filled (simplified - in real implementation, check order status)
            println!("⏳ Waiting for fill... ({}s elapsed)", 
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - entry_start);
            
            // Simulate fill after random time (replace with actual order status check)
            thread::sleep(Duration::from_secs(2));
            filled = true;
            break;
        }

        if !filled {
            println!("❌ Entry order timeout");
            log_rec.status = "ENTRY_TIMEOUT".to_string();
            log_rec.final_status = "NO_POSITION".to_string();
            self.save_log(&log_rec);
            self.traded_markets.insert(market.slug.clone());
            return;
        }

        println!("✅ Order filled! Position entered.");
        log_rec.entry1_time = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        log_rec.status = "POSITION_ENTERED".to_string();
        self.active_trade = true;

        // Monitor stop loss
        self.monitor_stop_loss(market, side, &mut log_rec);
        self.traded_markets.insert(market.slug.clone());
    }

    fn monitor_stop_loss(&mut self, market: &MarketData, side: &str, log_rec: &mut TradeRecord) {
        println!("\n🛡️ Stop Loss Monitor Active (Trigger: ${:.3})", STOP_LOSS_PRICE);
        
        let token_id = if side == "YES" { &market.yes_token } else { &market.no_token };
        let mut breach_start: Option<u64> = None;

        loop {
            if let Some(book) = self.get_order_book_depth(token_id) {
                if let Some(current_bid) = book.best_bid {
                    if current_bid < STOP_LOSS_PRICE {
                        if breach_start.is_none() {
                            breach_start = Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
                            println!("\n⚠️ {} price ${:.3} below SL ${:.3}. Timer started...", 
                                side, current_bid, STOP_LOSS_PRICE);
                        }

                        let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() 
                            - breach_start.unwrap();
                        
                        if elapsed >= SUSTAIN_TIME {
                            println!("\n🚨 STOP LOSS TRIGGERED! Price sustained below ${:.3} for {}s", 
                                STOP_LOSS_PRICE, SUSTAIN_TIME);
                            
                            log_rec.sl_time = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
                            log_rec.sl_price = format!("{:.3}", current_bid);
                            log_rec.final_status = "STOP_LOSS_EXECUTED".to_string();
                            log_rec.is_sl_triggered = "YES".to_string();
                            
                            self.save_log(log_rec);
                            self.active_trade = false;
                            println!("📉 Position Liquidated.");
                            return;
                        }
                    } else {
                        if breach_start.is_some() {
                            println!("\n✅ {} price recovered to ${:.3}. Resetting timer.", side, current_bid);
                        }
                        breach_start = None;
                    }
                }
            }

            thread::sleep(Duration::from_millis(500));
        }
    }

    fn save_abort_log(&self, market: &MarketData, side: &str, abort_ask: f64) {
        let log_rec = TradeRecord {
            title: market.title.clone(),
            link: market.link.clone(),
            status: "ABORTED".to_string(),
            entry_side: side.to_string(),
            final_status: "MARKET_ABORTED".to_string(),
            notes: format!("{} ASK ${:.3} exceeded abort threshold ${}", side, abort_ask, ABORT_ASK_PRICE),
            ..Default::default()
        };
        self.save_log(&log_rec);
    }

    fn save_log(&self, record: &TradeRecord) {
        if let Ok(mut file) = OpenOptions::new().append(true).open(LOG_FILE) {
            let line = format!(
                "{},{},{},{},{},{},{},{},{},{},{},{}\n",
                record.title, record.link, record.status,
                record.entry1_time, record.entry_side, record.entry_price,
                record.position_size, record.sl_time, record.sl_price,
                record.final_status, record.notes, record.is_sl_triggered
            );
            let _ = file.write_all(line.as_bytes());
        }
    }

    fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 ETH No Trend Bot Running...\n");

        loop {
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let ts = (current_time / 900) * 900;
            let slug = format!("eth-updown-15m-{}", ts);

            let elapsed_since_open = current_time - ts;
            let time_until_next = 900 - elapsed_since_open;

            let open_time = Utc.timestamp_opt(ts as i64, 0).unwrap().format("%H:%M:%S");
            print!("\n⏰ Current Market: {} | Open Time: {} | Next in: {}s\r", 
                slug, open_time, time_until_next);
            io::stdout().flush()?;

            // Skip if already traded
            if self.traded_markets.contains(&slug) {
                print!("   ✓ Already traded this market. Waiting for next...\r");
                io::stdout().flush()?;
                thread::sleep(Duration::from_secs(60));
                continue;
            }

            // Wait for market to be created
            if elapsed_since_open < 5 {
                print!("   ⏳ Market just opened. Waiting 5s for API indexing...\r");
                io::stdout().flush()?;
                thread::sleep(Duration::from_secs(5));
                continue;
            }

            if let Some(market) = self.get_market_from_slug(&slug) {
                self.monitor_market(market, ts);
            } else {
                print!("⚠️ Unable to fetch market {}. Retrying...\r", slug);
                io::stdout().flush()?;
                thread::sleep(Duration::from_secs(2));
            }

            thread::sleep(Duration::from_secs(1));
        }
    }
}

// ==========================================
// 🔧 UTILITY FUNCTIONS
// ==========================================

fn init_csv_log() -> Result<(), Box<dyn std::error::Error>> {
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
// 🚀 MAIN ENTRY POINT
// ==========================================

fn main() {
    match EthNoTrendBot::new() {
        Ok(mut bot) => {
            if let Err(e) = bot.run() {
                eprintln!("\n❌ Bot error: {}", e);
            }
        }
        Err(e) => {
            eprintln!("❌ Failed to initialize bot: {}", e);
            std::process::exit(1);
        }
    }
}
