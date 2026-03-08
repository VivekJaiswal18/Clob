// use reqwest::Client;
// use rmp_serde;
// use serde::{Deserialize, Serialize};
// use std::sync::Arc;
// use std::time::{Duration, Instant};
// use sysinfo::{CpuRefreshKind, RefreshKind, System};
// use tokio::sync::Mutex;
// use wincode_derive::{SchemaRead, SchemaWrite};

// // #[derive(Serialize, Deserialize, Clone)]
// #[derive(SchemaRead, SchemaWrite, Clone)]
// pub enum Side{
//     Buy,
//     Sell
// }

// // #[derive(Serialize, Deserialize)]
// #[derive(SchemaRead, SchemaWrite)]
// struct CreateOrderInput {
//     quantity: u64,
//     side: Side,
//     price: u64,
//     // user_id: u32,
// }

// struct TaskStats {
//     latencies: Vec<f64>,
//     failed: usize,
// }

// impl TaskStats {
//     fn new(capacity: usize) -> Self {
//         Self {
//             latencies: Vec::with_capacity(capacity),
//             failed: 0,
//         }
//     }

//     fn record(&mut self, duration_ms: f64, ok: bool) {
//         self.latencies.push(duration_ms);
//         if !ok {
//             self.failed += 1;
//         }
//     }
// }

// struct Stats {
//     latencies: Vec<f64>,
//     total: usize,
//     failed: usize,
// }

// impl Stats {
//     fn from_task_stats(task_stats: Vec<TaskStats>) -> Self {
//         let mut all_latencies = Vec::new();
//         let mut total_failed = 0;

//         for ts in task_stats {
//             all_latencies.extend(ts.latencies);
//             total_failed += ts.failed;
//         }

//         let total = all_latencies.len();

//         Self {
//             latencies: all_latencies,
//             total,
//             failed: total_failed,
//         }
//     }

//     fn summarize(&mut self) {
//         if self.latencies.is_empty() {
//             println!("No requests recorded.");
//             return;
//         }

//         self.latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
//         let total = self.latencies.len();

//         let avg: f64 = self.latencies.iter().sum::<f64>() / total as f64;
//         let p50 = self.latencies[((0.50 * total as f64) as usize).min(total - 1)];
//         let p95 = self.latencies[((0.95 * total as f64) as usize).min(total - 1)];
//         let p99 = self.latencies[((0.99 * total as f64) as usize).min(total - 1)];

//         println!("\n========== CLIENT-SIDE MEASUREMENTS ==========");
//         println!("Total Requests:   {}", total);
//         println!("Failed Requests:  {}", self.failed);
//         println!("Average Latency:  {:.2} ms", avg);
//         println!("P50:              {:.2} ms", p50);
//         println!("P95:              {:.2} ms", p95);
//         println!("P99:              {:.2} ms", p99);
//         println!("==============================================\n");
//     }
// }

// async fn monitor_cpu(stop: Arc<Mutex<bool>>) {
//     let mut sys =
//         System::new_with_specifics(RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()));
//     let mut samples = vec![];

//     loop {
//         if *stop.lock().await {
//             break;
//         }

//         sys.refresh_cpu_specifics(CpuRefreshKind::everything());
//         let cpus = sys.cpus();
//         if cpus.is_empty() {
//             continue;
//         }

//         let total_usage: f32 = cpus.iter().map(|c| c.cpu_usage()).sum();
//         let avg = total_usage / cpus.len() as f32;
//         samples.push(avg);

//         tokio::time::sleep(Duration::from_millis(500)).await;
//     }

//     if !samples.is_empty() {
//         let avg_cpu: f32 = samples.iter().sum::<f32>() / samples.len() as f32;
//         let peak = samples.iter().cloned().fold(0.0, f32::max);
//         println!("\n========== CPU USAGE (Test Client) ==========");
//         println!("Average CPU:  {:.1}%", avg_cpu);
//         println!("Peak CPU:     {:.1}%", peak);
//         println!("Samples:      {}", samples.len());
//         println!("=============================================\n");
//     }
// }

// async fn fetch_server_metrics(client: &Client) {
//     match client.get("http://127.0.0.1:8080/metrics").send().await {
//         Ok(resp) => {
//             if let Ok(text) = resp.text().await {
//                 println!("\n========== SERVER-SIDE METRICS ==========");

//                 for line in text.lines() {
//                     if line.starts_with("http_requests_total ") {
//                         if let Some(val) = line.split_whitespace().nth(1) {
//                             println!("HTTP Requests:        {}", val);
//                         }
//                     }
//                     if line.starts_with("orders_matched_total ") {
//                         if let Some(val) = line.split_whitespace().nth(1) {
//                             println!("Orders Matched:       {}", val);
//                         }
//                     }
//                     if line.starts_with("trades_executed_total ") {
//                         if let Some(val) = line.split_whitespace().nth(1) {
//                             println!("Trades Executed:      {}", val);
//                         }
//                     }
//                     if line.contains("http_request_latency_ms_sum") {
//                         if let Some(val) = line.split_whitespace().nth(1) {
//                             if let Ok(num) = val.parse::<f64>() {
//                                 println!("HTTP Latency Sum:     {:.2} ms", num);
//                             }
//                         }
//                     }
//                     if line.contains("http_request_latency_ms_count") {
//                         if let Some(val) = line.split_whitespace().nth(1) {
//                             println!("HTTP Latency Count:   {}", val);
//                         }
//                     }
//                     if line.contains("matching_engine_latency_ms_sum") {
//                         if let Some(val) = line.split_whitespace().nth(1) {
//                             if let Ok(num) = val.parse::<f64>() {
//                                 println!("Matching Latency Sum: {:.2} ms", num);
//                             }
//                         }
//                     }
//                 }

//                 println!("=========================================\n");
//             }
//         }
//         Err(_) => {
//             println!("\nCould not fetch server metrics (is the server running?)\n");
//         }
//     }
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
// async fn msgpack_stress_test_with_cpu() {
//     let base_url = "http://127.0.0.1:8080";
//     let total_requests = 500_000;
//     let concurrency = 2_000;
//     let requests_per_task = total_requests / concurrency;

//     let client = Arc::new(
//         Client::builder()
//             .pool_max_idle_per_host(500)
//             .pool_idle_timeout(Duration::from_secs(90))
//             .build()
//             .unwrap(),
//     );
//     let stop = Arc::new(Mutex::new(false));

//     println!("\n========== MessagePack Stress Test ==========");
//     println!("Total Requests:  {}", total_requests);
//     println!("Concurrency:     {}", concurrency);
//     println!("=============================================\n");

//     let stop_monitor = stop.clone();
//     tokio::spawn(async move {
//         monitor_cpu(stop_monitor).await;
//     });

//     let start_time = Instant::now();
//     let mut handles = vec![];

//     for i in 0..concurrency {
//         let client = client.clone();
//         let url = base_url.to_string();

//         handles.push(tokio::spawn(async move {
//             let mut task_stats = TaskStats::new(requests_per_task);

//             for j in 0..requests_per_task {
//                 let request_start = Instant::now();

//                 // let side = if (i + j) % 2 == 0 { Side::Buy } else { Side::Sell };
//                 let side = if (i + j) % 2 == 0 { Side::Buy } else { Side::Sell };
//                 let price = (10000 + ((i * j) % 2000)) as u64;
//                 let qty = (1 + ((i + j) % 20)) as u64;
//                 let user_id = 1000 + (i % 1000);

//                 let input = CreateOrderInput {
//                     quantity: qty,
//                     side: side,
//                     price,
//                     // user_id: user_id.try_into().unwrap(),
//                 };

//                 // let body = rmp_serde::to_vec(&input).unwrap();
//                 // let body = serde_json::to_vec(&input).unwrap();
//                 let body = wincode::serialize(&input).unwrap();

//                 let ok = client
//                     .post(format!("{}/order", url))
//                     // .header("Content-Type", "application/msgpack")
//                     // .header("Accept", "application/msgpack")
//                     // .header("Content-Type", "application/json")
//                     // .header("Accept", "application/json")
//                     .header("Content-Type", "application/octet-stream")
//                     .header("Accept", "application/octet-stream")
//                     .body(body)
//                     .send()
//                     .await
//                     .map(|r| r.status().is_success())
//                     .unwrap_or(false);

//                 let elapsed_ms = request_start.elapsed().as_secs_f64() * 1000.0;
//                 task_stats.record(elapsed_ms, ok);
//             }

//             task_stats
//         }));
//     }

//     let mut all_task_stats = Vec::with_capacity(concurrency);
//     for h in handles {
//         if let Ok(task_stats) = h.await {
//             all_task_stats.push(task_stats);
//         }
//     }

//     *stop.lock().await = true;

//     let total_time = start_time.elapsed().as_secs_f64();
//     let mut stats = Stats::from_task_stats(all_task_stats);
//     let rps = stats.total as f64 / total_time;

//     println!("\n========== TEST SUMMARY ==========");
//     println!("Total Time:   {:.2}s", total_time);
//     println!("Throughput:   {:.2} req/sec", rps);
//     println!("==================================");

//     stats.summarize();
//     fetch_server_metrics(&client).await;
// }







// use reqwest::Client;
// use serde::{Deserialize, Serialize};
// use std::sync::Arc;
// use std::time::{Duration, Instant};
// use tokio::sync::Mutex;

// #[derive(Serialize, Deserialize, Clone)]
// pub enum Side {
//     Buy,
//     Sell,
// }

// #[derive(Serialize, Deserialize)]
// struct CreateOrderInput {
//     quantity: u64,
//     side: Side,
//     price: u64,
// }

// #[derive(Deserialize, Debug)]
// struct Depth {
//     asks: Vec<[u64; 2]>,  // [price, quantity]
//     bids: Vec<[u64; 2]>,
//     last_update_id: String,
// }

// struct TaskStats {
//     latencies: Vec<f64>,
//     failed: usize,
// }

// impl TaskStats {
//     fn new(capacity: usize) -> Self {
//         Self {
//             latencies: Vec::with_capacity(capacity),
//             failed: 0,
//         }
//     }

//     fn record(&mut self, duration_ms: f64, ok: bool) {
//         self.latencies.push(duration_ms);
//         if !ok {
//             self.failed += 1;
//         }
//     }
// }

// struct Stats {
//     latencies: Vec<f64>,
//     total: usize,
//     failed: usize,
// }

// impl Stats {
//     fn from_task_stats(task_stats: Vec<TaskStats>) -> Self {
//         let mut all_latencies = Vec::new();
//         let mut total_failed = 0;

//         for ts in task_stats {
//             all_latencies.extend(ts.latencies);
//             total_failed += ts.failed;
//         }

//         let total = all_latencies.len();

//         Self {
//             latencies: all_latencies,
//             total,
//             failed: total_failed,
//         }
//     }

//     fn summarize(&mut self) {
//         if self.latencies.is_empty() {
//             println!("No requests recorded.");
//             return;
//         }

//         self.latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
//         let total = self.latencies.len();

//         let avg: f64 = self.latencies.iter().sum::<f64>() / total as f64;
//         let p50 = self.latencies[((0.50 * total as f64) as usize).min(total - 1)];
//         let p95 = self.latencies[((0.95 * total as f64) as usize).min(total - 1)];
//         let p99 = self.latencies[((0.99 * total as f64) as usize).min(total - 1)];

//         println!("\n========== CLIENT-SIDE MEASUREMENTS ==========");
//         println!("Total Requests:   {}", total);
//         println!("Failed Requests:  {}", self.failed);
//         println!("Average Latency:  {:.2} ms", avg);
//         println!("P50:              {:.2} ms", p50);
//         println!("P95:              {:.2} ms", p95);
//         println!("P99:              {:.2} ms", p99);
//         println!("==============================================\n");
//     }
// }

// /// Fetch orderbook depth to verify matching happened
// async fn get_depth(client: &Client, base_url: &str) -> Option<Depth> {
//     client
//         .get(format!("{}/depth", base_url))
//         .send()
//         .await
//         .ok()?
//         .json::<Depth>()
//         .await
//         .ok()
// }

// /// Calculate total quantity in orderbook (asks + bids)
// fn total_orderbook_quantity(depth: &Depth) -> u64 {
//     let ask_qty: u64 = depth.asks.iter().map(|level| level[1]).sum();
//     let bid_qty: u64 = depth.bids.iter().map(|level| level[1]).sum();
//     ask_qty + bid_qty
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
// async fn matching_performance_test() {
//     let base_url = "http://127.0.0.1:8080";
//     let total_requests = 100_000;  // Reduced for matching test
//     let concurrency = 500;
//     let requests_per_task = total_requests / concurrency;
    
//     // Use a single price so orders WILL match
//     let match_price: u64 = 10000;

//     let client = Arc::new(
//         Client::builder()
//             .pool_max_idle_per_host(100)
//             .pool_idle_timeout(Duration::from_secs(90))
//             .build()
//             .unwrap(),
//     );

//     println!("\n========== Matching Performance Test ==========");
//     println!("Total Requests:  {}", total_requests);
//     println!("Concurrency:     {}", concurrency);
//     println!("Match Price:     {}", match_price);
//     println!("================================================\n");

//     // Get initial depth
//     let initial_depth = get_depth(&client, base_url).await;
//     let initial_qty = initial_depth.as_ref().map(total_orderbook_quantity).unwrap_or(0);
//     println!("Initial orderbook quantity: {}", initial_qty);

//     let start_time = Instant::now();
//     let mut handles = vec![];

//     // Track total quantity sent
//     let total_buy_qty = Arc::new(std::sync::atomic::AtomicU64::new(0));
//     let total_sell_qty = Arc::new(std::sync::atomic::AtomicU64::new(0));

//     for i in 0..concurrency {
//         let client = client.clone();
//         let url = base_url.to_string();
//         let buy_qty_counter = total_buy_qty.clone();
//         let sell_qty_counter = total_sell_qty.clone();

//         handles.push(tokio::spawn(async move {
//             let mut task_stats = TaskStats::new(requests_per_task);

//             for j in 0..requests_per_task {
//                 let request_start = Instant::now();

//                 // Alternate buy/sell at SAME PRICE to force matching
//                 let side = if (i + j) % 2 == 0 { Side::Buy } else { Side::Sell };
//                 let qty = (1 + ((i + j) % 10)) as u64;  // Quantity 1-10

//                 // Track quantities
//                 match side {
//                     Side::Buy => buy_qty_counter.fetch_add(qty, std::sync::atomic::Ordering::Relaxed),
//                     Side::Sell => sell_qty_counter.fetch_add(qty, std::sync::atomic::Ordering::Relaxed),
//                 };

//                 let input = CreateOrderInput {
//                     quantity: qty,
//                     side,
//                     price: match_price,  // Same price = will match!
//                 };

//                 let body = serde_json::to_vec(&input).unwrap();

//                 let ok = client
//                     .post(format!("{}/order", url))
//                     .header("Content-Type", "application/json")
//                     .header("Accept", "application/json")
//                     .body(body)
//                     .send()
//                     .await
//                     .map(|r| r.status().is_success())
//                     .unwrap_or(false);

//                 let elapsed_ms = request_start.elapsed().as_secs_f64() * 1000.0;
//                 task_stats.record(elapsed_ms, ok);
//             }

//             task_stats
//         }));
//     }

//     let mut all_task_stats = Vec::with_capacity(concurrency);
//     for h in handles {
//         if let Ok(task_stats) = h.await {
//             all_task_stats.push(task_stats);
//         }
//     }

//     let total_time = start_time.elapsed().as_secs_f64();

//     // Wait a bit for matching engine to process remaining orders
//     tokio::time::sleep(Duration::from_millis(500)).await;

//     // Get final depth
//     let final_depth = get_depth(&client, base_url).await;
//     let final_qty = final_depth.as_ref().map(total_orderbook_quantity).unwrap_or(0);

//     let total_buy = total_buy_qty.load(std::sync::atomic::Ordering::Relaxed);
//     let total_sell = total_sell_qty.load(std::sync::atomic::Ordering::Relaxed);
    
//     // Calculate expected matches
//     // If buy_qty == sell_qty, everything should match, final_qty should be ~0
//     // If buy_qty > sell_qty, remaining = buy_qty - sell_qty (resting buys)
//     let matched_qty = total_buy.min(total_sell);
//     let expected_remaining = total_buy.abs_diff(total_sell);

//     let mut stats = Stats::from_task_stats(all_task_stats);
//     let rps = stats.total as f64 / total_time;

//     println!("\n========== TEST SUMMARY ==========");
//     println!("Total Time:       {:.2}s", total_time);
//     println!("Throughput:       {:.2} req/sec", rps);
//     println!("==================================");

//     println!("\n========== MATCHING VERIFICATION ==========");
//     println!("Total Buy Qty Sent:    {}", total_buy);
//     println!("Total Sell Qty Sent:   {}", total_sell);
//     println!("Expected Matched Qty:  {}", matched_qty);
//     println!("Expected Remaining:    {}", expected_remaining);
//     println!("Actual Remaining:      {}", final_qty);
    
//     // Verify matching worked
//     let matching_worked = (final_qty as i64 - expected_remaining as i64).abs() < 100; // Allow small variance
//     println!("Matching Verified:     {}", if matching_worked { "YES ✓" } else { "NO ✗" });
//     println!("=============================================\n");

//     if let Some(depth) = &final_depth {
//         println!("Final Depth Snapshot:");
//         println!("  Asks: {:?}", &depth.asks[..depth.asks.len().min(5)]);
//         println!("  Bids: {:?}", &depth.bids[..depth.bids.len().min(5)]);
//     }

//     stats.summarize();

//     // Assert matching actually happened
//     assert!(matching_worked, "Matching did not work as expected! Expected ~{} remaining, got {}", expected_remaining, final_qty);
// }