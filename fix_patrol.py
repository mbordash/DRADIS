import re

with open('src/squadron/patrol_impl.rs', 'r') as f:
    content = f.read()

# Find the section with the StrategyContext struct literal that has .await inside it
# We need to hoist the three await calls before the struct literal

old_section = (
    '                    let dyn_cfg = config_rx.borrow().clone();\n'
    '\n'
    '                    let ctx = StrategyContext {\n'
    '                        market: hourly_market_config_for_ctx.clone(),\n'
    '                        snapshot: MarketSnapshot {\n'
    '                            yes_bid: hourly_yb, yes_bid_depth: hourly_ybd, yes_ask: hourly_ya, yes_ask_depth: hourly_yad,\n'
    '                            no_bid: hourly_nb, no_bid_depth: hourly_nbd, no_ask: hourly_na, no_ask_depth: hourly_nad,\n'
    '                            oracle_price: *oracle_rx.borrow(),\n'
    '                            velocity: velocity_rx.borrow().0,\n'
    '                            velocity_1s: velocity_rx.borrow().1,\n'
    '                            acceleration: velocity_rx.borrow().2,\n'
    '                            funding_rate: *funding_rx.borrow(),\n'
    '                            oracle_drift_60m: drift_rx.borrow().0,\n'
    '                            oracle_drift_10m: drift_rx.borrow().1,\n'
    '                            secs_to_expiry: hourly_market_close_time\n'
    '                                .map(|t| (t - Utc::now()).num_seconds())\n'
    '                                .unwrap_or(0),\n'
    '                            timestamp: hourly_snap_ts,\n'
    '                        },\n'
    '                        positions: Arc::clone(&positions),\n'
    '                        session_pnl: *total_pnl.lock().await,\n'
    '                        starting_collateral: *starting_collateral_store.lock().await,\n'
    '                        available_collateral: *live_collateral.lock().await,\n'
)

new_section = (
    '                    let dyn_cfg = config_rx.borrow().clone();\n'
    '\n'
    '                    // Hoist mutex-await calls OUT of the struct literal so that\n'
    '                    // borrow() Ref guards (oracle_rx, velocity_rx, etc.) in the\n'
    '                    // snapshot fields are NOT alive at any .await point.\n'
    '                    // Without this the future is non-Send and tokio::spawn rejects\n'
    '                    // it (Phase 3f-6: concurrent multi-asset spawning).\n'
    '                    let ctx_session_pnl          = *total_pnl.lock().await;\n'
    '                    let ctx_starting_collateral  = *starting_collateral_store.lock().await;\n'
    '                    let ctx_available_collateral = *live_collateral.lock().await;\n'
    '\n'
    '                    let ctx = StrategyContext {\n'
    '                        market: hourly_market_config_for_ctx.clone(),\n'
    '                        snapshot: MarketSnapshot {\n'
    '                            yes_bid: hourly_yb, yes_bid_depth: hourly_ybd, yes_ask: hourly_ya, yes_ask_depth: hourly_yad,\n'
    '                            no_bid: hourly_nb, no_bid_depth: hourly_nbd, no_ask: hourly_na, no_ask_depth: hourly_nad,\n'
    '                            oracle_price: *oracle_rx.borrow(),\n'
    '                            velocity: velocity_rx.borrow().0,\n'
    '                            velocity_1s: velocity_rx.borrow().1,\n'
    '                            acceleration: velocity_rx.borrow().2,\n'
    '                            funding_rate: *funding_rx.borrow(),\n'
    '                            oracle_drift_60m: drift_rx.borrow().0,\n'
    '                            oracle_drift_10m: drift_rx.borrow().1,\n'
    '                            secs_to_expiry: hourly_market_close_time\n'
    '                                .map(|t| (t - Utc::now()).num_seconds())\n'
    '                                .unwrap_or(0),\n'
    '                            timestamp: hourly_snap_ts,\n'
    '                        },\n'
    '                        positions: Arc::clone(&positions),\n'
    '                        session_pnl:          ctx_session_pnl,\n'
    '                        starting_collateral:  ctx_starting_collateral,\n'
    '                        available_collateral: ctx_available_collateral,\n'
)

if old_section in content:
    content = content.replace(old_section, new_section, 1)
    with open('src/squadron/patrol_impl.rs', 'w') as f:
        f.write(content)
    print('SUCCESS: replaced section')
else:
    print('NOT FOUND')
    idx = content.find('session_pnl: *total_pnl.lock().await')
    if idx >= 0:
        print(f'Found session_pnl at index {idx}')
        print(repr(content[max(0,idx-200):idx+100]))
    else:
        print('session_pnl not found either')

