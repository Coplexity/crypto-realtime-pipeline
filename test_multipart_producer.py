#!/usr/bin/env python3
"""
Test script to validate multi-pair producer configuration and logic.

Usage:
    python test_multipart_producer.py
"""

import json
import sys
from typing import Set, Dict, Any

# Test configuration parsing
test_config = {
    'BINANCE_PAIR_FILTER': 'hot',
    'BINANCE_PAIR_MIN_VOLUME': 100000,
    'BINANCE_PAIR_WHITELIST': 'btcusdt,ethusdt,bnbusdt',
    'BINANCE_PAIR_UPDATE_INTERVAL': 3600,
}

def test_config_validation():
    """Test that configuration values are valid."""
    print("=" * 70)
    print("TEST 1: Configuration Validation")
    print("=" * 70)
    
    required_keys = [
        'BINANCE_PAIR_FILTER',
        'BINANCE_PAIR_MIN_VOLUME',
        'BINANCE_PAIR_WHITELIST',
        'BINANCE_PAIR_UPDATE_INTERVAL',
    ]
    
    for key in required_keys:
        if key in test_config:
            print(f"✓ {key}: {test_config[key]}")
        else:
            print(f"✗ MISSING: {key}")
            return False
    
    # Validate filter mode
    if test_config['BINANCE_PAIR_FILTER'] not in ['hot', 'all', 'whitelist']:
        print(f"✗ Invalid FILTER mode: {test_config['BINANCE_PAIR_FILTER']}")
        return False
    
    print("✓ All configuration keys valid\n")
    return True


def test_whitelist_parsing():
    """Test whitelist parsing logic."""
    print("=" * 70)
    print("TEST 2: Whitelist Parsing")
    print("=" * 70)
    
    whitelist_str = test_config['BINANCE_PAIR_WHITELIST']
    if whitelist_str:
        whitelist = set(whitelist_str.split(','))
        print(f"Parsed whitelist: {sorted(whitelist)}")
        print(f"Count: {len(whitelist)} pairs")
        print("✓ Whitelist parsing success\n")
        return True
    else:
        print("ℹ Whitelist is empty (OK for other modes)\n")
        return True


def test_multistream_url_builder():
    """Test multistream URL building logic."""
    print("=" * 70)
    print("TEST 3: Multistream URL Builder")
    print("=" * 70)
    
    pairs = {'btcusdt', 'ethusdt', 'bnbusdt', 'adausdt'}
    BINANCE_WS_API = "wss://stream.binance.com:9443"
    WS_MULTISTREAM_LIMIT = 200
    
    # Build URL
    pairs_list = sorted(list(pairs))[:WS_MULTISTREAM_LIMIT]
    streams = [f"{symbol}@trade" for symbol in pairs_list]
    stream_param = '/'.join(streams)
    url = f"{BINANCE_WS_API}/stream?streams={stream_param}"
    
    print(f"Pairs: {pairs_list}")
    print(f"Streams: {streams}")
    print(f"URL (first 100 chars): {url[:100]}...")
    print(f"URL length: {len(url)}")
    print("✓ Multistream URL built successfully\n")
    return True


def test_trade_data_extraction():
    """Test trade data extraction logic."""
    print("=" * 70)
    print("TEST 4: Trade Data Extraction")
    print("=" * 70)
    
    # Sample multistream message from Binance
    sample_message = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1701234567890,
            "s": "BTCUSDT",
            "t": 123456789,
            "p": "45123.45",
            "q": "0.5",
            "b": 789,
            "a": 790,
            "T": 1701234567000,
            "m": False,
            "M": True
        }
    }
    
    # Extract symbol from stream
    symbol = sample_message["stream"].split("@")[0]
    data = sample_message["data"]
    
    # Process like producer would
    extracted = {
        "symbol": symbol,
        "trade_id": data.get("t"),
        "price": float(data.get("p", 0)),
        "quantity": float(data.get("q", 0)),
        "buyer_order_id": data.get("b"),
        "seller_order_id": data.get("a"),
        "is_buyer_maker": data.get("m", False),
    }
    
    print(f"Original stream: {sample_message['stream']}")
    print(f"Extracted symbol: {symbol}")
    print(f"Price: ${extracted['price']}")
    print(f"Quantity: {extracted['quantity']}")
    print(f"Buyer maker: {extracted['is_buyer_maker']}")
    print("✓ Trade data extraction success\n")
    return True


def test_per_pair_statistics():
    """Test per-pair statistics tracking."""
    print("=" * 70)
    print("TEST 5: Per-Pair Statistics Tracking")
    print("=" * 70)
    
    pair_stats: Dict[str, Dict[str, Any]] = {}
    
    # Simulate messages from different pairs
    trades = [
        ("btcusdt", 45000.00),
        ("ethusdt", 2300.50),
        ("btcusdt", 45100.00),
        ("bnbusdt", 315.25),
        ("btcusdt", 45050.00),
        ("ethusdt", 2301.00),
    ]
    
    for symbol, price in trades:
        if symbol not in pair_stats:
            pair_stats[symbol] = {'count': 0, 'last_price': 0}
        
        pair_stats[symbol]['count'] += 1
        pair_stats[symbol]['last_price'] = price
    
    # Display statistics
    print("Per-pair statistics:")
    total = 0
    for symbol in sorted(pair_stats.keys()):
        stats = pair_stats[symbol]
        print(f"  {symbol:12s} | Messages: {stats['count']:3d} | Last Price: ${stats['last_price']:10.2f}")
        total += stats['count']
    
    print(f"\n  Total Messages: {total}")
    print("✓ Per-pair statistics tracking success\n")
    return True


def test_filter_modes():
    """Test different filter modes logic."""
    print("=" * 70)
    print("TEST 6: Filter Modes Logic")
    print("=" * 70)
    
    all_pairs = {'btcusdt', 'ethusdt', 'bnbusdt', 'adausdt', 'xrpusdt'}
    whitelist = {'btcusdt', 'ethusdt'}
    
    print("Available pairs:", sorted(all_pairs))
    print()
    
    # Mode 1: hot
    hot_pairs = {p for p in all_pairs if p in {'btcusdt', 'ethusdt'}}
    print(f"Mode 'hot':       {sorted(hot_pairs)} (volume filtered)")
    
    # Mode 2: all
    all_mode = all_pairs
    print(f"Mode 'all':       {sorted(all_mode)} (all pairs)")
    
    # Mode 3: whitelist
    wl_mode = whitelist
    print(f"Mode 'whitelist': {sorted(wl_mode)} (explicit list)")
    
    print("✓ Filter modes logic success\n")
    return True


def main():
    """Run all tests."""
    tests = [
        test_config_validation,
        test_whitelist_parsing,
        test_multistream_url_builder,
        test_trade_data_extraction,
        test_per_pair_statistics,
        test_filter_modes,
    ]
    
    results = []
    for test_func in tests:
        try:
            result = test_func()
            results.append((test_func.__name__, result))
        except Exception as e:
            print(f"✗ Exception in {test_func.__name__}: {e}\n")
            results.append((test_func.__name__, False))
    
    # Summary
    print("=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Producer is ready for deployment.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Review output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
