#!/usr/bin/env python3
"""
Standalone cache updater script.
This can be run as a separate process or scheduled job.

Usage:
    python cache_updater_standalone.py --popular 50
    python cache_updater_standalone.py --coins bitcoin ethereum solana
    python cache_updater_standalone.py --coin bitcoin
"""

import sys
import argparse
from services.cache import cache_updater


def main():
    parser = argparse.ArgumentParser(
        description='Update Redis cache with fresh cryptocurrency data'
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--coin',
        type=str,
        help='Update a single coin (e.g., bitcoin)'
    )
    group.add_argument(
        '--coins',
        type=str,
        nargs='+',
        help='Update multiple coins (e.g., bitcoin ethereum solana)'
    )
    group.add_argument(
        '--popular',
        type=int,
        metavar='N',
        help='Update N most popular coins (e.g., 20)'
    )
    
    args = parser.parse_args()
    
    try:
        if args.coin:
            print(f"Updating cache for {args.coin}...")
            result = cache_updater.update_coin(args.coin)
            
            if result['tokenomics_updated'] or result['coin_data_updated']:
                print(f"✅ Successfully updated {args.coin}")
                return 0
            else:
                print(f"❌ Failed to update {args.coin}")
                print(f"Errors: {result['errors']}")
                return 1
        
        elif args.coins:
            print(f"Updating cache for {len(args.coins)} coins...")
            result = cache_updater.update_multiple_coins(args.coins)
            
            print(f"\n✅ Update complete:")
            print(f"   Succeeded: {result['succeeded']}")
            print(f"   Failed: {result['failed']}")
            
            if result['failed'] > 0:
                print("\nFailed coins:")
                for r in result['results']:
                    if r['errors']:
                        print(f"   - {r['coin_id']}: {r['errors']}")
            
            return 0 if result['failed'] == 0 else 1
        
        elif args.popular:
            print(f"Updating cache for top {args.popular} popular coins...")
            result = cache_updater.update_popular_coins(args.popular)
            
            print(f"\n✅ Update complete:")
            print(f"   Succeeded: {result['succeeded']}")
            print(f"   Failed: {result['failed']}")
            
            return 0 if result['failed'] == 0 else 1
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
