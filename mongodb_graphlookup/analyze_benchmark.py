#!/usr/bin/env python3
"""
Analyze MongoDB graphLookup benchmark results
Generates statistics and visualizations
"""

import json
import sys
from statistics import mean, median, stdev
from pathlib import Path

def analyze_results(data):
    """Analyze benchmark results and print statistics"""
    
    print("\n" + "=" * 100)
    print("BENCHMARK SUMMARY")
    print("=" * 100)
    
    # Metadata
    print(f"\nTest Configuration:")
    print(f"  Timestamp: {data['metadata']['timestamp']}")
    print(f"  Total tests: {data['metadata']['totalTests']}")
    print(f"  Start points: {len(data['metadata']['startPoints'])}")
    
    # Summary Table
    print("\n" + "=" * 115)
    print(f"{'maxDepth':<10} {'Avg Latency (ms)':<18} {'Median (ms)':<15} {'Min (ms)':<12} {'Max (ms)':<12} {'Avg Edges':<12} {'Avg Nodes':<12}")
    print("-" * 115)
    
    max_depths = sorted(set(r['maxDepth'] for r in data['singleStart']))
    
    for depth in max_depths:
        depth_results = [r for r in data['singleStart'] if r['maxDepth'] == depth]
        
        latencies = [r['medianLatency'] for r in depth_results]
        edge_counts = [r['edgeCount'] for r in depth_results]
        node_counts = [r['nodeCount'] for r in depth_results]
        
        avg_lat = mean(latencies)
        med_lat = median(latencies)
        min_lat = min(latencies)
        max_lat = max(latencies)
        avg_edges = mean(edge_counts)
        avg_nodes = mean(node_counts)
        
        print(f"{depth:<10} {avg_lat:<18.2f} {med_lat:<15.2f} {min_lat:<12.2f} {max_lat:<12.2f} {avg_edges:<12.0f} {avg_nodes:<12.0f}")
    
    # Detailed Single Start Analysis
    print("\n" + "=" * 115)
    print("DETAILED SINGLE START VALUE ANALYSIS")
    print("=" * 115)
    
    for depth in max_depths:
        depth_results = [r for r in data['singleStart'] if r['maxDepth'] == depth]
        
        latencies = [r['medianLatency'] for r in depth_results]
        edge_counts = [r['edgeCount'] for r in depth_results]
        node_counts = [r['nodeCount'] for r in depth_results]
        
        print(f"\nmaxDepth = {depth}")
        print(f"  Latency:")
        print(f"    Mean: {mean(latencies):.2f}ms")
        print(f"    Median: {median(latencies):.2f}ms")
        if len(latencies) > 1:
            print(f"    Std Dev: {stdev(latencies):.2f}ms")
        print(f"    Min: {min(latencies)}ms, Max: {max(latencies)}ms")
        print(f"  Edges: {mean(edge_counts):.0f} (avg)")
        print(f"  Nodes: {mean(node_counts):.0f} (avg)")
    
    # Multiple Start Analysis
    print("\n" + "=" * 115)
    print("MULTIPLE START VALUE TEST (maxDepth=3)")
    print("=" * 115)
    
    r = data['multipleStart'][0]
    print(f"\n{r['numStartValues']} start values:")
    print(f"  Latency: {r['medianLatency']}ms")
    print(f"  Edges: {r['edgeCount']}, Nodes: {r['nodeCount']}")
    
    # Scalability Analysis
    print("\n" + "=" * 115)
    print("SCALABILITY ANALYSIS")
    print("=" * 115)
    
    print("\nLatency vs maxDepth:")
    for depth in max_depths:
        depth_results = [r for r in data['singleStart'] if r['maxDepth'] == depth]
        avg_latency = mean([r['medianLatency'] for r in depth_results])
        print(f"  maxDepth {depth:2d}: {avg_latency:8.2f}ms")
    
    print("\nLatency vs Number of Start Values (maxDepth=3):")
    r = data['multipleStart'][0]
    print(f"  {r['numStartValues']:2d} starts: {r['medianLatency']:8.2f}ms")

def main():
    if len(sys.argv) < 2:
        # Find most recent JSON file
        json_files = sorted(Path('.').glob('benchmark_results_*.json'))
        if not json_files:
            print("Error: No benchmark results found")
            print("Usage: python3 analyze_benchmark.py <results.json>")
            sys.exit(1)
        json_file = json_files[-1]
        print(f"Using most recent results: {json_file}\n")
    else:
        json_file = sys.argv[1]
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    analyze_results(data)

if __name__ == '__main__':
    main()
