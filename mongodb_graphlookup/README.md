# MongoDB $graphLookup Benchmark Suite

## ✅ Verified Working Scripts

All scripts have been tested and are fully functional.

## Files

1. **graphLookup_benchmark.js** - Full benchmark (10 start points, 7 depths, 5 iterations)
2. **test_benchmark.js** - Quick test (3 start points, 3 depths, 3 iterations)
3. **run_full_benchmark.sh** - Runner script that saves results
4. **analyze_benchmark.py** - Statistical analysis tool

## Quick Start

### Run Quick Test (1-2 minutes)
```bash
mongosh ldbc test_benchmark.js
```

### Run Full Benchmark (10-20 minutes)
```bash
./run_full_benchmark.sh
```

Results saved to: `benchmark_results_<timestamp>.txt` and `.json`

### Run with Custom Start Points
```bash
mongosh ldbc --eval "var USER_START_POINTS = [123, 456, 789, 1011, 1213]" graphLookup_benchmark.js
```

Replace the numbers with your desired person IDs. The script will use these instead of random selection.

### Disable Multiple Start Test
```bash
mongosh ldbc --eval "var DISABLE_MULTIPLE_START = true" graphLookup_benchmark.js
```

This will skip the multiple start value test and only run single start tests.

### Analyze Results
```bash
python3 analyze_benchmark.py
```

## Methodology

Following your requirements:

1. **N=10 random starting points** - Randomly selected person IDs
2. **maxDepth gradient** - Tests [0, 1, 3, 5]
3. **5 iterations per query** - Median latency recorded
4. **Multiple start values** - Tests 10 start values with maxDepth=3
5. **Statistics** - Comprehensive analysis of all results

## Test Coverage

### Single Start Tests
- 10 start points × 4 maxDepth values = 40 configurations
- 40 × 5 iterations = 200 queries

### Multiple Start Test  
- 1 configuration (10 start values, maxDepth=3)
- 1 × 5 iterations = 5 queries

### Total
- **41 test configurations**
- **205 total queries**

## Sample Output

```
maxDepth=3:
  Latency - Avg: 1250.50ms, Min: 270ms, Max: 3134ms
  Avg Results: 2500 documents

3 start value(s):
  Latency: 3500ms
  Results: 7500 documents
```

## Performance Tips

Create indexes for better performance:
```bash
mongosh ldbc --eval "
  db.person_knows_person.createIndex({ person1_id: 1 });
  db.person_knows_person.createIndex({ person2_id: 1 });
"
```

## Notes

- PIT (Point-in-Time) removed - MongoDB-specific implementation
- Benchmark is read-only (non-destructive)
- Results vary based on graph structure
- Higher maxDepth values take longer but may not find more results
