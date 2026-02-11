#!/bin/bash
# Run comprehensive graphLookup benchmark and save results

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="benchmark_results_${TIMESTAMP}.txt"
JSON_FILE="benchmark_results_${TIMESTAMP}.json"

echo "Starting MongoDB graphLookup Benchmark..."
echo "=========================================="
echo "Configuration:"
echo "  - Start points: 10 (randomly selected)"
echo "  - maxDepth values: [0, 1, 3, 5]"
echo "  - Iterations per query: 5 (median latency)"
echo ""
echo "This may take several minutes..."
echo ""

# Run benchmark with random nodes and save output
# mongosh ldbc graphLookup_benchmark.js | tee "$OUTPUT_FILE"
# Run benchmark with specific nodes and save output
# mongosh ldbc --eval "var USER_START_POINTS = [2199023262543,17592186053137,4398046513018,6597069777240,30786325585162,24189255819727,2199023259756,19791209302403,8796093029267,2783]" graphLookup_benchmark.js | tee "$OUTPUT_FILE"
# Run benchmark with specific nodes on ldbc30 database and save output
mongosh ldbc30 --eval "var USER_START_POINTS = [8796093094586,6597069787890,21990232663185,17592186091826,26388279239178,2199023409152,26388279074356,4398046634345,77464,32985349004135]" graphLookup_benchmark.js
# Extract JSON from output
echo ""
echo "Results saved to: $OUTPUT_FILE"

# Extract and save JSON separately
sed -n '/=== RAW RESULTS (JSON) ===/,$ p' "$OUTPUT_FILE" | tail -n +2 > "$JSON_FILE"

if [ -s "$JSON_FILE" ]; then
    echo "JSON results saved to: $JSON_FILE"
else
    rm "$JSON_FILE"
fi

echo ""
echo "Benchmark complete!"
