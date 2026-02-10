#!/bin/bash
# Run comprehensive graphLookup benchmark and save results

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="benchmark_results_${TIMESTAMP}.txt"
JSON_FILE="benchmark_results_${TIMESTAMP}.json"

echo "Starting MongoDB graphLookup Benchmark..."
echo "=========================================="
echo "Configuration:"
echo "  - Start points: 10 (randomly selected)"
echo "  - maxDepth values: [0, 1, 3, 5, 10, 30, 50]"
echo "  - Iterations per query: 5 (median latency)"
echo ""
echo "This may take several minutes..."
echo ""

# Run benchmark and save output
mongosh ldbc graphLookup_benchmark.js | tee "$OUTPUT_FILE"

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
