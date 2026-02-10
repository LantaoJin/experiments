// Quick test version - 3 start points, 3 iterations
const db = db.getSiblingDB('ldbc');

const N_START_POINTS = 3;
const MAX_DEPTHS = [0, 1, 3];
const ITERATIONS = 3;

function median(arr) {
    const sorted = arr.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function executeWithMedian(queryFn, iterations = ITERATIONS) {
    const latencies = [];
    let resultCount = 0;
    
    for (let i = 0; i < iterations; i++) {
        const start = new Date();
        const result = queryFn();
        const end = new Date();
        latencies.push(end - start);
        resultCount = result.length;
    }
    
    return {
        medianLatency: median(latencies),
        allLatencies: latencies,
        resultCount: resultCount
    };
}

print("=== Quick Test ===");
const startPoints = db.person.aggregate([
    { $sample: { size: N_START_POINTS } }
]).toArray().map(p => p.id);

print(`Start points: ${startPoints.join(', ')}`);

startPoints.forEach((startId, idx) => {
    print(`\nStart point ${idx + 1}: ${startId}`);
    
    MAX_DEPTHS.forEach(depth => {
        const result = executeWithMedian(() => {
            return db.person_knows_person.aggregate([
                { $match: { person1_id: startId } },
                { $graphLookup: {
                    from: "person_knows_person",
                    startWith: "$person2_id",
                    connectFromField: "person2_id",
                    connectToField: "person1_id",
                    as: "connections",
                    maxDepth: depth
                }},
                { $unwind: "$connections" },
                { $group: { _id: "$connections.person2_id" } }
            ]).toArray();
        });
        
        print(`  maxDepth=${depth}: ${result.medianLatency}ms (${result.resultCount} results)`);
    });
});

print("\n✓ Test completed successfully!");
