// MongoDB $graphLookup Benchmark - Comprehensive Methodology
// Tests: maxDepth gradient and multiple start values

const db = db.getSiblingDB('ldbc');

// Configuration
const N_START_POINTS = 10;
const MAX_DEPTHS = [0, 1, 3, 5, 10, 30, 50];
const ITERATIONS = 5;
const MULTIPLE_START_DEPTHS = 3;

// Results storage
const results = {
    singleStart: [],
    multipleStart: [],
    metadata: {
        startPoints: [],
        timestamp: new Date(),
        totalTests: 0
    }
};

// Helper: Get median from array
function median(arr) {
    const sorted = arr.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

// Helper: Execute query multiple times and get median latency
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

// Step 1: Randomly select N starting points
print("=== Step 1: Selecting Random Starting Points ===");
const totalPersons = db.person.countDocuments();
const startPoints = [];

// Get random person IDs
const randomPersons = db.person.aggregate([
    { $sample: { size: N_START_POINTS } },
    { $project: { id: 1, firstName: 1, lastName: 1 } }
]).toArray();

randomPersons.forEach(p => {
    startPoints.push(p.id);
    results.metadata.startPoints.push({
        id: p.id,
        name: `${p.firstName} ${p.lastName}`
    });
    print(`  Selected: ${p.id} (${p.firstName} ${p.lastName})`);
});

// Step 2: Single Start Value Tests with maxDepth gradient
print("\n=== Step 2: Single Start Value Tests (maxDepth gradient) ===");

startPoints.forEach((startId, idx) => {
    print(`\nTesting start point ${idx + 1}/${N_START_POINTS}: ${startId}`);
    
    MAX_DEPTHS.forEach(depth => {
        print(`  maxDepth=${depth}...`);
        
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
        
        results.singleStart.push({
            startId: startId,
            maxDepth: depth,
            medianLatency: result.medianLatency,
            latencies: result.allLatencies,
            resultCount: result.resultCount
        });
        
        print(`    Latency: ${result.medianLatency}ms (${result.resultCount} results)`);
        results.metadata.totalTests += 1;
    });
});

// Step 3: Multiple Start Value Tests
print("\n=== Step 3: Multiple Start Value Tests (static maxDepth=3) ===");

for (let numStarts = 1; numStarts <= N_START_POINTS; numStarts++) {
    const currentStarts = startPoints.slice(0, numStarts);
    print(`\nTesting with ${numStarts} start value(s)...`);
    
    const result = executeWithMedian(() => {
        return db.person_knows_person.aggregate([
            { $match: { person1_id: { $in: currentStarts } } },
            { $graphLookup: {
                from: "person_knows_person",
                startWith: "$person2_id",
                connectFromField: "person2_id",
                connectToField: "person1_id",
                as: "connections",
                maxDepth: MULTIPLE_START_DEPTHS
            }},
            { $unwind: "$connections" },
            { $group: { _id: "$connections.person2_id" } }
        ]).toArray();
    });
    
    results.multipleStart.push({
        numStartValues: numStarts,
        startIds: currentStarts,
        maxDepth: MULTIPLE_START_DEPTHS,
        medianLatency: result.medianLatency,
        latencies: result.allLatencies,
        resultCount: result.resultCount
    });
    
    print(`  Latency: ${result.medianLatency}ms (${result.resultCount} results)`);
    results.metadata.totalTests += 1;
}

// Step 4: Statistics
print("\n\n=== STATISTICS SUMMARY ===");

// Single Start Statistics
print("\n--- Single Start Value Tests ---");
MAX_DEPTHS.forEach(depth => {
    const depthResults = results.singleStart.filter(r => r.maxDepth === depth);
    
    const latencies = depthResults.map(r => r.medianLatency);
    const resultCounts = depthResults.map(r => r.resultCount);
    
    const avgLatency = latencies.reduce((a,b)=>a+b,0) / latencies.length;
    const minLatency = Math.min(...latencies);
    const maxLatency = Math.max(...latencies);
    const avgResults = resultCounts.reduce((a,b)=>a+b,0) / resultCounts.length;
    
    print(`\nmaxDepth=${depth}:`);
    print(`  Latency - Avg: ${avgLatency.toFixed(2)}ms, Min: ${minLatency}ms, Max: ${maxLatency}ms`);
    print(`  Avg Results: ${avgResults.toFixed(0)} documents`);
});

// Multiple Start Statistics
print("\n--- Multiple Start Value Tests (maxDepth=3) ---");
results.multipleStart.forEach(r => {
    print(`\n${r.numStartValues} start value(s):`);
    print(`  Latency: ${r.medianLatency}ms`);
    print(`  Results: ${r.resultCount} documents`);
});

// Scalability Analysis
print("\n--- Scalability Analysis ---");
print("\nLatency vs maxDepth:");
MAX_DEPTHS.forEach(depth => {
    const depthResults = results.singleStart.filter(r => r.maxDepth === depth);
    const avgLatency = depthResults.reduce((sum, r) => sum + r.medianLatency, 0) / depthResults.length;
    print(`  maxDepth ${depth.toString().padStart(2)}: ${avgLatency.toFixed(2).padStart(10)}ms`);
});

print("\nLatency vs Number of Start Values (maxDepth=3):");
results.multipleStart.forEach(r => {
    print(`  ${r.numStartValues.toString().padStart(2)} start(s): ${r.medianLatency.toFixed(2).padStart(10)}ms`);
});

print(`\nTotal tests executed: ${results.metadata.totalTests}`);
print(`Total queries run: ${results.metadata.totalTests * ITERATIONS}`);

// Save results to JSON
print("\n=== RAW RESULTS (JSON) ===");
print(JSON.stringify(results, null, 2));
