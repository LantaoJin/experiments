// MongoDB $graphLookup Benchmark - Comprehensive Methodology
// Tests: maxDepth gradient and multiple start values

print(`Using database: ${db.getName()}`);

// Configuration
const N_START_POINTS = 10;
const MAX_DEPTHS = [0, 1, 3, 5];
const ITERATIONS = 5;
const MULTIPLE_START_DEPTHS = 3;
const ENABLE_MULTIPLE_START_TEST = typeof DISABLE_MULTIPLE_START === 'undefined' ? true : !DISABLE_MULTIPLE_START;

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

// Step 1: Get starting points (user-specified or random)
print("=== Step 1: Selecting Starting Points ===");

let startPoints = [];

// Check if user provided start points via --eval
if (typeof USER_START_POINTS !== 'undefined' && USER_START_POINTS.length > 0) {
    print("Using user-specified start points:");
    startPoints = USER_START_POINTS;
    
    // Debug: test direct query
    print(`Debug: Testing direct query for ID ${startPoints[0]}`);
    const testResult = db.person.findOne({ id: startPoints[0] });
    print(`Debug: Direct query result: ${testResult ? 'FOUND' : 'NOT FOUND'}`);
    if (testResult) {
        print(`Debug: Found person: ${testResult.firstName} ${testResult.lastName}`);
    }
    
    // Get person details for the specified IDs
    const personDetails = db.person.find(
        { id: { $in: startPoints } },
        { id: 1, firstName: 1, lastName: 1 }
    ).toArray();
    
    personDetails.forEach(p => {
        results.metadata.startPoints.push({
            id: p.id,
            name: `${p.firstName} ${p.lastName}`
        });
        print(`  Using: ${p.id} (${p.firstName} ${p.lastName})`);
    });
    
    // Warn if some IDs not found
    if (personDetails.length < startPoints.length) {
        print(`  Warning: ${startPoints.length - personDetails.length} ID(s) not found in database`);
    }
} else {
    print("Randomly selecting start points:");
    
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
}

// Step 2: Single Start Value Tests with maxDepth gradient
print("\n=== Step 2: Single Start Value Tests (maxDepth gradient) ===");

startPoints.forEach((startId, idx) => {
    print(`\nTesting start point ${idx + 1}/${N_START_POINTS}: ${startId}`);
    
    MAX_DEPTHS.forEach(depth => {
        print(`  maxDepth=${depth}...`);
        
        const result = executeWithMedian(() => {
            return db.person.aggregate([
                { $match: { id: startId } },
                { $graphLookup: {
                    from: "person_knows_person",
                    startWith: "$id",
                    connectFromField: "person2_id",
                    connectToField: "person1_id",
                    as: "connections",
                    maxDepth: depth
                }},
                { $project: { connectionCount: { $size: "$connections" } } }
            ]).toArray();
        });
        
        // Count edges and nodes separately (not timed)
        const edgeDocs = db.person.aggregate([
            { $match: { id: startId } },
            { $graphLookup: {
                from: "person_knows_person",
                startWith: "$id",
                connectFromField: "person2_id",
                connectToField: "person1_id",
                as: "connections",
                maxDepth: depth
            }},
            { $unwind: "$connections" }
        ]).toArray();
        
        const nodeDocs = db.person.aggregate([
            { $match: { id: startId } },
            { $graphLookup: {
                from: "person_knows_person",
                startWith: "$id",
                connectFromField: "person2_id",
                connectToField: "person1_id",
                as: "connections",
                maxDepth: depth
            }},
            { $unwind: "$connections" },
            { $group: { _id: "$connections.person2_id" } }
        ]).toArray();
        
        result.edgeCount = edgeDocs.length;
        result.nodeCount = nodeDocs.length;
        
        results.singleStart.push({
            startId: startId,
            maxDepth: depth,
            medianLatency: result.medianLatency,
            latencies: result.allLatencies,
            edgeCount: result.edgeCount,
            nodeCount: result.nodeCount
        });
        
        print(`    Latency: ${result.medianLatency}ms (${result.edgeCount} edges, ${result.nodeCount} nodes)`);
        results.metadata.totalTests += 1;
    });
});

// Step 3: Multiple Start Value Tests
if (ENABLE_MULTIPLE_START_TEST) {
    print("\n=== Step 3: Multiple Start Value Test (10 starts, maxDepth=3) ===");

    print(`\nTesting with ${N_START_POINTS} start values...`);

    const result = executeWithMedian(() => {
        return db.person.aggregate([
            { $match: { id: { $in: startPoints } } },
            { $graphLookup: {
                from: "person_knows_person",
                startWith: "$id",
                connectFromField: "person2_id",
                connectToField: "person1_id",
                as: "connections",
                maxDepth: MULTIPLE_START_DEPTHS
            }},
            { $project: { connectionCount: { $size: "$connections" } } }
        ]).toArray();
    });

    // Count edges and nodes separately (not timed)
    const edgeDocs = db.person.aggregate([
        { $match: { id: { $in: startPoints } } },
        { $graphLookup: {
            from: "person_knows_person",
            startWith: "$id",
            connectFromField: "person2_id",
            connectToField: "person1_id",
            as: "connections",
            maxDepth: MULTIPLE_START_DEPTHS
        }},
        { $unwind: "$connections" }
    ]).toArray();

    const nodeDocs = db.person.aggregate([
        { $match: { id: { $in: startPoints } } },
        { $graphLookup: {
            from: "person_knows_person",
            startWith: "$id",
            connectFromField: "person2_id",
            connectToField: "person1_id",
            as: "connections",
            maxDepth: MULTIPLE_START_DEPTHS
        }},
        { $unwind: "$connections" },
        { $group: { _id: "$connections.person2_id" } }
    ]).toArray();

    result.edgeCount = edgeDocs.length;
    result.nodeCount = nodeDocs.length;

    results.multipleStart.push({
        numStartValues: N_START_POINTS,
        startIds: startPoints,
        maxDepth: MULTIPLE_START_DEPTHS,
        medianLatency: result.medianLatency,
        latencies: result.allLatencies,
        edgeCount: result.edgeCount,
        nodeCount: result.nodeCount
    });

    print(`  Latency: ${result.medianLatency}ms (${result.edgeCount} edges, ${result.nodeCount} nodes)`);
    results.metadata.totalTests += 1;
} else {
    print("\n=== Step 3: Multiple Start Value Test (SKIPPED) ===");
}

// Step 4: Statistics
print("\n\n=== STATISTICS SUMMARY ===");

// Single Start Statistics
print("\n--- Single Start Value Tests ---");
MAX_DEPTHS.forEach(depth => {
    const depthResults = results.singleStart.filter(r => r.maxDepth === depth);
    
    const latencies = depthResults.map(r => r.medianLatency);
    const edgeCounts = depthResults.map(r => r.edgeCount);
    const nodeCounts = depthResults.map(r => r.nodeCount);
    
    const avgLatency = latencies.reduce((a,b)=>a+b,0) / latencies.length;
    const minLatency = Math.min(...latencies);
    const maxLatency = Math.max(...latencies);
    const avgEdges = edgeCounts.reduce((a,b)=>a+b,0) / edgeCounts.length;
    const avgNodes = nodeCounts.reduce((a,b)=>a+b,0) / nodeCounts.length;
    
    print(`\nmaxDepth=${depth}:`);
    print(`  Latency - Avg: ${avgLatency.toFixed(2)}ms, Min: ${minLatency}ms, Max: ${maxLatency}ms`);
    print(`  Avg Edges: ${avgEdges.toFixed(0)}, Avg Nodes: ${avgNodes.toFixed(0)}`);
});

// Multiple Start Statistics
if (ENABLE_MULTIPLE_START_TEST && results.multipleStart.length > 0) {
    print("\n--- Multiple Start Value Test (maxDepth=3) ---");
    const r = results.multipleStart[0];
    print(`\n${r.numStartValues} start values:`);
    print(`  Latency: ${r.medianLatency}ms`);
    print(`  Edges: ${r.edgeCount}, Nodes: ${r.nodeCount}`);
}

// Scalability Analysis
print("\n--- Scalability Analysis ---");
print("\nLatency vs maxDepth:");
MAX_DEPTHS.forEach(depth => {
    const depthResults = results.singleStart.filter(r => r.maxDepth === depth);
    const avgLatency = depthResults.reduce((sum, r) => sum + r.medianLatency, 0) / depthResults.length;
    print(`  maxDepth ${depth.toString().padStart(2)}: ${avgLatency.toFixed(2).padStart(10)}ms`);
});

print("\nLatency vs Number of Start Values (maxDepth=3):");
if (ENABLE_MULTIPLE_START_TEST && results.multipleStart.length > 0) {
    const multiResult = results.multipleStart[0];
    print(`  ${multiResult.numStartValues.toString().padStart(2)} starts: ${multiResult.medianLatency.toFixed(2).padStart(10)}ms`);
} else {
    print("  (Test disabled)");
}

print(`\nTotal tests executed: ${results.metadata.totalTests}`);
print(`Total queries run: ${results.metadata.totalTests * ITERATIONS}`);

// Save results to JSON
print("\n=== RAW RESULTS (JSON) ===");
print(JSON.stringify(results, null, 2));
