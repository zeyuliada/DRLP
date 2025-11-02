/*
 * Copyright (c) 2025 ADA Laboratory
 * School of Computer Science and Technology, Soochow University, China
 * 
 * This work is based on the original DRLPartitioner developed by:
 * Data Intensive Applications and Systems Laboratory (DIAS)
 * Ecole Polytechnique Federale de Lausanne (EPFL), Switzerland
 * 
 * Original source code available at: https://github.com/ezapridou/Dalton
 * 
 * We gratefully acknowledge the foundational work of the DIAS Lab team.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package partitioning.dqn;

import record.Record;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import partitioning.Partitioner;
import partitioning.state.State;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import partitioning.dqn.containers.NormalizeObservation;
import partitioning.dqn.containers.ScaleReward;
import java.util.concurrent.CompletableFuture;

/**
 * Dual-threaded DRL (Deep Reinforcement Learning) Partitioner with target networks
 * Uses deep reinforcement learning for adaptive partitioning, with separate networks for training and inference in parallel
 * Added dynamic partition constraint mechanism based on key frequency
 */
public class DRLPartitioner extends Partitioner {
    private final State state;
    private final int windowSize; // Window size
    private final transient MultiLayerNetwork mainNetwork;     // Main network for training
    private final transient MultiLayerNetwork targetNetwork1;  // Target network 1
    private final transient MultiLayerNetwork targetNetwork2;  // Target network 2
    private volatile int activeTargetNetwork = 1; // Currently active target network (1 or 2)
    private final AtomicBoolean isUpdatingTargetNetwork = new AtomicBoolean(false); // Whether target network is being updated
    
    private final int stateSize;
    private final int actionSize;
    private static final double GAMMA = 0.99;
    private static final double EPSILON_START = 1.0;
    private static final double EPSILON_END = 0.05;
    private static final double EPSILON_DECAY = 0.9999;
    private double epsilon;
    
    // Scheduled target network update parameters
    private static final int TARGET_UPDATE_FREQUENCY = 256; // Update target network after processing 256 samples (changed from 100 to 1000)
    private int sampleCounter = 0;
    
    // Thread-related
    private transient ExecutorService trainingExecutor;
    private transient ExecutorService inferenceExecutor;
    private final AtomicBoolean isTraining = new AtomicBoolean(false);
    private final AtomicBoolean isInferencing = new AtomicBoolean(false);
    private transient boolean executorsInitialized = false;
    private final ReentrantReadWriteLock networkLock = new ReentrantReadWriteLock();
    
    // Record counter for triggering training
    private final AtomicLong recordCounter = new AtomicLong(0);
    
    // Record buffer for batch training
    private final transient List<TrainingRecord> recordBuffer = new ArrayList<>();
    private final transient Object recordBufferLock = new Object();
    
    // Routing table: cache key-to-worker mapping
    private final Map<Integer, Integer> routingTable = new ConcurrentHashMap<>();
    private final AtomicBoolean routingTableValid = new AtomicBoolean(true);
    
    // Routing table statistics
    private final AtomicLong routingTableHits = new AtomicLong(0);
    private final AtomicLong routingTableMisses = new AtomicLong(0);
    
    // LRU cache size limit
    private static final int MAX_ROUTING_TABLE_SIZE = 100;
    
    // Normalization components
    private final transient NormalizeObservation obsNormalizer;
    private final transient ScaleReward rewardScaler;
    private static final double EPSILON = 1e-8; // Small constant for normalization
    
    // Batch training related
    private static final int BATCH_SIZE = 128; // Batch size for training, also used as buffer upper limit
    
    // ===== Dynamic partition constraint mechanism based on State key partition info =====
    
    // Partition constraint parameters
    private static final int MIN_PARTITIONS_FOR_CONSTRAINT = 2;  // Minimum 2 partitions required to enable constraint
    private static final boolean ENABLE_PARTITION_CONSTRAINT = true; // Whether to enable partition constraint
    
    // ===== Frequency-based dynamic partition limit control parameters =====
    private static final boolean ENABLE_FREQUENCY_BASED_LIMIT = true;  // Enable frequency-based dynamic limit
    private static final int MIN_PARTITIONS_PER_KEY = 1;              // Minimum partitions per key
    private static final int MAX_PARTITIONS_PER_KEY = 16;              // Maximum partitions per key (absolute upper limit)
    private static final double FREQUENCY_LOG_BASE = 1.5;             // Logarithm base
    // private static final double FREQUENCY_SCALE_FACTOR = 1.0;      // Frequency scale factor (removed)
    
    // Frequency statistics and cache
    private final Map<Integer, Integer> keyFrequencyCache = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> keyPartitionLimitCache = new ConcurrentHashMap<>();
    private double globalAverageFrequency = 1.0;  // Global average frequency
    private int frequencyUpdateCounter = 0;
    private static final int FREQUENCY_UPDATE_INTERVAL = 1000; // Frequency statistics update interval
    private static final int STATS_OUTPUT_INTERVAL = 10000; // Statistics output interval: every 10000 records
    private final AtomicLong processedRecords = new AtomicLong(0); // Total processed records count
    
    // Constraint statistics
    private final AtomicLong constrainedDecisions = new AtomicLong(0);
    private final AtomicLong unconstrainedDecisions = new AtomicLong(0);
    private final AtomicLong limitedExpansions = new AtomicLong(0);  // Number of expansions blocked by limit
    private final AtomicLong allowedExpansions = new AtomicLong(0);   // Number of allowed expansions
    
    // Training record class
    private static class TrainingRecord implements java.io.Serializable {
        final double[] stateVector;
        final int action;
        final double reward;
        
        TrainingRecord(double[] stateVector, int action, double reward) {
            this.stateVector = stateVector;
            this.action = action;
            this.reward = reward;
        }
    }
    
    public DRLPartitioner(int numWorkers, int slide, int size, int numOfKeys) {
        super(numWorkers);
        this.state = new State(size, slide, numWorkers, numOfKeys);
        this.windowSize = slide; // Initialize window size
        this.stateSize = numWorkers + numWorkers; // Only load + fragmentation vector, removed key encoding
        this.actionSize = numWorkers;
        this.epsilon = EPSILON_START;
        
        // Build simplified DRL network - only one hidden layer
        MultiLayerConfiguration conf = createNetworkConfig();
        
        // Initialize main network and two target networks
        this.mainNetwork = new MultiLayerNetwork(conf);
        this.mainNetwork.init();
        
        this.targetNetwork1 = new MultiLayerNetwork(conf);
        this.targetNetwork1.init();
        
        this.targetNetwork2 = new MultiLayerNetwork(conf);
        this.targetNetwork2.init();
        
        // Copy main network weights to both target networks
        this.targetNetwork1.setParameters(mainNetwork.params());
        this.targetNetwork2.setParameters(mainNetwork.params());
        
        // Initialize thread pool
        initializeExecutors();
        
        // Initialize normalization components
        this.obsNormalizer = new NormalizeObservation(stateSize, EPSILON);
        this.rewardScaler = new ScaleReward(GAMMA, EPSILON);
        
        // Verify normalization components are properly initialized
        if (this.obsNormalizer == null || this.rewardScaler == null) {
            throw new IllegalStateException("Normalization components initialization failed");
        }
       
        // Set reasonable hot key detection threshold to avoid initial threshold being too high
        int initialThreshold = Math.max(10, numWorkers * 2);
        state.setFrequencyThreshold(initialThreshold);
    }
    
    /**
     * Initialize thread pool
     */
    private void initializeExecutors() {
        if (!executorsInitialized) {
            trainingExecutor = Executors.newSingleThreadExecutor();
            inferenceExecutor = Executors.newSingleThreadExecutor();
            executorsInitialized = true;
        }
    }
    
    /**
     * Create neural network configuration
     */
    private MultiLayerConfiguration createNetworkConfig() {
        // Calculate hidden layer neuron count: input layer + output layer
        int hiddenLayerSize = (stateSize + actionSize);

        return new NeuralNetConfiguration.Builder()
            .seed(83)
            .weightInit(WeightInit.XAVIER)
            .updater(new Adam(0.0001))
            .list()
            .layer(0, new DenseLayer.Builder()
                .nIn(stateSize)
                .nOut(hiddenLayerSize)
                .activation(Activation.RELU)
                .build())
            .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.MSE)
                .nIn(hiddenLayerSize)
                .nOut(actionSize)
                .activation(Activation.IDENTITY)
                .build())
            .build();
    }

    /**
     * Get currently active target network
     */
    private MultiLayerNetwork getActiveTargetNetwork() {
        return activeTargetNetwork == 1 ? targetNetwork1 : targetNetwork2;
    }
    
    /**
     * Get inactive target network (for updating)
     */
    private MultiLayerNetwork getInactiveTargetNetwork() {
        return activeTargetNetwork == 1 ? targetNetwork2 : targetNetwork1;
    }
    
    /**
     * Asynchronously update inactive target network and switch
     */
    private void updateTargetNetworkAsync() {
        if (isUpdatingTargetNetwork.compareAndSet(false, true)) {
            if (trainingExecutor != null) {
                trainingExecutor.submit(() -> {
                    try {
                        // Get inactive network and update its parameters
                        MultiLayerNetwork inactiveNetwork = getInactiveTargetNetwork();
                        INDArray mainNetworkParams = mainNetwork.params();
                        
                        inactiveNetwork.setParameters(mainNetworkParams);
                        
                        // Switch active network
                        activeTargetNetwork = activeTargetNetwork == 1 ? 2 : 1;
                        
                    } catch (Exception e) {
                        System.err.println("Target network update failed: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        isUpdatingTargetNetwork.set(false);
                    }
                });
            } else {
                isUpdatingTargetNetwork.set(false);
            }
        }
    }
    
    /**
     * Copy weights from main network to target network - Deprecated, using async double-buffering approach
     */
    @Deprecated
    private void copyNetworkWeights() {
        // Keep this method for serialization compatibility, but no longer used
    }

    /**
     * Reset routing table
     * Called after neural network training completes to ensure using latest network weights for inference
     */
    private void resetRoutingTable() {
        int oldSize = routingTable.size();
        long hits = routingTableHits.get();
        long misses = routingTableMisses.get();
        long total = hits + misses;
        double hitRate = total > 0 ? (double) hits / total * 100 : 0.0;
        
        routingTable.clear();
        routingTableValid.set(true);
        
        // Reset statistics counters
        routingTableHits.set(0);
        routingTableMisses.set(0);
        
       
    }
    

    
    
    @Override
    public void flatMap(Record record, Collector<Tuple2<Integer, Record>> out) throws Exception {
        // Ensure ExecutorService is initialized
        if (!executorsInitialized) {
            initializeExecutors();
        }
        
        int keyId = record.getKeyId();
        int worker = -1; // Initialize to -1, ensure variable is initialized
        
        // Count processed records
        long recordCount = processedRecords.incrementAndGet();
        
        // Output statistics every 10000 records
        if (recordCount % STATS_OUTPUT_INTERVAL == 0) {
            //outputStatistics(recordCount);
        }

        // 1. Hot key check
        boolean isHot = state.isHot(record, null) == 1;
        state.updateExpired(record, isHot);
        
        double[] stateVector = null;
        
        if (!isHot) {
            // For non-hot keys, still use simple hash partitioning
            worker = Math.abs(keyId % parallelism);
            state.update(record, worker);
            out.collect(new Tuple2<>(worker, record));
            return;
        }
        
        // 2. Get current partition distribution and dynamic limit for key
        BitSet currentPartitions = state.keyfragmentation(keyId);
        int dynamicLimit = getDynamicPartitionLimit(keyId);
        
        // 3. Routing table query (need to check if selected worker is within current partitions)
        boolean needsInference = false;
        
        if (routingTableValid.get() && routingTable.containsKey(keyId)) {
            worker = routingTable.get(keyId);
            
            // If constraint is enabled and key has partition history, verify cached worker is within assigned partitions
            if (ENABLE_PARTITION_CONSTRAINT && currentPartitions.cardinality() >= MIN_PARTITIONS_FOR_CONSTRAINT) {
                if (currentPartitions.get(worker)) {
                    routingTableHits.incrementAndGet();
                } else {
                    // Cached worker is not in assigned partitions, need to recompute
                    routingTableMisses.incrementAndGet();
                    routingTable.remove(keyId); // Remove invalid cache
                    needsInference = true;
                    worker = -1; // Reset worker, will be reassigned in inference step
                }
            } else {
                routingTableHits.incrementAndGet();
            }
        } else {
            routingTableMisses.incrementAndGet();
            needsInference = true;
            // Worker keeps initial value -1, will be assigned in inference step
        }
        
        // If inference is needed
        if (needsInference) {
            // Build state vector
            stateVector = buildStateVector(record);
            
            // Execute neural network inference
            worker = performConstrainedInferenceWithDynamicLimit(stateVector, keyId, currentPartitions, dynamicLimit);
            
            if (routingTableValid.get()) {
                updateRoutingTable(keyId, worker);
            }
        }
        
        // 4. Training
        if (stateVector == null) {
            stateVector = buildStateVector(record);
        }
        
        // Safety check: ensure worker has been initialized to valid value
        if (worker < 0) {
            // If worker is not properly initialized, use default hash partitioning
            worker = Math.abs(keyId % parallelism);
            System.err.println("Warning: worker not initialized, using default hash partitioning: " + worker);
        }
        
        // Calculate reward
        double reward = calculateReward(record, worker);
        
        // Train network
        trainMainNetwork(stateVector, worker, reward);
        
        // Update state
        state.update(record, worker);
        out.collect(new Tuple2<>(worker, record));
        
        epsilon = Math.max(EPSILON_END, epsilon * EPSILON_DECAY);
        
        sampleCounter++;
        if (sampleCounter >= TARGET_UPDATE_FREQUENCY) {
            updateTargetNetworkAsync();
            
             // Clear routing table
            routingTable.clear();
            routingTableValid.set(true);
            sampleCounter = 0;
        }
    }
    
    /**
     * Select the highest Q-value from allocated partitions
     */
    private int selectBestFromExistingPartitions(INDArray qValues, BitSet currentPartitions) {
        int bestAction = -1;
        double bestQValue = Double.NEGATIVE_INFINITY;
        
        for (int i = 0; i < parallelism; i++) {
            if (currentPartitions.get(i)) {
                double qValue = qValues.getDouble(0, i);
                if (qValue > bestQValue) {
                    bestQValue = qValue;
                    bestAction = i;
                }
            }
        }
        
        return bestAction;
    }
    
    /**
     * Get list of allocated partitions
     */
    private List<Integer> getExistingPartitions(BitSet currentPartitions) {
        List<Integer> existingPartitions = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            if (currentPartitions.get(i)) {
                existingPartitions.add(i);
            }
        }
        return existingPartitions;
    }
    
    private double[] buildStateVector(Record record) {
        double[] stateVector = new double[stateSize];
        
        // 1. Calculate load balancing information
        double avgLoad = state.avgLoad();
        for (int i = 0; i < parallelism; i++) {
            double workerLoad = state.getLoad(i);
            // Calculate load deviation rate for each node
            stateVector[i] = avgLoad > 0 ? (workerLoad - avgLoad) / avgLoad : 0.0;
        }
        
        // 2. Fragmentation status - use full fragmentation vector instead of single ratio
        BitSet keyFragmentation = state.keyfragmentation(record.getKeyId());
        for (int i = 0; i < parallelism; i++) {
            stateVector[parallelism + i] = keyFragmentation.get(i) ? 1.0 : 0.0;
        }
        
        // 3. Normalize state vector (if normalizer is available)
        if (obsNormalizer != null) {
            double[] normalizedState = obsNormalizer.process(stateVector);
            return normalizedState;
        } else {
            // If normalizer is not initialized, return original state vector directly
            return stateVector;
        }
    }

    private double calculateReward(Record record, int action) {
        double reward = 0.0;
        
        // 1. Load balancing reward
        double avgLoad = state.avgLoad();
        double L = state.getLoad(action);
        double loadDiff = (L - avgLoad)/avgLoad;
        reward -= loadDiff * 0.5;
        
        // 2. Fragmentation penalty
        double fragmentation = state.keyfragmentation(record.getKeyId()).cardinality() / (double)parallelism;
        reward -= fragmentation * 0.5;

        // 3. Scale and normalize reward (if scaler is available)
        if (rewardScaler != null) {
            double scaledReward = rewardScaler.process(reward, false);
            return scaledReward;
        } else {
            // If reward scaler is not initialized, return original reward directly
            return reward;
        }
    }

    /**
     * DRL training with experience - asynchronous batch execution, limited buffer size
     */
    private void trainMainNetwork(double[] stateVector, int action, double reward) {
        // Check if network is initialized
        if (mainNetwork == null) {
            return;
        }
        
        // Ensure executor is initialized
        if (!executorsInitialized) {
            initializeExecutors();
        }
        
        // Add training sample to buffer
        synchronized (recordBuffer) {
            // If buffer is full, discard new record to avoid over-training
            if (recordBuffer.size() >= BATCH_SIZE) {
                return; // Discard new record directly
            }
            
            recordBuffer.add(new TrainingRecord(stateVector, action, reward));
            
            // If buffer reaches batch size, start training
            if (recordBuffer.size() >= BATCH_SIZE && !isTraining.get()) {
                // Copy current buffer content
                List<TrainingRecord> batch = new ArrayList<>(recordBuffer);
                recordBuffer.clear();
                
                // Execute batch training asynchronously
                trainingExecutor.submit(() -> trainBatch(batch));
            }
        }
    }
    
    /**
     * Batch train main network
     */
    private void trainBatch(List<TrainingRecord> batch) {
        if (isTraining.compareAndSet(false, true)) {
            try {
                //networkLock.writeLock().lock();
                
                // 1. Prepare batch input
                int batchSize = batch.size();
                INDArray stateInputs = Nd4j.create(batchSize, stateSize);
                INDArray targetQValues = Nd4j.create(batchSize, actionSize);
                
                // 2. Fill state input and target Q-values
                for (int i = 0; i < batchSize; i++) {
                    TrainingRecord sample = batch.get(i);
                    
                    // Fill state input
                    stateInputs.putRow(i, Nd4j.create(sample.stateVector));
                    
                    // Get current Q-values
                    INDArray currentQ = mainNetwork.output(stateInputs.getRow(i).reshape(1, stateSize));
                    
                    // Update target Q-values
                    INDArray targetRow = currentQ.dup();
                    double newQValue = currentQ.getDouble(0, sample.action) *(1-GAMMA)+ GAMMA * sample.reward;
                    targetRow.putScalar(new int[]{0, sample.action}, newQValue);
                    targetQValues.putRow(i, targetRow);
                }
                
                // 3. Batch train main network
                mainNetwork.fit(stateInputs, targetQValues);
                
                // 4. Reset routing table after training
                resetRoutingTable();
                
            } catch (Exception e) {
                System.err.println("Batch training failed: " + e.getMessage());
                e.printStackTrace();
            } finally {
                //networkLock.writeLock().unlock();
                isTraining.set(false);
            }
        }
    }
    
    /**
     * Custom serialization method - ensure executors are not serialized
     */
    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        out.defaultWriteObject();
    }
    
    /**
     * Custom deserialization method - initialize executors and networks
     */
    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();
        executorsInitialized = false;  // Will be reinitialized on next call
        
        // Reinitialize record buffer
        try {
            java.lang.reflect.Field recordBufferField = this.getClass().getDeclaredField("recordBuffer");
            recordBufferField.setAccessible(true);
            recordBufferField.set(this, new ArrayList<>());
            
            java.lang.reflect.Field recordBufferLockField = this.getClass().getDeclaredField("recordBufferLock");
            recordBufferLockField.setAccessible(true);
            recordBufferLockField.set(this, new Object());
        } catch (Exception e) {
            throw new IOException("Failed to initialize record buffer during deserialization", e);
        }
        
        // Reinitialize networks
        MultiLayerConfiguration conf = createNetworkConfig();
        
        // Initialize main network and two target networks
        MultiLayerNetwork tempMainNetwork = new MultiLayerNetwork(conf);
        tempMainNetwork.init();
        
        MultiLayerNetwork tempTargetNetwork1 = new MultiLayerNetwork(conf);
        tempTargetNetwork1.init();
        
        MultiLayerNetwork tempTargetNetwork2 = new MultiLayerNetwork(conf);
        tempTargetNetwork2.init();
        
        // Copy main network weights to both target networks
        tempTargetNetwork1.setParameters(tempMainNetwork.params().dup());
        tempTargetNetwork2.setParameters(tempMainNetwork.params().dup());
        
        // Set final fields using reflection
        try {
            java.lang.reflect.Field mainNetworkField = this.getClass().getDeclaredField("mainNetwork");
            mainNetworkField.setAccessible(true);
            mainNetworkField.set(this, tempMainNetwork);
            
            java.lang.reflect.Field targetNetwork1Field = this.getClass().getDeclaredField("targetNetwork1");
            targetNetwork1Field.setAccessible(true);
            targetNetwork1Field.set(this, tempTargetNetwork1);
            
            java.lang.reflect.Field targetNetwork2Field = this.getClass().getDeclaredField("targetNetwork2");
            targetNetwork2Field.setAccessible(true);
            targetNetwork2Field.set(this, tempTargetNetwork2);
            
            // Reinitialize normalization components
            NormalizeObservation tempObsNormalizer = new NormalizeObservation(stateSize, EPSILON);
            ScaleReward tempRewardScaler = new ScaleReward(GAMMA, EPSILON);
            
            java.lang.reflect.Field obsNormalizerField = this.getClass().getDeclaredField("obsNormalizer");
            obsNormalizerField.setAccessible(true);
            obsNormalizerField.set(this, tempObsNormalizer);
            
            java.lang.reflect.Field rewardScalerField = this.getClass().getDeclaredField("rewardScaler");
            rewardScalerField.setAccessible(true);
            rewardScalerField.set(this, tempRewardScaler);
            
        } catch (Exception e) {
            throw new IOException("Failed to initialize networks and normalizers during deserialization", e);
        }
    }

    /**
     * Update routing table using LRU strategy
     */
    private void updateRoutingTable(int keyId, int worker) {
        if (!routingTableValid.get()) {
            return;
        }
        
        // If maximum capacity is reached, remove oldest entry
        if (routingTable.size() >= MAX_ROUTING_TABLE_SIZE) {
            // Use keySet().iterator().next() to get first (oldest) element
            routingTable.remove(routingTable.keySet().iterator().next());
        }
        
        routingTable.put(keyId, worker);
    }

    /**
     * Get partition constraint statistics (enhanced version)
     * 
     * @return Statistics string
     */
    public String getPartitionConstraintStats() {
        long totalDecisions = constrainedDecisions.get() + unconstrainedDecisions.get();
        long totalExpansionAttempts = limitedExpansions.get() + allowedExpansions.get();
        
        double constraintRatio = totalDecisions > 0 ? 
            (double) constrainedDecisions.get() / totalDecisions * 100 : 0;
        double limitRatio = totalExpansionAttempts > 0 ?
            (double) limitedExpansions.get() / totalExpansionAttempts * 100 : 0;
            
        StringBuilder stats = new StringBuilder();
        stats.append("Frequency-based Dynamic Partition Constraint Statistics:\n");
        stats.append(String.format("  Constraint status: %s\n", ENABLE_PARTITION_CONSTRAINT ? "Enabled" : "Disabled"));
        stats.append(String.format("  Dynamic limit control: %s\n", ENABLE_FREQUENCY_BASED_LIMIT ? "Enabled" : "Disabled"));
        stats.append(String.format("  Min partitions for constraint: %d\n", MIN_PARTITIONS_FOR_CONSTRAINT));
        stats.append(String.format("  Partition count range: %d - %d\n", MIN_PARTITIONS_PER_KEY, MAX_PARTITIONS_PER_KEY));
        stats.append(String.format("  Logarithm base: %.2f\n", FREQUENCY_LOG_BASE));
        stats.append(String.format("  Global avg frequency: %.2f\n", globalAverageFrequency));
        stats.append(String.format("  Total decisions: %d\n", totalDecisions));
        stats.append(String.format("  Constrained decisions: %d (%.2f%%)\n", constrainedDecisions.get(), constraintRatio));
        stats.append(String.format("  Unconstrained decisions: %d (%.2f%%)\n", unconstrainedDecisions.get(), 100 - constraintRatio));
        stats.append(String.format("  Limited expansions: %d (%.2f%%)\n", limitedExpansions.get(), limitRatio));
        stats.append(String.format("  Allowed expansions: %d (%.2f%%)\n", allowedExpansions.get(), 100 - limitRatio));
        
        // Add frequency and partition limit distribution statistics
        if (!keyPartitionLimitCache.isEmpty()) {
            Map<Integer, Integer> limitDistribution = new HashMap<>();
            for (Integer limit : keyPartitionLimitCache.values()) {
                limitDistribution.put(limit, limitDistribution.getOrDefault(limit, 0) + 1);
            }
            stats.append("  Partition limit distribution:\n");
            for (Map.Entry<Integer, Integer> entry : limitDistribution.entrySet()) {
                stats.append(String.format("    %d partitions: %d keys\n", entry.getKey(), entry.getValue()));
            }
        }
        
        return stats.toString();
    }
    
    /**
     * Check if key is currently under partition constraint
     * 
     * @param keyId Key ID
     * @return true if key is constrained, false otherwise
     */
    public boolean isKeyCurrentlyConstrained(int keyId) {
        if (!ENABLE_PARTITION_CONSTRAINT) {
            return false;
        }
        BitSet fragmentation = state.keyfragmentation(keyId);
        return fragmentation.cardinality() >= MIN_PARTITIONS_FOR_CONSTRAINT;
    }
    
    /**
     * Get current partition count for key
     * 
     * @param keyId Key ID
     * @return Current partition count
     */
    public int getKeyCurrentPartitionCount(int keyId) {
        return state.keyfragmentation(keyId).cardinality();
    }
    
    /**
     * Get current partition set for key
     * 
     * @param keyId Key ID
     * @return Current partition set
     */
    public Set<Integer> getKeyCurrentPartitions(int keyId) {
        BitSet fragmentation = state.keyfragmentation(keyId);
        Set<Integer> partitions = new HashSet<>();
        for (int i = 0; i < parallelism; i++) {
            if (fragmentation.get(i)) {
                partitions.add(i);
            }
        }
        return partitions;
    }
    
    /**
     * Clean up resources on close
     */
    public void close() {
        if (trainingExecutor != null) {
            trainingExecutor.shutdown();
        }
        if (inferenceExecutor != null) {
            inferenceExecutor.shutdown();
        }
        
        // Output final partition constraint statistics
        System.out.println("=== DRL Partitioner Shutdown Statistics ===");
        System.out.println(getPartitionConstraintStats());
    }

    /**
     * Check if key has reached partition limit
     * 
     * @param keyId Key ID
     * @return true if limit reached, false otherwise
     */
    public boolean isKeyAtPartitionLimit(int keyId) {
        if (!ENABLE_FREQUENCY_BASED_LIMIT) {
            return false;
        }
        BitSet fragmentation = state.keyfragmentation(keyId);
        int dynamicLimit = getDynamicPartitionLimit(keyId);
        return fragmentation.cardinality() >= dynamicLimit;
    }

    /**
     * Get dynamic partition limit for key
     * 
     * @param keyId Key ID
     * @return Partition limit for the key
     */
    public int getKeyDynamicPartitionLimit(int keyId) {
        return getDynamicPartitionLimit(keyId);
    }

    
    /**
     * Monitor detailed status information for specified key
     * 
     * @param keyId Key ID
     * @return Detailed status string for the key
     */
    public String monitorKeyStatus(int keyId) {
        StringBuilder status = new StringBuilder();
        status.append(String.format("=== Key %d Status Monitor ===\n", keyId));
        
        // Basic information
        int frequency = getKeyFrequency(keyId);
        int dynamicLimit = getDynamicPartitionLimit(keyId);
        int currentPartitions = getKeyCurrentPartitionCount(keyId);
        Set<Integer> partitionSet = getKeyCurrentPartitions(keyId);
        
        status.append(String.format("  Key frequency: %d\n", frequency));
        status.append(String.format("  Dynamic partition limit: %d\n", dynamicLimit));
        status.append(String.format("  Current partition count: %d\n", currentPartitions));
        status.append(String.format("  Current partition set: %s\n", partitionSet));
        
        // Detailed calculation info
        double normalizedFrequency = (double) frequency / windowSize * parallelism;
        status.append(String.format("  Normalized frequency density: %.6f\n", normalizedFrequency));
        status.append(String.format("  Calculation formula: %d / %d * %d = %.6f\n", frequency, windowSize, parallelism, normalizedFrequency));
        
        // Constraint status
        boolean isConstrained = isKeyCurrentlyConstrained(keyId);
        boolean atLimit = isKeyAtPartitionLimit(keyId);
        status.append(String.format("  Is constrained: %s\n", isConstrained ? "Yes" : "No"));
        status.append(String.format("  Is at limit: %s\n", atLimit ? "Yes" : "No"));
        
        // Limit calculation process
        status.append(String.format("  Limit calculation process:\n"));
        status.append(String.format("    Normalized frequency density: %.6f\n", normalizedFrequency));
        status.append(String.format("    floor(%.6f) = %d\n", normalizedFrequency, (int) Math.floor(normalizedFrequency)));
        status.append(String.format("    Final limit: %d + %d = %d\n", 
            MIN_PARTITIONS_PER_KEY, (int) Math.floor(normalizedFrequency), dynamicLimit));
            
        status.append("============================\n");
        
        return status.toString();
    }
    
    /**
     * Monitor status of multiple keys
     * 
     * @param keyIds Key ID array
     * @return Status string for all keys
     */
    public String monitorMultipleKeys(int... keyIds) {
        StringBuilder result = new StringBuilder();
        result.append("=== Multiple Keys Status Monitor ===\n");
        for (int keyId : keyIds) {
            result.append(monitorKeyStatus(keyId));
        }
        return result.toString();
    }
    
    /**
     * Constrained inference based on key partition distribution and dynamic limit
     */
    private int performConstrainedInferenceWithDynamicLimit(double[] stateVector, int keyId, 
                                                          BitSet currentPartitions, int dynamicLimit) {
        if (getActiveTargetNetwork() == null) {
            // If network is not initialized, prioritize selection from assigned partitions, otherwise use hash
            if (ENABLE_PARTITION_CONSTRAINT && currentPartitions.cardinality() > 0) {
                List<Integer> existingPartitions = getExistingPartitions(currentPartitions);
                int selectedWorker = existingPartitions.get(Math.abs(keyId % existingPartitions.size()));
                return selectedWorker;
            }
            int hashWorker = Math.abs(keyId % parallelism);
            return hashWorker;
        }
        
        return selectActionWithDynamicLimit(stateVector, keyId, currentPartitions, dynamicLimit);
    }
    
    /**
     * Action selection under dynamic partition limit constraint
     */
    private int selectActionWithDynamicLimit(double[] stateVector, int keyId, 
                                           BitSet currentPartitions, int dynamicLimit) {
        try {
            //networkLock.readLock().lock();
            
            // Check if constraint needs to be applied
            boolean shouldConstrain = ENABLE_PARTITION_CONSTRAINT && 
                                    currentPartitions.cardinality() >= MIN_PARTITIONS_FOR_CONSTRAINT;
            
            // Check if dynamic partition limit is reached
            boolean reachedDynamicLimit = ENABLE_FREQUENCY_BASED_LIMIT && 
                                        currentPartitions.cardinality() >= dynamicLimit;
            
            if (Math.random() < epsilon) {
                // Exploration phase
                return handleExplorationWithDynamicLimits(keyId, currentPartitions, shouldConstrain, reachedDynamicLimit);
            } else {
                // Exploitation phase: use Q-values for selection
                return handleExploitationWithDynamicLimits(stateVector, keyId, currentPartitions, shouldConstrain, reachedDynamicLimit);
            }
        } finally {
            //networkLock.readLock().unlock();
        }
    }
    
    /**
     * Handle partition selection in exploration phase (considering dynamic limits)
     */
    private int handleExplorationWithDynamicLimits(int keyId, BitSet currentPartitions, 
                                                  boolean shouldConstrain, boolean reachedDynamicLimit) {
        // If any constraint conditions exist (partition count constraint or dynamic limit), select from assigned partitions
        if (shouldConstrain || reachedDynamicLimit) {
            List<Integer> existingPartitions = getExistingPartitions(currentPartitions);
            if (!existingPartitions.isEmpty()) {
                // Update statistics based on constraint type
                if (reachedDynamicLimit) {
                    limitedExpansions.incrementAndGet();
                }
                constrainedDecisions.incrementAndGet();
                int selectedWorker = existingPartitions.get((int) (Math.random() * existingPartitions.size()));
                return selectedWorker;
            }
        }
        
        // If no historical partitions or constraint not enabled, random global selection
        unconstrainedDecisions.incrementAndGet();
        int globalWorker = (int) (Math.random() * parallelism);
        return globalWorker;
    }
    
    /**
     * Handle partition selection in exploitation phase (considering dynamic limits)
     */
    private int handleExploitationWithDynamicLimits(double[] stateVector, int keyId, BitSet currentPartitions,
                                                   boolean shouldConstrain, boolean reachedDynamicLimit) {
        INDArray stateInput = Nd4j.create(stateVector).reshape(1, stateSize);
        INDArray qValues = getActiveTargetNetwork().output(stateInput);
        
        if (reachedDynamicLimit) {
            // Dynamic limit reached, force selection of highest Q-value from assigned partitions
            int bestAction = selectBestFromExistingPartitions(qValues, currentPartitions);
            if (bestAction != -1) {
                limitedExpansions.incrementAndGet();
                constrainedDecisions.incrementAndGet();
                return bestAction;
            }
        } else if (shouldConstrain) {
            // Dynamic limit not reached, directly use global best Q-value
            int bestGlobal = Nd4j.argMax(qValues, 1).getInt(0);
            unconstrainedDecisions.incrementAndGet();
            return bestGlobal;
        }
        
        // If no historical partitions or constraint not enabled, select global best Q-value
        unconstrainedDecisions.incrementAndGet();
        int globalBest = Nd4j.argMax(qValues, 1).getInt(0);
        return globalBest;
    }
    
    /**
     * Determine if expansion to new partition should be allowed (simplified: based only on frequency limit)
     */
    private boolean shouldAllowExpansionWithDynamicLimit(int keyId, BitSet currentPartitions) {
        // Remove load-aware mechanism, directly allow expansion (controlled by dynamic limit)
        return true;
    }
    
    /**
     * Calculate dynamic partition limit based on key frequency
     * Uses logarithmic function mapping: log_base(frequency * scale + 1) 
     * 
     * @param keyId Key ID
     * @return Partition limit for the key
     */
    private int getDynamicPartitionLimit(int keyId) {
        if (!ENABLE_FREQUENCY_BASED_LIMIT) {
            return MAX_PARTITIONS_PER_KEY; // If dynamic limit not enabled, return maximum value
        }

        // Get key frequency
        int frequency = getKeyFrequency(keyId);
        
        // Use logarithmic function to map frequency to partition limit
        int dynamicLimit = calculatePartitionLimitFromFrequency(frequency);

        return dynamicLimit;
    }
    
    /**
     * Calculate partition limit based on frequency
     * New version: use normalized frequency density for logarithmic mapping
     * Use logarithmic mapping only when frequency exceeds threshold F = windowSize / numPartitions, otherwise fixed to 1
     * 
     * @param frequency Key frequency
     * @return Partition limit
     */
    private int calculatePartitionLimitFromFrequency(int frequency) {
        if (frequency <= 0) {
            return MIN_PARTITIONS_PER_KEY;
        }
        
        // Get window size and number of partitions
        int numPartitions = parallelism; // Number of partitions
        
        // Calculate frequency threshold: F = windowSize / numPartitions
        double frequencyThreshold = (double) windowSize / numPartitions;
        
        // If frequency is below threshold, return minimum partition count
        if (frequency <= frequencyThreshold) {
            return MIN_PARTITIONS_PER_KEY;
        }
        
        // Calculate normalized frequency density: actual frequency / window size * number of partitions
        double normalizedFrequency = (double) frequency / windowSize * numPartitions;
        
        // Logarithmic mapping: log_base(normalizedFrequency)
        // Use change of base formula: log_a(x) = ln(x) / ln(a)
        // Round up to ensure at least 1
        double logValue = Math.log(normalizedFrequency) / Math.log(FREQUENCY_LOG_BASE)+1;
        int dynamicLimit = (int) Math.ceil(logValue);
        
        // Ensure result is within valid range
        int finalLimit = Math.max(MIN_PARTITIONS_PER_KEY, Math.min(MAX_PARTITIONS_PER_KEY, dynamicLimit));
        
        return finalLimit;
    }
    
    /**
     * Get key frequency statistics (using real frequency data from HotStatistics)
     * 
     * @param keyId Key ID
     * @return Key frequency (based on HotStatistics)
     */
    private int getKeyFrequency(int keyId) {
        // First try to get from cache
        
        // Use real key frequency statistics from HotStatistics in State
        int frequency = state.getKeyFrequency(keyId); // At least 1, to avoid zero frequency
        
        // Cache result
        return frequency;
    }
    
    /**
     * Update global frequency statistics
     */
    /**
     * Output statistics
     */
    private void outputStatistics(long recordCount) {
        long totalDecisions = constrainedDecisions.get() + unconstrainedDecisions.get();
        long totalExpansionAttempts = limitedExpansions.get() + allowedExpansions.get();
        
        double constraintRatio = totalDecisions > 0 ? 
            (double) constrainedDecisions.get() / totalDecisions * 100 : 0;
        double limitRatio = totalExpansionAttempts > 0 ?
            (double) limitedExpansions.get() / totalExpansionAttempts * 100 : 0;
            
        System.out.println("=== DRL Partitioner Statistics (Record count: " + recordCount + ") ===");
        System.out.println(String.format("  Constraint status: %s", ENABLE_PARTITION_CONSTRAINT ? "Enabled" : "Disabled"));
        System.out.println(String.format("  Dynamic limit control: %s", ENABLE_FREQUENCY_BASED_LIMIT ? "Enabled" : "Disabled"));
        System.out.println(String.format("  Min constrained partitions: %d", MIN_PARTITIONS_FOR_CONSTRAINT));
        System.out.println(String.format("  Partition count range: %d - %d", MIN_PARTITIONS_PER_KEY, MAX_PARTITIONS_PER_KEY));
        System.out.println(String.format("  Logarithm base: %.2f", FREQUENCY_LOG_BASE));
        System.out.println(String.format("  Global avg frequency: %.2f", globalAverageFrequency));
        System.out.println(String.format("  Total records for hot key detection: %d", state.getTotalCountOfRecords()));
        System.out.println(String.format("  Total decisions: %d", totalDecisions));
        System.out.println(String.format("  Constrained decisions: %d (%.2f%%)", constrainedDecisions.get(), constraintRatio));
        System.out.println(String.format("  Unconstrained decisions: %d (%.2f%%)", unconstrainedDecisions.get(), 100 - constraintRatio));
        System.out.println(String.format("  Limited expansions: %d (%.2f%%)", limitedExpansions.get(), limitRatio));
        System.out.println(String.format("  Allowed expansions: %d (%.2f%%)", allowedExpansions.get(), 100 - limitRatio));
        
        // Add frequency and partition limit distribution statistics
        if (!keyPartitionLimitCache.isEmpty()) {
            Map<Integer, Integer> limitDistribution = new HashMap<>();
            for (Integer limit : keyPartitionLimitCache.values()) {
                limitDistribution.put(limit, limitDistribution.getOrDefault(limit, 0) + 1);
            }
            System.out.println("  Partition limit distribution:");
            for (Map.Entry<Integer, Integer> entry : limitDistribution.entrySet()) {
                System.out.println(String.format("    %d partitions: %d keys", entry.getKey(), entry.getValue()));
            }
        }
        System.out.println("========================");
    }
} 