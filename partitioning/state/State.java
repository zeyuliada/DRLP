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

package partitioning.state;

import java.util.*;
import java.io.Serializable;

import partitioning.containers.PartialAssignment;
import partitioning.containers.Worker;
import partitioning.containers.Frequency;
import partitioning.containers.HotStatistics;
import record.Record;

/**
 * Class responsible for keeping the necessary state to calculate Dalton's rewards
 */
public class State implements Serializable{

    // track load of each worker
    private List<Worker> workers;

    // track fragmentation in a sliding window
    private Queue<PartialAssignment> memPool;
    private Queue<PartialAssignment> activeSlides;
    private PartialAssignment lastSlide;
    private int aggrLoad;

    private Map<Integer, int[]> keyAssignmentsRefCnt;
    private Map<Integer, BitSet> keyAssignmentsBitSet;

    private int nextUpdate;
    private int slide;
    private int size;
    private int numWorkers;

    private boolean hasSeenHotKeys;

    private HotStatistics hotStatistics;

    private int maxSplit;
    private Set<Integer> hotMaxSplit;

    // Latency statistics fields
    private long totalProcessingTime;
    private long totalQueueingTime;
    private long totalEndToEndTime;
    private long recordCount;
    private long maxProcessingLatency;
    private long maxQueueingLatency;
    private long maxTotalLatency;
    
    // Output related fields
    private static final long OUTPUT_INTERVAL_MS = 10000; // 10 seconds
    private long lastOutputTime = 0;
    private transient java.io.BufferedWriter latencyWriter; // Marked as transient, not serialized
    private final String latencyFileName;
    private static final java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public State(int size, int slide, int numWorkers, int estimatedNumKeys){
        workers = new ArrayList<>(numWorkers);
        aggrLoad = 0;
        keyAssignmentsBitSet = new HashMap<>(estimatedNumKeys);
        keyAssignmentsRefCnt = new HashMap<>(estimatedNumKeys);
        for(int i = 0; i < numWorkers; i++){
            Worker w = new Worker(size, slide);
            workers.add(w);
        }

        int activeSlides = (int) Math.ceil(size/slide) + 1;
        memPool = new LinkedList<>();
        this.activeSlides = new LinkedList<>();
        for(int i = 0; i < activeSlides; i++){
            memPool.add(new PartialAssignment());
        }
        lastSlide = memPool.poll();

        nextUpdate = slide;

        this.slide = slide;
        this.size = size;
        this.numWorkers = numWorkers;
        this.hasSeenHotKeys = false;

        hotStatistics = new HotStatistics(numWorkers, estimatedNumKeys, slide);

        maxSplit = 2;
        hotMaxSplit = new HashSet<>(numWorkers);

        // Initialize latency statistics fields
        totalProcessingTime = 0;
        totalQueueingTime = 0;
        totalEndToEndTime = 0;
        recordCount = 0;
        maxProcessingLatency = 0;
        maxQueueingLatency = 0;
        maxTotalLatency = 0;
        
        // Initialize metrics file name using absolute path
        java.io.File outputDir = new java.io.File("output");
        this.latencyFileName = outputDir.getAbsolutePath() + "/latency_metrics_" + System.currentTimeMillis() + ".csv";
        // Do not initialize file writer in constructor to avoid creating too many files in distributed environment
        System.out.println("Latency metrics file will be saved to: " + latencyFileName);
    }

    /**
     * Initialize latency metrics writer
     */
    private void initializeLatencyWriter() {
        try {
            // Ensure directory exists
            java.io.File directory = new java.io.File("output");
            if (!directory.exists()) {
                boolean created = directory.mkdirs();
                System.out.println("Create output directory: " + (created ? "success" : "failed"));
            }
            
            // Create file using absolute path
            java.io.File file = new java.io.File(latencyFileName);
            System.out.println("Attempting to create file: " + file.getAbsolutePath());
            
            latencyWriter = new java.io.BufferedWriter(new java.io.FileWriter(file));
            // Write CSV header
            latencyWriter.write("Timestamp,AvgProcessingLatency,AvgQueueingLatency,AvgTotalLatency," +
                               "MaxProcessingLatency,MaxQueueingLatency,MaxTotalLatency,RecordCount\n");
            latencyWriter.flush();
            System.out.println("Latency metrics file initialized successfully");
        } catch (java.io.IOException e) {
            System.err.println("Failed to initialize latency writer: " + e.getMessage());
            e.printStackTrace(); // Print detailed error information
        }
    }
    
    /**
     * Record and update latency statistics
     * 
     * @param tuple Completed processing tuple
     */
    private void updateLatencyStatistics(Record tuple) {
        long processingLatency = tuple.getProcessingLatency();
        long queueingLatency = tuple.getQueueingLatency();
        long totalLatency = tuple.getTotalLatency();
        
        if (processingLatency > 0) {
            totalProcessingTime += processingLatency;
            maxProcessingLatency = Math.max(maxProcessingLatency, processingLatency);
        }
        
        if (queueingLatency > 0) {
            totalQueueingTime += queueingLatency;
            maxQueueingLatency = Math.max(maxQueueingLatency, queueingLatency);
        }
        
        if (totalLatency > 0) {
            totalEndToEndTime += totalLatency;
            maxTotalLatency = Math.max(maxTotalLatency, totalLatency);
        }
        
        recordCount++;
        
        // Periodically write latency statistics
        writeLatencyMetrics();
    }
    
    /**
     * Write latency statistics to CSV file
     */
    private void writeLatencyMetrics() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastOutputTime >= OUTPUT_INTERVAL_MS) {
            try {
                // Calculate average latency
                double avgProcessingLatency = recordCount > 0 ? (double)totalProcessingTime / recordCount : 0;
                double avgQueueingLatency = recordCount > 0 ? (double)totalQueueingTime / recordCount : 0;
                double avgTotalLatency = recordCount > 0 ? (double)totalEndToEndTime / recordCount : 0;
                
                // Output to console
                System.out.println("===== Latency Statistics =====");
                System.out.println("Record count: " + recordCount);
                System.out.println("Avg processing latency: " + String.format("%.2f", avgProcessingLatency) + " ms");
                System.out.println("Avg queueing latency: " + String.format("%.2f", avgQueueingLatency) + " ms");
                System.out.println("Avg end-to-end latency: " + String.format("%.2f", avgTotalLatency) + " ms");
                System.out.println("Max processing latency: " + maxProcessingLatency + " ms");
                System.out.println("Max queueing latency: " + maxQueueingLatency + " ms");
                System.out.println("Max end-to-end latency: " + maxTotalLatency + " ms");
                System.out.println("==============================");
                
                // Try to write to file (if available)
                if (latencyWriter == null) {
                    // Lazy initialization of file writer
                    initializeLatencyWriter();
                }
                
                if (latencyWriter != null) {
                    // Write CSV row
                    String timestamp = dateFormat.format(new java.util.Date(currentTime));
                    String metricsLine = String.format("%s,%.2f,%.2f,%.2f,%d,%d,%d,%d\n",
                        timestamp, avgProcessingLatency, avgQueueingLatency, avgTotalLatency,
                        maxProcessingLatency, maxQueueingLatency, maxTotalLatency, recordCount);
                    
                    latencyWriter.write(metricsLine);
                    latencyWriter.flush();
                    System.out.println("Latency data written to file: " + latencyFileName);
                } else {
                    System.err.println("Unable to write latency data: file writer not initialized");
                }
                
                lastOutputTime = currentTime;
            } catch (java.io.IOException e) {
                System.err.println("Failed to write latency metrics: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public int getExpirationTs(){
        return hotStatistics.getExpirationTs();
    }

    private void expireOld(long now){
        PartialAssignment expiredSlide = activeSlides.peek();
        if(expiredSlide != null && expiredSlide.timestamp <= now - size) {
            activeSlides.poll();

            for(Map.Entry<Integer, BitSet> e : expiredSlide.keyAssignment.entrySet()) {
                try {
                    for (int w = 0; w < e.getValue().size(); w++) {
                        if (e.getValue().get(w)) {
                            keyAssignmentsRefCnt.get(e.getKey())[w]--;
                            if (keyAssignmentsRefCnt.get(e.getKey())[w] == 0) {
                                keyAssignmentsBitSet.get(e.getKey()).set(e.getKey(), false);
                            }
                        }
                    }
                } catch (NullPointerException ex){}
            }
            expiredSlide.clear();
            memPool.offer(expiredSlide);
        }
        if (hasSeenHotKeys){
            for(Map.Entry<Integer, BitSet> e : lastSlide.keyAssignment.entrySet()){
                for(int i=0; i<numWorkers; i++){
                    if (e.getValue().get(i)){
                        try {
                            keyAssignmentsRefCnt.get(e.getKey())[i]++;
                        }
                        catch (NullPointerException ex){
                            int[] temp= new int[numWorkers];
                            temp[i] = 1;
                            keyAssignmentsRefCnt.put(e.getKey(), temp);
                        }
                    }
                }
                try {
                    keyAssignmentsBitSet.get(e.getKey()).or(e.getValue());
                }catch (NullPointerException ex){
                    keyAssignmentsBitSet.put(e.getKey(), e.getValue());
                }
            }
        }
    }

    private void addNewSlide(long ts){
        lastSlide.timestamp = ts;
        activeSlides.offer(lastSlide);
        lastSlide = memPool.poll();
    }

    public void updateExpired(Record t, boolean hotKey){ //diff
        if(t.getTs() >= nextUpdate){
            expireOld(t.getTs());
            addNewSlide(nextUpdate);
            for(Worker w : workers){
                aggrLoad -= w.expireOld(t.getTs());
                w.addNewSlide(nextUpdate);
            }
            hasSeenHotKeys = hotKey;
            nextUpdate += slide;
        }
        else if (!hasSeenHotKeys && hotKey){ //first hot key in the slide
            hasSeenHotKeys = true;
        }
    }

    public int isHot(Record tuple, List<Frequency> topKeys){
        return hotStatistics.isHot(tuple, keyAssignmentsBitSet.size(), topKeys);
    }

    public int isHotDAG(Record tuple){
        return hotStatistics.isHotDAG(tuple);
    }

    public void update(Record tuple, int worker){
        updateAssignments(tuple.getKeyId(), worker);
        aggrLoad++;
        workers.get(worker).updateState();
    }

    private void updateAssignments(int key, int worker){
        try {
            lastSlide.keyAssignment.get(key).set(worker);
        }catch (NullPointerException e){
            BitSet b = new BitSet(numWorkers);
            b.set(worker);
            lastSlide.keyAssignment.put(key, b);
        }
    }

    public BitSet keyfragmentation(int key){

        BitSet b = new BitSet(numWorkers);
        try {
            b.or(lastSlide.keyAssignment.get(key));
        }catch (NullPointerException e){
        }
        try {
            b.or(keyAssignmentsBitSet.get(key));
        }catch (NullPointerException e){
        }
        return b;
    }

    public double loadDAG(int selectedWorker){
        final double avgLoad = ((double) aggrLoad)/numWorkers;
        final double L = workers.get(selectedWorker).getLoad();
        return (L - avgLoad)/avgLoad;
    }

    public double avgLoad(){
        return ((double) aggrLoad)/numWorkers;
    }

    public double maxLoad(){
        double maxLoad = 0;
        for (int i = 0; i < numWorkers; i++) {
            double load = getLoad(i);
            if (load > maxLoad) maxLoad = load;
        }
        return maxLoad;
    }

    public double getLoad(int worker){
        return workers.get(worker).getLoad();
    }

    public double reward(int key, int selectedWorker, int ch){
        final double kf = keyfragmentation(key).cardinality();
        final double costReduce = kf / numWorkers;

        if (kf == maxSplit){
            hotMaxSplit.add(key);
        }

        final double avgLoad = avgLoad();
        final double L = getLoad(selectedWorker);
        final double costMap = (L - avgLoad)/Math.max(L,avgLoad);

        if (hotMaxSplit.size() >= 0.7*ch  && maxSplit < 6){
            hotMaxSplit.clear();
            maxSplit++;
        }

        return -(0.5 * costMap + 0.5 * costReduce);
    }

    public boolean inHotMax(int keyId){
        return hotMaxSplit.contains(keyId);
    }

    public void removeFromHotMax(int keyId){
        hotMaxSplit.remove(keyId);
    }

    public int getTotalCountOfRecords(){
        return hotStatistics.getTotal();
    }

    public void setFrequencyThreshold(int t){
        hotStatistics.setFrequencyThreshold(t);
    }

    public void setHotInterval(int h){
        hotStatistics.setHotInterval(h);
    }

    /**
     * Get frequency statistics for specified key
     * 
     * @param keyId Key ID
     * @return Key frequency, returns 0 if key does not exist
     */
    public int getKeyFrequency(int keyId) {
        return hotStatistics.getKeyFrequency(keyId);
    }

    /**
     * Get window size
     * 
     * @return Window size (slide)
     */
    public int getSlide() {
        return slide;
    }

    /**
     * Get average processing latency
     * @return Average processing latency (milliseconds)
     */
    public double getAvgProcessingLatency() {
        return recordCount > 0 ? (double)totalProcessingTime / recordCount : 0;
    }
    
    /**
     * Get average queueing latency
     * @return Average queueing latency (milliseconds)
     */
    public double getAvgQueueingLatency() {
        return recordCount > 0 ? (double)totalQueueingTime / recordCount : 0;
    }
    
    /**
     * Get average end-to-end latency
     * @return Average end-to-end latency (milliseconds)
     */
    public double getAvgTotalLatency() {
        return recordCount > 0 ? (double)totalEndToEndTime / recordCount : 0;
    }
    
    /**
     * Get maximum processing latency
     * @return Maximum processing latency (milliseconds)
     */
    public long getMaxProcessingLatency() {
        return maxProcessingLatency;
    }
    
    /**
     * Get maximum queueing latency
     * @return Maximum queueing latency (milliseconds)
     */
    public long getMaxQueueingLatency() {
        return maxQueueingLatency;
    }
    
    /**
     * Get maximum end-to-end latency
     * @return Maximum end-to-end latency (milliseconds)
     */
    public long getMaxTotalLatency() {
        return maxTotalLatency;
    }
    
    /**
     * Get processed record count
     * @return Record count
     */
    public long getRecordCount() {
        return recordCount;
    }
    
    /**
     * Close resources
     */
    public void close() {
        try {
            if (latencyWriter != null) {
                latencyWriter.close();
            }
        } catch (java.io.IOException e) {
            System.err.println("Failed to close latency writer: " + e.getMessage());
        }
    }
    
    // Custom serialization method
    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        // Close file writer before serialization
        if (latencyWriter != null) {
            try {
                latencyWriter.close();
            } catch (java.io.IOException e) {
                // Ignore close errors
            }
            latencyWriter = null;
        }
        out.defaultWriteObject();
    }
    
    // Custom deserialization method
    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();
        lastOutputTime = 0;
        // Do not initialize file writer during deserialization, initialize on first use instead
        // This avoids creating too many files in distributed environment
    }
}
