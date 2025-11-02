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

package record;

/**
 * Encapsulates a record
 * stores its partitioning key, timestamp and whether the key is hot (used for key-forwarding)
 */
public class Record {
    protected int keyId; // partitioning key
    protected long ts; // timestamp

    private boolean isHot = true;
    
    // End-to-end latency measurement fields
    protected long creationTime; // Record creation time
    protected long processingStartTime; // Processing start time
    protected long processingEndTime; // Processing end time

    public Record(int keyId, long ts){
        this.keyId = keyId;
        this.ts = ts;
        this.creationTime = System.currentTimeMillis();
        this.processingStartTime = 0;
        this.processingEndTime = 0;
    }

    public int getKeyId() {
        return keyId;
    }

    public void setKeyId(int keyId) {
        this.keyId = keyId;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long newTs){
        ts = newTs;
    }

    public boolean isHot(){
        return isHot;
    }

    public void setHot(boolean isHot){
        this.isHot = isHot;
    }
    
    /**
     * Set processing start time
     */
    public void setProcessingStartTime() {
        this.processingStartTime = System.currentTimeMillis();
    }
    
    /**
     * Set processing end time
     */
    public void setProcessingEndTime() {
        this.processingEndTime = System.currentTimeMillis();
    }
    
    /**
     * Get record creation time
     * @return Creation timestamp
     */
    public long getCreationTime() {
        return creationTime;
    }
    
    /**
     * Get processing start time
     * @return Processing start timestamp
     */
    public long getProcessingStartTime() {
        return processingStartTime;
    }
    
    /**
     * Get processing end time
     * @return Processing end timestamp
     */
    public long getProcessingEndTime() {
        return processingEndTime;
    }
    
    /**
     * Calculate latency from creation to processing start
     * @return Latency time (milliseconds), returns -1 if processing has not started
     */
    public long getQueueingLatency() {
        if (processingStartTime == 0) {
            return -1;
        }
        return processingStartTime - creationTime;
    }
    
    /**
     * Calculate processing latency
     * @return Processing latency (milliseconds), returns -1 if processing is not complete
     */
    public long getProcessingLatency() {
        if (processingStartTime == 0 || processingEndTime == 0) {
            return -1;
        }
        return processingEndTime - processingStartTime;
    }
    
    /**
     * Calculate total end-to-end latency
     * @return Total latency (milliseconds), returns -1 if processing is not complete
     */
    public long getTotalLatency() {
        if (processingEndTime == 0) {
            return -1;
        }
        return processingEndTime - creationTime;
    }
}

