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

package partitioning.dqn.containers;

import java.io.Serializable;

/**
 * Observation normalization wrapper
 * Uses plain array computation, independent of INDArray
 */
public class NormalizeObservation implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private SampleMeanStd obsStats;
    private double epsilon;
    private int obsShape;
    
    /**
     * Constructor
     * @param obsShape Dimensionality of observations
     * @param epsilon Small constant to avoid division by zero
     */
    public NormalizeObservation(int obsShape, double epsilon) {
        this.obsShape = obsShape;
        this.obsStats = new SampleMeanStd(obsShape);
        this.epsilon = epsilon;
    }
    
    /**
     * Normalize observation
     * @param obs Raw observation
     * @return Normalized observation
     */
    public double[] process(double[] obs) {
        if (obs.length != obsShape) {
            throw new IllegalArgumentException("Observation dimension mismatch: " + obs.length + " vs " + obsShape);
        }
        
        // Update statistics and normalize
        obsStats.update(obs);
        return obsStats.normalize(obs, epsilon);
    }
    
    /**
     * Normalize observation only without updating statistics (for testing or evaluation phase)
     * @param obs Raw observation
     * @return Normalized observation
     */
    public double[] normalizeOnly(double[] obs) {
        if (obs.length != obsShape) {
            throw new IllegalArgumentException("Observation dimension mismatch: " + obs.length + " vs " + obsShape);
        }
        
        return obsStats.normalize(obs, epsilon);
    }
    
    /**
     * Get observation statistics
     * @return SampleMeanStd statistics object
     */
    public SampleMeanStd getObsStats() {
        return obsStats;
    }
    
    /**
     * Get observation dimensionality
     */
    public int getObsShape() {
        return obsShape;
    }
    
    /**
     * Get number of processed observation samples
     */
    public long getObsCount() {
        return obsStats.getCount();
    }
    
    /**
     * Get current observation mean
     */
    public double[] getObsMean() {
        return obsStats.getMean();
    }
    
    /**
     * Get current observation variance
     */
    public double[] getObsVar() {
        return obsStats.getVar();
    }
    
    /**
     * Get current observation standard deviation
     */
    public double[] getObsStd() {
        return obsStats.getStd(epsilon);
    }
    
    /**
     * Reset all statistics
     */
    public void reset() {
        obsStats.reset();
    }
} 