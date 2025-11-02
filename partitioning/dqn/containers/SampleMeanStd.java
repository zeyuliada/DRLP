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
 * Track and calculate sample mean and variance
 * Implemented with plain arrays, independent of INDArray
 */
public class SampleMeanStd implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private double[] mean; // Mean
    private double[] var;  // Variance
    private double[] m2;   // Sum of squared differences (for Welford's algorithm)
    private long count;    // Sample count
    private int size;      // Array size
    
    /**
     * Constructor, initialize with specified array size
     */
    public SampleMeanStd(int size) {
        this.size = size;
        this.mean = new double[size];
        this.var = new double[size];
        this.m2 = new double[size];
        this.count = 0;
        
        // Initialize variance to 1
        for (int i = 0; i < size; i++) {
            var[i] = 1.0;
        }
    }
    
    /**
     * Update statistics with new sample (using Welford's online algorithm)
     */
    public void update(double[] x) {
        if (x.length != size) {
            throw new IllegalArgumentException("Input array size mismatch: " + x.length + " vs " + size);
        }
        
        count++;
        
        for (int i = 0; i < size; i++) {
            // Welford's online algorithm for updating mean and variance
            double delta = x[i] - mean[i];
            mean[i] += delta / count;
            double delta2 = x[i] - mean[i];
            m2[i] += delta * delta2;
            
            // Calculate variance
            if (count > 1) {
                var[i] = m2[i] / (count - 1);
            } else {
                var[i] = 1.0; // Set variance to 1 for single sample
            }
        }
    }
    
    /**
     * Get a copy of the mean
     */
    public double[] getMean() {
        return mean.clone();
    }
    
    /**
     * Get a copy of the variance
     */
    public double[] getVar() {
        return var.clone();
    }
    
    /**
     * Get sample count
     */
    public long getCount() {
        return count;
    }
    
    /**
     * Get array size
     */
    public int getSize() {
        return size;
    }
    
    /**
     * Normalize input: (x - mean) / sqrt(var + epsilon)
     */
    public double[] normalize(double[] x, double epsilon) {
        if (x.length != size) {
            throw new IllegalArgumentException("Input array size mismatch: " + x.length + " vs " + size);
        }
        
        double[] normalized = new double[size];
        for (int i = 0; i < size; i++) {
            double std = Math.sqrt(var[i] + epsilon);
            normalized[i] = (x[i] - mean[i]) / std;
        }
        return normalized;
    }
    
    /**
     * Reset all statistics
     */
    public void reset() {
        count = 0;
        for (int i = 0; i < size; i++) {
            mean[i] = 0.0;
            var[i] = 1.0;
            m2[i] = 0.0;
        }
    }
    
    /**
     * Get standard deviation
     */
    public double[] getStd(double epsilon) {
        double[] std = new double[size];
        for (int i = 0; i < size; i++) {
            std[i] = Math.sqrt(var[i] + epsilon);
        }
        return std;
    }
} 