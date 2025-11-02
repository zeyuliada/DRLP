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
 * Reward scaling wrapper
 * Uses plain numerical computation, independent of INDArray
 */
public class ScaleReward implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private SampleMeanStd rewardStats;
    private double rewardTrace;
    private double gamma;
    private double epsilon;
    
    /**
     * Constructor
     * @param gamma Discount factor
     * @param epsilon Small constant to avoid division by zero
     */
    public ScaleReward(double gamma, double epsilon) {
        this.rewardStats = new SampleMeanStd(1); // Reward is a single value
        this.rewardTrace = 0.0;
        this.gamma = gamma;
        this.epsilon = epsilon;
    }
    
    /**
     * Process and scale reward
     * @param reward Raw reward
     * @param terminated Whether episode is terminated
     * @return Scaled reward
     */
    public double process(double reward, boolean terminated) {
        // Update reward trace
        rewardTrace = rewardTrace * gamma * (terminated ? 0 : 1) + reward;
        
        // Update statistics
        double[] rewardArr = new double[]{rewardTrace};
        rewardStats.update(rewardArr);
        
        // Scale reward
        double[] rewardVar = rewardStats.getVar();
        double stdReward = Math.sqrt(rewardVar[0] + epsilon);
        return reward / stdReward;
    }
    
    /**
     * Reset reward trace
     */
    public void reset() {
        rewardTrace = 0.0;
    }
    
    /**
     * Get current reward trace value
     */
    public double getRewardTrace() {
        return rewardTrace;
    }
    
    /**
     * Get reward statistics
     */
    public SampleMeanStd getRewardStats() {
        return rewardStats;
    }
    
    /**
     * Get current reward standard deviation
     */
    public double getRewardStd() {
        double[] var = rewardStats.getVar();
        return Math.sqrt(var[0] + epsilon);
    }
    
    /**
     * Get current reward mean
     */
    public double getRewardMean() {
        double[] mean = rewardStats.getMean();
        return mean[0];
    }
    
    /**
     * Get number of processed reward samples
     */
    public long getRewardCount() {
        return rewardStats.getCount();
    }
} 