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

package partitioning.containers;

import java.io.Serializable;

/**
 * 存储频繁键及其频�? * 用于 topKeys 列表（转发到 QTableReducer�? * 
 * 此类�?DaltonCooperative 中提取出来，成为独立的容器类
 * 以便在不同的分区器实现中重用，避免循环依�? */
public class Frequency implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public int key;
    public int freq;

    public Frequency(int k, int f) {
        key = k;
        freq = f;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Frequency))
            return false;
        Frequency other = (Frequency) o;
        return this.key == other.key;
    }

    @Override
    public final int hashCode() {
        return key;
    }
    
    @Override
    public String toString() {
        return "Frequency{key=" + key + ", freq=" + freq + "}";
    }
}


