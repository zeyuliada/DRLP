# DRLPartitioner - Deep Reinforcement Learning Partitioner

## Overview

This project implements a **Deep Reinforcement Learning (DRL) based stream partitioner** for Apache Flink. It uses Deep Q-Networks (DQN) to adaptively partition data streams based on key frequencies, achieving better load balancing and reduced data fragmentation compared to traditional hash partitioning.

## What Does It Do?

The DRL Partitioner intelligently assigns data keys to worker nodes by:

1. **Learning optimal partitioning strategies** through reinforcement learning
2. **Detecting hot keys** and handling them adaptively
3. **Balancing load** across worker nodes dynamically
4. **Minimizing fragmentation** to reduce communication overhead

## Key Features

- **Deep Q-Learning**: Uses dual target networks for stable training
- **Frequency-aware Partitioning**: Dynamically limits partitions based on key frequency
- **State Normalization**: Normalizes observations and scales rewards for better learning
- **Routing Cache**: LRU cache to speed up inference for frequently accessed keys

## Project Structure

```
DRLPartitioner_Complete/
│
├── partitioning/
│   ├── dqn/
│   │   ├── DRLPartitioner.java              # ★ Main DRL partitioner implementation
│   │   └── containers/                       # Normalization and reward scaling
│   │
│   ├── state/
│   │   └── State.java                        # State management for load & fragmentation
│   │
│   ├── containers/
│   │   ├── HotStatistics.java                # Hot key detection
│   │   ├── CountMinSketch.java               # Frequency estimation
│   │   └── ...                               # Other container classes
│   │
│   ├── DaltonCooperative.java                # Baseline: Contextual Bandits partitioner
│   ├── ContextualBandits.java                # Q-learning based partitioner
│   └── Partitioner.java                      # Abstract base class
│
└── record/
    └── Record.java                           # Data record wrapper
```

## Dependencies

- **Apache Flink**: Stream processing framework
- **DeepLearning4j**: Deep learning library for neural networks
- **ND4J**: N-dimensional arrays for Java

## How It Works

1. **Observation**: The partitioner observes the current load balance and key fragmentation across workers
2. **Decision**: Uses a neural network to select the best worker for each key
3. **Reward**: Calculates reward based on load balance improvement and fragmentation reduction
4. **Learning**: Updates the neural network using experience replay to improve future decisions
