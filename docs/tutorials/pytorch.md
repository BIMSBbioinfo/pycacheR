# PyTorch Deep Learning

This tutorial shows how cachepy accelerates a typical PyTorch workflow by caching
every expensive step — data loading, training, evaluation, and feature extraction.
It corresponds to the
[`03_pytorch_cachepy.ipynb`](https://github.com/BIMSBbioinfo/cachepy/blob/main/notebooks/03_pytorch_cachepy.ipynb) notebook.

We use MNIST for simplicity, but the same pattern applies to any PyTorch pipeline.

## Setup

```python
import sys, time, shutil, warnings
from pathlib import Path
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, TensorDataset
from torchvision import datasets, transforms

from cachepy import cache_file, cache_tree_nodes, cache_tree_reset
from cachepy.cache_file import cache_stats, _file_state_cache

CACHE_DIR = Path("pytorch_cache")
DATA_DIR = Path("data")
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
```

## 1. Cached Data Loading

```python
@cache_file(CACHE_DIR, verbose=True)
def load_mnist(data_dir="data", flatten=False):
    """Download MNIST and return train/test tensors."""
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,)),
    ])
    train_ds = datasets.MNIST(data_dir, train=True, download=True, transform=transform)
    test_ds = datasets.MNIST(data_dir, train=False, download=True, transform=transform)

    X_train = torch.stack([train_ds[i][0] for i in range(len(train_ds))])
    y_train = torch.tensor([train_ds[i][1] for i in range(len(train_ds))])
    X_test = torch.stack([test_ds[i][0] for i in range(len(test_ds))])
    y_test = torch.tensor([test_ds[i][1] for i in range(len(test_ds))])

    return {"X_train": X_train, "y_train": y_train,
            "X_test": X_test, "y_test": y_test}

data = load_mnist(str(DATA_DIR))
```

## 2. Model Definition

```python
class SimpleCNN(nn.Module):
    def __init__(self, n_filters=32, dropout=0.25):
        super().__init__()
        self.conv1 = nn.Conv2d(1, n_filters, 3, padding=1)
        self.conv2 = nn.Conv2d(n_filters, n_filters * 2, 3, padding=1)
        self.pool = nn.MaxPool2d(2)
        self.dropout = nn.Dropout(dropout)
        self.fc1 = nn.Linear(n_filters * 2 * 7 * 7, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = self.dropout(x)
        x = x.view(x.size(0), -1)
        x = F.relu(self.fc1(x))
        x = self.dropout(x)
        return self.fc2(x)
```

## 3. Cached Training

Different hyperparameter combinations produce different cache entries.
Re-running with the same hyperparameters loads the trained model instantly.

```python
@cache_file(CACHE_DIR, verbose=True)
def train_model(X_train, y_train, n_filters=32, dropout=0.25,
                lr=1e-3, epochs=3, batch_size=128, seed=42):
    torch.manual_seed(seed)
    model = SimpleCNN(n_filters=n_filters, dropout=dropout).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.CrossEntropyLoss()
    # ... training loop ...
    return {"state_dict": model.cpu().state_dict(), "history": history,
            "n_filters": n_filters, "dropout": dropout}

result = train_model(data["X_train"], data["y_train"],
                     n_filters=32, dropout=0.25, lr=1e-3, epochs=3)
```

## 4. Cached Evaluation

Evaluation is cached separately — change visualization code without re-running inference.

```python
@cache_file(CACHE_DIR, verbose=True)
def evaluate_model(train_result, X_test, y_test):
    model = SimpleCNN(
        n_filters=train_result["n_filters"],
        dropout=train_result["dropout"]
    )
    model.load_state_dict(train_result["state_dict"])
    model.eval()
    # ... evaluation logic ...
    return {"accuracy": acc, "class_acc": class_acc, "confusion": confusion}

metrics = evaluate_model(result, data["X_test"], data["y_test"])
```

## 5. Hyperparameter Search with Caching

Only new configurations train. Re-running the cell is instant for all previously seen configs.

```python
configs = [
    {"n_filters": 16, "lr": 1e-3, "epochs": 3},
    {"n_filters": 32, "lr": 1e-3, "epochs": 3},
    {"n_filters": 32, "lr": 5e-4, "epochs": 3},
    {"n_filters": 64, "lr": 1e-3, "epochs": 3},
]

for cfg in configs:
    res = train_model(X_sub, y_sub, **cfg)
    ev = evaluate_model(res, data["X_test"], data["y_test"])
    print(f"Test acc: {ev['accuracy']:.4f}")
```

## 6. Cached Feature Extraction

Extract intermediate features for downstream analysis (clustering, visualization).

```python
@cache_file(CACHE_DIR, verbose=True)
def extract_features(train_result, X, batch_size=256):
    model = SimpleCNN(...)
    model.load_state_dict(train_result["state_dict"])
    model.eval()
    # Hook into fc1 output
    features_list = []
    def hook_fn(module, input, output):
        features_list.append(output.detach().cpu())
    handle = model.fc1.register_forward_hook(hook_fn)
    # ... forward pass ...
    return torch.cat(features_list, dim=0)

features = extract_features(result, data["X_test"])
```

## Re-run Demo

When re-opening the notebook, all expensive steps are instant:

```python
data = load_mnist(str(DATA_DIR))         # from cache
result = train_model(...)                 # from cache
metrics = evaluate_model(...)             # from cache
features = extract_features(...)          # from cache
# Total: < 1 second
```

## Speed Benchmark

Cache overhead is constant and tiny. The bigger the computation, the bigger the speedup.
See the [full notebook](https://github.com/BIMSBbioinfo/cachepy/blob/main/notebooks/03_pytorch_cachepy.ipynb) for benchmark plots.
