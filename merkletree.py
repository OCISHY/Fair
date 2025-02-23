import csv
import json
from eth_utils import keccak
from merkletools import MerkleTools

def keccak_hash(data):
    return keccak(text=data).hex()

# **1. 读取快照地址**
with open("./airdrop_address_total.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)  # 跳过标题行
    addresses = [row[0] for row in reader]

# **2. 计算每个地址的 Keccak256 哈希**
leaf_nodes = [keccak_hash(addr) for addr in addresses]

# **3. 构建 Merkle Tree**
mt = MerkleTools()
mt.add_leaf(leaf_nodes, True)
mt.make_tree()

# **4. 仅获取 Merkle Root**
merkle_root = mt.get_merkle_root()
print(f"✅ Merkle Root: {merkle_root}")

# **5. 仅保存 Merkle Root**
with open("merkle_root.json", "w") as f:
    json.dump({"root": merkle_root}, f, indent=4)

print("✅ Merkle Root 已保存到 merkle_root.json")
