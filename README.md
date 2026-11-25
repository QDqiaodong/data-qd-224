# data-qd-224 · 构建缓存对象存储

构建者按标签发布制品字节，存储以 SHA-256 地址复用相同内容；读取者按标签获取并校验制品完整性。发布原子更新引用，回收只删除当时未被任何标签或活动构建检出引用的对象。单个本地 SQLite 文件存放小型制品，多个实例或进程打开同一路径时看到同一份状态，每个公开操作在库内表现为某个串行顺序。

Python 3.9+，仅标准库。示例使用本仓合成制品和临时数据库。

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

两个标签指向同一制品时显示 `same digest: True`；旧标签移除后输出 `reclaimed: 1`，稳定标签仍取回 `b'artifact-v2'`。

## 构建检出

标签是可换绑的别名，构建的后续阶段不应被别的构建者中途换绑影响：

```python
handle, bindings = store.begin_build(["compiler", "runtime"])
# bindings == {"compiler": "<sha256>", "runtime": "<sha256>"}
store.publish("compiler", new_bytes)          # 别的构建者换绑，不影响本次检出
blob = store.fetch_for_build(handle, "compiler")   # 仍按检出时的绑定取字节
store.finish_build(handle)
```

- `begin_build(names)`：一次性解析 1–24 个互不重复的标签，返回不复用的不透明句柄和 `{标签: SHA-256}`。参数无效或已有 64 个活动检出时抛 `ValueError`；任一标签不存在时抛 `KeyError`，失败不留下引用。
- `fetch_for_build(handle, name)`：按检出时的绑定取字节并校验 SHA-256。未知句柄或标签不在该检出中抛 `KeyError`；字节与摘要不符抛 `CorruptBlob`。
- `finish_build(handle)`：释放该句柄，返回是否释放成功；未知或已释放返回 `False`。只解除引用，不删除对象。

保留关系按 digest 记在数据库里：同一 digest 被多个构建保留时，最后一个句柄释放后才解除保护。句柄、绑定和保留关系在重新打开后继续有效，不需要持有长连接。检出、构建读取和释放都不会刷新普通缓存的使用顺序。

## 容量准入

`configure_capacity(max_bytes)` 持久设置容量（0 到 1 GiB 的整数，不接受 `bool`），成功返回 `None`；未配置时保持原有的无容量模式。容量按库中实际驻留的唯一 digest 的字节数求和：按标签数、构建数或 SQLite 文件长度都不计费，尚未回收的无引用字节也占容量。

设置容量和容量模式下的每次 `publish` 都在同一个事务中完成准入、换绑与必要的驱逐。超限时按以下顺序驱逐，一旦不超限就停止：

1. 没有标签的可驱逐对象，按 digest 升序；
2. 仍有标签的可驱逐对象，按最早使用在先（`fetch` 成功返回或 `publish` 成功提交即算使用，平局按 digest 升序）。

一次驱逐某个仍挂标签的 digest 会移除指向它的所有标签和对象本身。活动检出保留的 digest 和本次发布的 digest 不可驱逐。若所有可驱逐对象加起来也不够，抛 `CapacityExceeded`，本次尝试不改动容量、标签、对象、检出关系或使用顺序。

普通 `fetch` 任一别名都会刷新整个 digest 的使用次序；该次序只按库内成功操作的先后持久化，不依赖墙钟，升级前已存在且尚未被使用的对象并列为最旧。`remove` 仍只移除标签；`collect` 返回实际删除的对象数，但只删除没有标签也没有活动检出保留的对象。

## 测试

```sh
python3 -B -m unittest -v
```

所有测试都在临时 SQLite 文件上运行，不触碰仓库内文件。`test_scale.py` 会建立 112 个各 1 MiB 的旧格式对象、原地升级并在 Linux 子进程的 96 MiB 地址空间限制内降到 64 MiB，然后校验留下的制品可读。只跑某个文件：

```sh
python3 -B -m unittest -v test_objects test_builds_capacity test_scale
```
