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

## 按接收端缺件的构建传输

两个节点各用各的 SQLite 文件，互不打开对方的库；调用者负责在节点间搬运普通 Python 字典形态的清单和原始字节流。整个特性不需要网络服务、后台线程或续传协议。

清单是 `{标签: (digest, 字节数)}` 的普通字典，沿用一次检出的 1–24 个有效标签；digest 为 64 位小写十六进制字符串，字节数为 0–1 MiB 的整数（不接受 `bool`），同一 digest 可对应多个标签但长度必须一致。

```python
manifest = source.describe_build(handle)          # 以句柄绑定为准，不看当前标签
needed = dest.transfer_needs(manifest)            # 接收端缺什么才传什么
sink = ...                                        # 调用者提供的阻塞式二进制 sink
source.send_build(handle, manifest, needed, sink)
handle2, bindings = dest.install_build(
    manifest, needed, source_stream)              # 一次性原子安装为新检出
```

- `describe_build(handle)`：返回该检出绑定的清单，不看当前标签，不更新使用次序；未知或已释放句柄抛 `KeyError`。
- `transfer_needs(manifest)`：在同一个本地视图里，按 digest 升序返回缺失、长度不符或 SHA-256 不符的唯一 digest。当前是否有标签不影响复用。它只是减传建议，不建立保留关系，也不更新普通 LRU。
- `send_build(handle, manifest, needed, sink)`：以发送开始时取得的单一源视图为准，先在一个短事务里完成全部校验并把所选制品暂存到本地临时文件，随后**不持有任何数据库锁**地从暂存文件喂给 sink。因此句柄在发送期间被另一连接释放、换绑并回收也不会改变这次发送的版本，慢 sink 阻塞期间对端的写事务照常提交。任何参数或清单不符（`ValueError`）、未知句柄（`KeyError`）、源字节损坏（`CorruptBlob`）都在写出第一个字节之前抛出。每个 digest 只发一次，线上格式是按 digest 升序拼接的原始内容，无头尾；`needed` 为空时不写字节。发送不刷新 LRU、不占用额外活动构建名额。
- `install_build(manifest, needed, source)`：先在不持有数据库锁的情况下把线上字节按清单长度切分、流式暂存并逐个校验 SHA-256，随后用**一个**排他事务完成全部准入。短读、尾随字节抛 `ValueError`；线上内容与应有 digest 不符抛 `CorruptBlob`。未传输的 digest 在最终准入视图里重新确认长度和 SHA-256，缺失或损坏抛 `TransferBaseUnavailable`，其 `digests` 属性是需要重传的唯一 digest 升序元组——这样“传来的帧坏了”（`CorruptBlob`）与“没传的本地副本不再可用”（`TransferBaseUnavailable`）明确可区分。
- 传完且校验通过的内容可以在同一事务里原子修复同 digest 的本地坏副本；普通 `publish` 遇到坏副本的语义保持不变。安装绝不会用清单名字发布或换绑目的库标签。新句柄计入现有 64 个名额，重开数据库后仍可用原来的 `fetch_for_build` / `finish_build`；同一 digest 在一个检出里只占一份驻留字节。
- 容量按加上新唯一制品、修复坏制品后的实际驻留字节计算，整份清单的 digest 与既有活动检出均不可驱逐；需要腾空间时仍走无标签优先、普通 LRU 的现有规则，腾不够抛 `CapacityExceeded`。复用、接收、修复和新检出都不刷新普通 LRU。成功时新制品、修复、必要驱逐和全部绑定一起生效；任何失败都不改目的库标签、制品、用序、容量或保留关系。等待 source 读取期间，目的库上的其它实例可以完成普通写操作，最终准入按当时的现实状态判断，不撤销别人已提交的操作。

流式约定：调用者提供整块接受或抛异常的 `sink.write(bytes)` 与可短读、空字节串表示 EOF 的 `source.read(n)`；双方单次读写请求都不超过 64 KiB，流不可 seek 也不会被本库关闭，流 I/O 异常原样向上传递。允许使用本地临时磁盘暂存，调用结束后清理；实现不依赖在应用堆内攒下整包字节。最多支持一个检出的 24 个唯一制品、共 24 MiB 线上数据。

## 测试

```sh
python3 -B -m unittest -v
```

所有测试都在临时 SQLite 文件上运行，不触碰仓库内文件。`test_scale.py` 会建立 112 个各 1 MiB 的旧格式对象、原地升级并在 Linux 子进程的 96 MiB 地址空间限制内降到 64 MiB，然后校验留下的制品可读。`test_build_transfer.py` 覆盖缺件传输、同名标签不换绑、驱逐保护、同步点并发、截短/尾随/错 digest、坏副本修复与失败回滚；其中 Linux 规模用例在两个子进程里经 FIFO 和非 seek 的限块读写接口传输 24 个 1 MiB 制品，并以模块载入后实测虚拟内存加 16 MiB 设置地址空间上限。只跑某个文件：

```sh
python3 -B -m unittest -v test_objects test_builds_capacity test_scale test_build_transfer
```
