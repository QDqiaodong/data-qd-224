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

## 单次生产协调与死锁解围

多个 worker 未命中同一配方时，用配方键（同时也是普通标签名；1–48 个 ASCII 字母、数字、`_`、`-`）领取唯一生产权。协调状态和普通缓存位于同一个 SQLite 文件，所有等待替换、解围撤销和完成提交都在一个 `BEGIN IMMEDIATE` 事务中串行生效。

```python
status, token = store.claim_output("linux-header")
# status == "claimed"；已有生产者时为 ("busy", None)，已有协调产出时
# 校验字节后为 ("ready", data)。

store.set_output_waits(
    token,
    [["linux-clang", "linux-gcc"], ["headers"]],
)
# 子句之间是 AND；["linux-clang", "linux-gcc"] 表示任一兼容提供方即可。

digest = store.complete_output(token, header_bytes)
```

- `claim_output(key)`：已有活动生产权时返回 `("busy", None)`，绝不返回他人令牌；否则若存在当前协调产出，校验实际字节 SHA-256 后返回 `("ready", data)`，坏字节抛 `CorruptBlob`；都没有才返回 `("claimed", token)`。最多 256 个活动生产者，满额只阻止新领权（`ValueError`），不影响 busy/ready，也不占用 64 个构建检出名额。
- `set_output_waits(token, groups)`：先整体校验 0–16 个 list/tuple 子句，每子句 1–16 个互不重复的有效键；然后整体替换等待并在同一事务内解围，返回按实际撤销顺序排列的配方键元组（可能包含调用者）。空序列清除等待。
- `complete_output(token, data)`：接受当前令牌和 0–1 MiB `bytes`。提交视图中每个已声明子句都必须至少有一个**当前协调产出**，否则抛 `OutputWaiting` 并保留令牌和全部等待；未认领或仅可推进不算产出。检查先于制品准入。成功时按现有内容寻址、完整性、容量、驱逐和 LRU 规则发布到同名普通标签，写入一次性完成标记，删除生产权和等待，并返回 digest。容量/完整性失败整体回滚，令牌仍可重试。
- `cancel_output(token)`：删除当前生产权及等待但不删除普通标签；成功 `True`，未知或已失效令牌 `False`。已释放、已被解围撤销或已完成的令牌再调用 `set_output_waits` / `complete_output` 一律抛 `StaleOutputClaim`。无效参数抛 `ValueError`；参数有效而令牌失效时，失效判断先于等待或容量业务。

完成标记只在标记与同名、同 digest 标签绑定同时存在时有效。普通 `publish(key, ...)`（即使 digest 相同）、`remove(key)` 或容量驱逐删除该标签都会在同一事务内使标记失效；之后再次普通发布同名不会复活旧标记。只共享 digest 的其它标签或其独立完成标记不受影响。标记不固定 blob、不是新的回收根，也不保留历史。等待判断只读取配方、等待和“标记+标签绑定”元数据，不重算依赖制品摘要；只有 `claim_output` 实际返回 ready 字节时才校验 SHA-256。除成功的 `complete_output` 外，这些入口不刷新普通 LRU。

解围使用替换等待后的完整已提交状态，而不是单次调用的静态图：

1. 已有当前协调产出的键、以及当前没有活动生产者的键，首先是“可能推进”的键；空闲键视为调度器可另派 worker，不考虑外部 worker 池或 256 个生产槽是否耗尽。
2. 活动生产者只有在每个 AND 子句都至少有一个已可能推进的 OR 候选时才变为可能推进；单调扩展到不动点，因此环内成员不能互相担保。
3. 对剩余无法推进的生产者，只取“整条子句都没有可能推进候选”的子句，从等待者向其中所有候选键的当前生产者连边。
4. 在该图没有出边到其它分量的强连通分量（包括自环）中，汇总全部键并取 ASCII 字典序最小者，撤销其当前令牌和等待。指入死锁分量、但自身分量有出边的上游旁观者不会被撤销。
5. 以撤销后的新状态重复判断，直到没有死锁；撤销只释放资格，不发布制品。

## 工作流计划

`prepare_workflow(actions, targets, byte_budget)` 从目标动作的联合依赖图中决定哪些动作实际执行、哪些直接从缓存取，并为非空复用一次性创建按**动作 ID** 绑定的活动检出。

- `actions` 含 1–64 个动作；动作 ID 是 1–48 个 ASCII 字母、数字、`_` 或 `-`。每个声明为 `(work, deps, tag, digest)`：`work` 是 1–1,000,000 的整数（不接受 `bool`），`deps` 是不重复且均已声明的动作 ID 元组。没有候选时后两项均为 `None`；否则是有效标签和 64 位小写 SHA-256。最多 18 个候选动作。
- `targets` 是 1–24 个不重复的已声明动作 ID；`byte_budget` 是 0–24 MiB 的整数（不接受 `bool`）。所有动作和依赖都会校验，包括目标不可达节点；未知依赖、重复项、环或其它无效参数均抛 `ValueError`，且不会创建检出。
- 候选只有在该动作**自己的标签**仍绑定指定 digest、库内副本存在、长度不超过 1 MiB 且 SHA-256 校验通过时才可命中。标签缺失、换绑或副本损坏只使该候选不可用；同一 digest 能从别的标签读到不能替代本标签。不会修复或删除坏副本。
- 命中一个动作后不再访问它的依赖；未命中的动作必须执行并继续满足其依赖。同一动作只执行或复用一次，不可达动作不进入计划。预算按所选 digest 的唯一实际字节数计算；多个动作选中同一 digest 时，`cached` 分别列出动作，但字节只计一次。
- 优化顺序为：实际执行动作的 `work` 总和最小 → 唯一复用字节最少 → 升序复用动作 ID 元组的字典序最小。搜索比较所有目标的联合闭包，而不是分别拼接分支。`run_order` 只包含待执行动作；仍需执行的依赖先运行，每一步在可运行动作中取最小 ID。

```python
handle, run_order, cached = store.prepare_workflow(
    {
        "A": (6, ("P",), "tag-a", digest_x),
        "B": (6, ("P",), "tag-b", digest_x),
        "P": (10, (), None, None),
        "C": (9, (), "tag-c", digest_y),
    },
    ["A", "B", "C"],
    4,
)
# 预算只收一份 X 的 4 字节：cached == {"A": digest_x, "B": digest_x}
# run_order == ["C"]
store.fetch_for_build(handle, "A")       # 按动作 ID 读取，而不是 "tag-a"
store.describe_build(handle)             # 键也是 "A"、"B"
```

`cached` 非空时，返回前用一个最终 `BEGIN IMMEDIATE` 准入事务同时创建检出和全部动作 ID 绑定，占用现有 64 个活动检出名额之一。最终视图名额已满会抛 `ValueError`，不会改交较差计划。`cached` 为空时返回 `handle=None`，不占名额，即使已有 64 个检出也能交付全执行计划。

候选快照读取和最多 `2^18` 个子集的图搜索都在数据库事务外进行，不持有阻塞其它连接提交的 SQLite 锁。最终准入会重新读取全部候选的标签绑定、长度和实际 SHA-256；只要任何候选（包括原先不可用或未选中的候选）发生变化，就释放准入锁、按新视图重算并再次尝试。因此并发修改停止后会得到最终视图中的全局最优计划。探测、计划、绑定和准入都不刷新普通 LRU，也不修改标签、容量或驻留对象；失败事务不留下检出或保护关系。

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

所有测试都在临时 SQLite 文件上运行，不触碰仓库内文件。`test_output_coordination.py` 覆盖 AND-of-OR 等待、确定性 SCC 解围和旁观者保护、自环、重开后的失效令牌、完成标记与普通发布/移除/驱逐的关系、损坏 ready 制品、容量失败保留生产权、Barrier 同步的双连接领权竞争、256 生产槽与 LRU 不变；其中 Linux 验收用例先在父进程备好 80 个 1 MiB 制品和 256 个生产者及多组独立链/跨组长链，再在冷开子进程导入并打开库后按当时 `VmSize + 24 MiB` 设置 `RLIMIT_AS`，执行一次闭链等待更新和解围。`test_workflow_plan.py` 覆盖联合图优化、共享 digest 预算、标签换绑/损坏未命中、平局规则、零字节候选、不可达节点校验、并发候选恢复后的最终重算、动作 ID 检出保护、64 槽原子准入和 LRU 不变；`test_scale.py` 会建立 112 个各 1 MiB 的旧格式对象、原地升级并在 Linux 子进程的 96 MiB 地址空间限制内降到 64 MiB，然后校验留下的制品可读。`test_build_transfer.py` 覆盖缺件传输、同名标签不换绑、驱逐保护、同步点并发、截短/尾随/错 digest、坏副本修复与失败回滚；其中 Linux 规模用例在两个子进程里经 FIFO 和非 seek 的限块读写接口传输 24 个 1 MiB 制品，并以模块载入后实测虚拟内存加 16 MiB 设置地址空间上限。运行一条命令执行全部新旧回归：

```sh
python3 -B -m unittest -v test_objects test_builds_capacity test_workflow_plan test_scale test_build_transfer test_output_coordination
```

也可用 unittest 自动发现：

```sh
python3 -B -m unittest -v
```
