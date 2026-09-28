package buildengine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// cache.go 实现基于输入摘要与前置任务传递键的增量缓存。
//
// 缓存目录布局：
//
//	<cacheDir>/v<version>/<task-id>/entry.json          缓存元数据
//	<cacheDir>/v<version>/<task-id>/outputs/<相对路径>  产物快照
//
// task-id 为任务名的 SHA-256，避免非法路径字符与碰撞。
//
// 命中判定（任一不满足即失效并给出原因）：
//  1. 缓存版本与当前引擎一致；
//  2. 输入摘要（输入文件内容哈希 + Params 哈希）与记录一致；
//  3. 每个前置依赖任务的传递缓存键与记录一致；
//  4. 缓存中的产物快照完整且摘要匹配。
//
// 只有任务完全成功（且产物校验通过）后才以原子方式写入缓存，
// 失败与重试过程绝不触碰缓存，因此不会污染既有成功结果。

// CacheVersion 是缓存布局与键算法的版本号；不兼容时整体失效。
const CacheVersion = 1

// cacheEntry 是单个任务的缓存元数据。
type cacheEntry struct {
	Version        int               `json:"version"`
	TaskName       string            `json:"task_name"`
	InputDigest    string            `json:"input_digest"`
	ParamsHash     string            `json:"params_hash"`
	DependencyKeys map[string]string `json:"dependency_keys"`
	OutputDigests  map[string]string `json:"output_digests"`
}

// Cache 管理缓存的查询、原子写入与失效。
type Cache struct {
	dir string
}

// NewCache 在 dir 下打开一个缓存（惰性创建）。dir 为空表示禁用缓存。
func NewCache(dir string) *Cache {
	return &Cache{dir: dir}
}

// Enabled 报告缓存是否启用。
func (c *Cache) Enabled() bool {
	return c.dir != ""
}

// LookupResult 描述一次缓存判定的结果与可解释依据。
type LookupResult struct {
	Hit    bool
	Reason string
	Entry  *cacheEntry
}

func (c *Cache) taskDir(task string) string {
	sum := sha256.Sum256([]byte(task))
	return filepath.Join(c.dir, fmt.Sprintf("v%d", CacheVersion), hex.EncodeToString(sum[:]))
}

// lookup 判定任务是否存在可复用缓存（同时校验快照完整性），但不恢复产物。
func (c *Cache) lookup(task, inputDigest, paramsHash string, depKeys map[string]string) LookupResult {
	if !c.Enabled() {
		return LookupResult{Reason: "cache disabled"}
	}
	dir := c.taskDir(task)
	data, err := os.ReadFile(filepath.Join(dir, "entry.json"))
	if err != nil {
		return LookupResult{Reason: "cache entry missing"}
	}
	var entry cacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		// 元数据损坏：立即剔除，绝不复用。
		c.invalidate(task)
		return LookupResult{Reason: "cache entry corrupt: " + err.Error()}
	}
	if entry.Version != CacheVersion {
		return LookupResult{Reason: fmt.Sprintf("cache version mismatch: entry=%d engine=%d", entry.Version, CacheVersion)}
	}
	if entry.InputDigest != inputDigest {
		return LookupResult{Reason: "input digest changed"}
	}
	if entry.ParamsHash != paramsHash {
		return LookupResult{Reason: "params changed"}
	}
	if !sameKeys(entry.DependencyKeys, depKeys) {
		return LookupResult{Reason: "dependency key changed"}
	}
	// 快照完整性：每个声明产物必须存在且摘要一致，否则视为损坏并失效。
	for rel, want := range entry.OutputDigests {
		got, err := digestFile(filepath.Join(dir, "outputs", filepath.FromSlash(rel)))
		if err != nil {
			c.invalidate(task)
			return LookupResult{Reason: "cached artifact missing: " + rel}
		}
		if got != want {
			c.invalidate(task)
			return LookupResult{Reason: "cached artifact corrupt: " + rel}
		}
	}
	return LookupResult{Hit: true, Reason: "cache hit", Entry: &entry}
}

// restore 将缓存产物快照原子地复制回根目录。任一产物恢复失败即返回 false。
func (c *Cache) restore(ctx context.Context, task, rootDir string, outputs []string) (bool, string) {
	dir := c.taskDir(task)
	for _, out := range outputs {
		if err := ctx.Err(); err != nil {
			return false, "canceled while restoring cache"
		}
		target, err := safeJoin(rootDir, out)
		if err != nil {
			return false, "invalid output path: " + out
		}
		snapshot, err := os.ReadFile(filepath.Join(dir, "outputs", filepath.FromSlash(filepath.Clean(out))))
		if err != nil {
			c.invalidate(task)
			return false, "snapshot unreadable: " + out
		}
		if err := atomicWriteFile(target, snapshot); err != nil {
			return false, "restore write failed: " + out
		}
	}
	return true, "cache hit"
}

// store 以原子方式写入任务的缓存条目与产物快照。
// 采用“临时目录 -> 换入”的方式：写入失败只留下临时目录，不会破坏既有条目。
func (c *Cache) store(ctx context.Context, task, rootDir, inputDigest, paramsHash string, depKeys map[string]string, outputs []string) error {
	if !c.Enabled() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	versionDir := filepath.Join(c.dir, fmt.Sprintf("v%d", CacheVersion))
	if err := os.MkdirAll(versionDir, 0o755); err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp(versionDir, ".tmp-entry-*")
	if err != nil {
		return err
	}
	abort := func() { _ = os.RemoveAll(tmpDir) }

	outDigests := make(map[string]string, len(outputs))
	for _, out := range outputs {
		if err := ctx.Err(); err != nil {
			abort()
			return err
		}
		clean := filepath.Clean(out)
		src, err := safeJoin(rootDir, clean)
		if err != nil {
			abort()
			return err
		}
		sum, err := digestFile(src)
		if err != nil {
			abort()
			return fmt.Errorf("artifact %s missing after success: %w", clean, err)
		}
		data, err := os.ReadFile(src)
		if err != nil {
			abort()
			return err
		}
		snapTarget := filepath.Join(tmpDir, "outputs", filepath.FromSlash(clean))
		if err := os.MkdirAll(filepath.Dir(snapTarget), 0o755); err != nil {
			abort()
			return err
		}
		if err := os.WriteFile(snapTarget, data, 0o644); err != nil {
			abort()
			return err
		}
		outDigests[clean] = sum
	}

	entry := cacheEntry{
		Version:        CacheVersion,
		TaskName:       task,
		InputDigest:    inputDigest,
		ParamsHash:     paramsHash,
		DependencyKeys: depKeys,
		OutputDigests:  outDigests,
	}
	raw, err := json.MarshalIndent(&entry, "", "  ")
	if err != nil {
		abort()
		return err
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "entry.json"), raw, 0o644); err != nil {
		abort()
		return err
	}

	finalDir := c.taskDir(task)
	// 换入：旧目录先改名挪开，新条目就位后再清理旧目录。
	var oldDir string
	if _, err := os.Stat(finalDir); err == nil {
		oldDir = finalDir + ".old"
		_ = os.RemoveAll(oldDir)
		if err := os.Rename(finalDir, oldDir); err != nil {
			abort()
			return err
		}
	}
	if err := os.Rename(tmpDir, finalDir); err != nil {
		// 尽量把旧目录还原。
		if oldDir != "" {
			_ = os.Rename(oldDir, finalDir)
		}
		abort()
		return err
	}
	if oldDir != "" {
		_ = os.RemoveAll(oldDir)
	}
	return nil
}

// invalidate 删除任务的缓存条目（发现损坏/快照缺失时立即剔除）。
func (c *Cache) invalidate(task string) {
	if !c.Enabled() {
		return
	}
	_ = os.RemoveAll(c.taskDir(task))
}

// cacheKey 计算任务对外暴露的传递缓存键：输入、参数与所有依赖键共同决定。
// 任一输入或任何上游（含传递）变化都会改变该键。
func cacheKey(task, inputDigest, paramsHash string, depKeys map[string]string) string {
	h := sha256.New()
	fmt.Fprintf(h, "v%d|task=%s|inputs=%s|params=%s", CacheVersion, task, inputDigest, paramsHash)
	keys := make([]string, 0, len(depKeys))
	for k := range depKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(h, "|dep:%s=%s", k, depKeys[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// digestInputs 计算任务声明输入文件的确定性摘要。
// 缺失的输入以 MISSING 标记参与摘要（稳定且不会误命中）。
func digestInputs(root string, inputs []string) (string, error) {
	h := sha256.New()
	sorted := append([]string(nil), inputs...)
	sort.Strings(sorted)
	for _, in := range sorted {
		clean := filepath.Clean(in)
		path, err := safeJoin(root, clean)
		if err != nil {
			return "", err
		}
		sum, err := digestFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Fprintf(h, "%s=MISSING\n", clean)
				continue
			}
			return "", err
		}
		fmt.Fprintf(h, "%s=%s\n", clean, sum)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hashParams 以排序后的键值对计算参数摘要。
func hashParams(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%s\n", k, params[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func digestFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func sameKeys(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
