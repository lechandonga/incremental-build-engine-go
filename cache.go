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

// cacheFormatVersion 是缓存布局/元数据格式自身的版本。
// 升级组件导致缓存格式不兼容时调高此值，旧缓存会被整体判为不兼容而失效。
const cacheFormatVersion = 1

// cacheMeta 是单个缓存条目的元数据（缓存命中判定依据，JSON 落盘）。
type cacheMeta struct {
	FormatVersion int               `json:"format_version"`
	TaskName      string            `json:"task_name"`
	TaskVersion   int               `json:"task_version"`
	InputDigest   string            `json:"input_digest"`
	DepDigest     string            `json:"dep_digest"`
	InputValues   map[string]string `json:"input_values,omitempty"` // 诊断用
	Key           string            `json:"key"`
}

// CacheStore 是基于本地文件的增量缓存。
//
// 目录布局：
//
//	<dir>/<cache-key>/meta.json
//	<dir>/<cache-key>/art/<artifact-name...>
//
// 写入采用“先写临时条目目录、再原子 rename”的方式，
// 因此重试失败或进程被杀都不会覆盖已有的成功条目。
type CacheStore struct {
	dir string
}

// NewCacheStore 在给定目录上创建缓存存储（目录延迟创建）。
func NewCacheStore(dir string) *CacheStore { return &CacheStore{dir: dir} }

// InputDigest 对任务的声明式输入做稳定摘要：Inputs 键值（按键排序）+ 任务版本。
func InputDigest(spec *TaskSpec) string {
	h := sha256.New()
	fmt.Fprintf(h, "task=%s\nversion=%d\n", spec.Name, spec.Version)
	keys := make([]string, 0, len(spec.Inputs))
	for k := range spec.Inputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%s\n", k, spec.Inputs[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// DepDigestOfFiles 对前置任务产物文件做稳定摘要：
// 按依赖任务名、产物名排序，流式 SHA-256 每个文件后聚合。
// 任意依赖产物内容变化都会改变该值，从而触发下游重新执行。
// 任一文件缺失返回错误，调用方应据此视为缓存不可用（不应误命中）。
func DepDigestOfFiles(depFiles map[string]map[string]string) (string, error) {
	h := sha256.New()
	deps := make([]string, 0, len(depFiles))
	for d := range depFiles {
		deps = append(deps, d)
	}
	sort.Strings(deps)
	for _, d := range deps {
		fmt.Fprintf(h, "dep=%s\n", d)
		arts := depFiles[d]
		names := make([]string, 0, len(arts))
		for n := range arts {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fh, err := os.Open(arts[n])
			if err != nil {
				return "", fmt.Errorf("hash dependency artifact %s/%s: %w", d, n, err)
			}
			fhSum := sha256.New()
			if _, err := io.Copy(fhSum, fh); err != nil {
				fh.Close()
				return "", err
			}
			fh.Close()
			fmt.Fprintf(h, "art=%s sha=%s\n", n, hex.EncodeToString(fhSum.Sum(nil)))
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func cacheKey(taskName, inputDigest, depDigest string) string {
	h := sha256.New()
	fmt.Fprintf(h, "v%d task=%s inputs=%s deps=%s", cacheFormatVersion, taskName, inputDigest, depDigest)
	return hex.EncodeToString(h.Sum(nil))
}

// lookup 检查缓存命中。命中时返回条目目录；未命中（含缓存损坏、产物缺失、
// 版本不兼容）时返回 ok=false，并由调用方删除坏条目实现自动失效。
func (c *CacheStore) lookup(ctx context.Context, spec *TaskSpec, inputDigest, depDigest string) (string, bool) {
	if c == nil {
		return "", false
	}
	key := cacheKey(spec.Name, inputDigest, depDigest)
	entryDir := filepath.Join(c.dir, key)
	metaPath := filepath.Join(entryDir, "meta.json")

	raw, err := os.ReadFile(metaPath)
	if err != nil {
		return "", false // 无缓存（含尚未创建），不属损坏
	}
	var meta cacheMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		c.invalidate(entryDir)
		return "", false
	}
	if meta.FormatVersion != cacheFormatVersion {
		c.invalidate(entryDir)
		return "", false
	}
	if meta.TaskVersion != spec.Version || meta.InputDigest != inputDigest ||
		meta.DepDigest != depDigest || meta.Key != key {
		// 正常情况下目录名即 key；任何不一致都视为损坏。
		c.invalidate(entryDir)
		return "", false
	}
	// 元数据通过后，校验每个声明产物真实存在且为普通文件。
	for _, a := range spec.Produces {
		p := filepath.Join(entryDir, "art", filepath.FromSlash(a.Name))
		info, err := os.Stat(p)
		if err != nil || info.IsDir() {
			c.invalidate(entryDir)
			return "", false
		}
	}
	return entryDir, true
}

// commit 把某次成功尝试的暂存目录原子提交为缓存条目。
// 先写入同级临时目录再 rename，保证失败/并发/重试期间旧的有效条目不受影响。
func (c *CacheStore) commit(ctx context.Context, spec *TaskSpec, inputDigest, depDigest, stagingDir string) error {
	if c == nil {
		return nil
	}
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	key := cacheKey(spec.Name, inputDigest, depDigest)
	entryDir := filepath.Join(c.dir, key)
	artDir := filepath.Join(entryDir, "art")

	tmpDir, err := os.MkdirTemp(c.dir, ".tmp-"+key+"-")
	if err != nil {
		return fmt.Errorf("create cache temp entry: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }

	if err := os.MkdirAll(filepath.Join(tmpDir, "art"), 0o755); err != nil {
		cleanup()
		return err
	}

	// 复制暂存产物，只接受声明过的产物文件；缺失产物视为任务执行失败。
	for _, a := range spec.Produces {
		src := filepath.Join(stagingDir, filepath.FromSlash(a.Name))
		info, err := os.Stat(src)
		if err != nil || info.IsDir() {
			cleanup()
			return &ExecutionError{Task: spec.Name, Err: fmt.Errorf("declared artifact %q missing after run", a.Name)}
		}
		dst := filepath.Join(tmpDir, "art", filepath.FromSlash(a.Name))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			cleanup()
			return err
		}
		if err := copyFile(src, dst); err != nil {
			cleanup()
			return err
		}
	}

	meta := cacheMeta{
		FormatVersion: cacheFormatVersion,
		TaskName:      spec.Name,
		TaskVersion:   spec.Version,
		InputDigest:   inputDigest,
		DepDigest:     depDigest,
		InputValues:   spec.Inputs,
		Key:           key,
	}
	metaRaw, err := json.MarshalIndent(&meta, "", "  ")
	if err != nil {
		cleanup()
		return err
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "meta.json"), metaRaw, 0o644); err != nil {
		cleanup()
		return err
	}

	// Rename 在同文件系统内是原子的。Windows/跨设备情况下若失败则退化为
	// 先删旧目录再改名（此时极端崩溃最多没有缓存，不会产生半成品命中）。
	if err := os.Rename(tmpDir, entryDir); err != nil {
		_ = os.RemoveAll(entryDir)
		if err2 := os.Rename(tmpDir, entryDir); err2 != nil {
			cleanup()
			return fmt.Errorf("commit cache entry: %w (fallback: %v)", err, err2)
		}
	}
	if err := c.chmodRecursive(artDir); err != nil {
		return err
	}
	return nil
}

func (c *CacheStore) invalidate(entryDir string) {
	_ = os.RemoveAll(entryDir)
}

func (c *CacheStore) chmodRecursive(dir string) error {
	// 产物默认随进程 umask；这里不强制改权限，仅保留为扩展点。
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
